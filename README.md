# Continuum

**English** | [简体中文](https://github.com/Yyeger/Continuum/blob/main/README.zh-CN.md)

[![CI](https://github.com/Yyeger/Continuum/actions/workflows/ci.yml/badge.svg)](https://github.com/Yyeger/Continuum/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/continuum.svg)](https://hex.pm/packages/continuum)
[![Documentation](https://img.shields.io/badge/hexdocs-docs-8e44ad.svg)](https://hexdocs.pm/continuum)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/Yyeger/Continuum/blob/main/LICENSE)

**Continuum lets an Elixir function survive your application crashing halfway
through it.**

```elixir
def run(%{order_id: id, items: items}) do
  {:ok, validated} = activity Validation.check(items)
  {:ok, charge}    = activity Payments.charge(id, validated.total)

  # ─── kill -9 the entire VM right here ───

  {:ok, shipment}  = activity Fulfillment.ship(id)
  {:ok, %{charge: charge, shipment: shipment}}
end
```

Kill the node on that middle line and nothing is lost and nothing is repeated.
The process is gone; the *run* is not. A new VM picks the run up, executes
`run/1` again from the top, replays the charge out of the journal instead of
calling the payment gateway a second time, and carries on into `ship`.

Continuum is a durable execution engine — Temporal's programming model, but
OTP-native and Postgres-backed. No separate cluster service, no paid SaaS
dependency, no polyglot SDK. It lives in your application's supervision tree
and uses the database you already run.

## See it happen

`mix continuum.demo` is not a diagram. It runs that workflow against a real
Postgres, and then really kills the BEAM.

![Continuum surviving a hard crash mid-workflow](https://raw.githubusercontent.com/Yyeger/Continuum/main/dev/demo/continuum-demo.gif)

What you just watched, in order:

1. Start a checkout run.
2. `ChargeCard` executes and commits its result to the journal.
3. `:erlang.halt/1` — no graceful shutdown, no `terminate/2`, no cleanup.
4. Start a **brand new** VM.
5. Boot recovery and the dispatcher find a run leased by a node that no longer exists.
6. The workflow body runs again from its first line — and the charge does **not** happen again.
7. `ShipOrder` runs for the first time, and the run completes.

```bash
docker compose up -d              # Postgres on localhost:5433
mix continuum.demo                # phase 1: charge the card, then kill the VM
mix continuum.demo --resume       # phase 2: replay, ship, never re-charge
```

<details>
<summary>The same demo as plain text, if your client blocks GIFs</summary>

```text
$ mix continuum.demo

── phase 1 — a checkout that dies halfway through ────────────────────
[demo] starting checkout for order #123 (4200 cents)

[continuum] run ac7e3425 started
[workflow] checkout started
[continuum] activity scheduled: ContinuumDemo.ChargeCard.run
[continuum] run ac7e3425 suspended — waiting on durable work
[continuum] activity completed: ContinuumDemo.ChargeCard.run
[workflow] card charged pay_5b1a7dbe

[demo] *** KILLING THE BEAM (erlang:halt/1, no shutdown, no cleanup) ***
[demo] the shipment has not been scheduled — the charge is already journaled


$ mix continuum.demo --resume

── phase 2 — a brand new VM picks the run back up ────────────────────
[continuum] found run ac7e3425 in state=suspended, leased by a node that no longer exists

── continuum_events for run ac7e3425 ─────────────────────────────────
   0  18:09:44.495  workflow_log          %{message: "checkout started", ...}
   1  18:09:44.498  activity_scheduled    %{mfa: {ContinuumDemo.ChargeCard, :run, ...}}
   2  18:09:45.488  activity_completed    %{mfa: {ContinuumDemo.ChargeCard, :run, ...}}
   3  18:09:45.505  workflow_log          %{message: "card charged pay_5b1a7dbe", ...}

[demo] the charge is already journaled, so replay will not re-run it

[continuum] dispatcher claimed 1 orphaned run(s) — lease had expired
[continuum] run ac7e3425 resumed — replaying its journal from event 0
[continuum] activity scheduled: ContinuumDemo.ShipOrder.run
[continuum] activity completed: ContinuumDemo.ShipOrder.run
[workflow] order shipped ship_c66c1cbb
[continuum] run ac7e3425 completed

[demo] this VM took 1.2s to finish someone else's work

── verdict ───────────────────────────────────────────────────────────
  ✓ card charged exactly once — 1 CHARGE line(s) in the ledger
  ✓ order shipped exactly once — 1 SHIP line(s) in the ledger
  ✓ replay stayed silent — this VM printed 1 workflow log line, not the 3 in the journal
  ✓ the whole function body ran again, harmlessly — run/1 executed twice;
    its side effects executed once each

The function survived the VM dying halfway through it.
```

</details>

The demo's activities append to `tmp/continuum_demo/ledger.log`, which stands in
for the outside world. That file, not the log lines, is the actual claim:

```text
2026-08-26T18:09:45.486Z  CHARGE  order=123 payment=pay_5b1a7dbe cents=4200
2026-08-26T18:10:07.132Z  SHIP    order=123 shipment=ship_c66c1cbb
```

One `CHARGE`. `ChargeCard` deliberately declares **no** idempotency key, so
nothing deduplicated a second call to the gateway — there was no second call.

Run `mix continuum.demo --observer` in a second terminal to watch the same
journal fill in through the Observer LiveView while it happens, and
`mix continuum.demo --help` for the rest. The workflow, the activities, and the
kill switch are about 150 lines in [`dev/demo/`](https://github.com/Yyeger/Continuum/tree/main/dev/demo).

## Why this works

`run/1` is a pure function of its journal. Every effect — `activity`,
`await signal`, `timer`, `Continuum.now/0`, even `Continuum.log/2` — goes through
one bridge that either **replays** the recorded result sitting at the current
cursor, or **suspends** the run to go produce a new one. Re-executing the body
from the top is therefore free of side effects right up to the point where
history ends.

That only holds if the code *between* effects is deterministic, so Continuum
does not ask you to be careful about it. A compile-time AST scanner rejects
`DateTime.utc_now/0`, `:rand.uniform/0`, `:ets.*`, `send/2`, `File.*`, `Logger.*`
and friends inside workflow code, each with a remediation hint naming the
replacement. Every cursor position also carries a structured identity
(`{kind, module, function, line, hash, ordinal}`), so editing a workflow under a
run that is mid-flight raises a loud `Continuum.ReplayDriftError` instead of
silently taking a different branch than the one that was journaled.

Postgres is the only moving part — journal, lease store, timer wheel, and signal
bus. Every write is a compare-and-set on a fencing token, so a node you thought
was dead cannot come back and write into the history of a run somebody else has
already taken over.

## What you get

Continuum is to durable execution what Phoenix is to web and Oban is to job
queues: the obvious answer to *"how do I run a multi-step business process that
survives a crash?"* for Elixir-first teams.

- **Straight-line code.** Express orchestration as ordinary Elixir control
  flow — `case`, `with`, comprehensions. Effects go through `activity/2`,
  `await signal`, and `timer`; everything else is pure.
- **Deterministic replay.** A run re-executes from the top on every wake.
  Structured cursor identity means any divergence between replay and the
  original execution surfaces as a loud `Continuum.ReplayDriftError`, never
  silent corruption.
- **One dependency.** Postgres is the only thing you need to operate — it is
  the journal, the lease store, the timer wheel, and the signal bus
  (`LISTEN`/`NOTIFY`).
- **It's just OTP.** Continuum is a supervision tree you add to your own app.
  Crash recovery, leasing, and back-pressure are built on processes, not an
  external coordinator.

**Deliberately out of scope:** polyglot SDKs, cross-language activities, a
separate cluster service, and Kubernetes operators.

## Quickstart

```elixir
defmodule MyApp.OrderFlow do
  use Continuum.Workflow, version: 1

  def run(%{order_id: id, items: items}) do
    {:ok, validated} = activity Validation.check(items)

    {:ok, charge} =
      activity Payments.charge(id, validated.total),
        retry: [max_attempts: 5, backoff: :exponential],
        compensate: {Payments, :refund, [id]}

    case await signal(:fraud_review, timeout: hours(24)) do
      :approved -> activity Fulfillment.ship(id)
      :rejected ->
        compensate(charge)
        {:error, :fraud_rejected}

      :timeout  -> activity Fulfillment.ship(id)
    end
  end
end
```

```elixir
{:ok, run_id} = Continuum.start(MyApp.OrderFlow, %{order_id: "o1", items: [...]})

# from anywhere — durable mailbox, survives restarts
:ok = Continuum.signal(run_id, :fraud_review, :approved)

# blocks via PubSub with poll fallback
{:ok, %{state: :completed, result: result}} = Continuum.await(run_id, 30_000)
```

## Installation

Continuum supports Elixir 1.19 and 1.20. CI exercises the compatibility
endpoints on Elixir 1.19 / OTP 27 / PostgreSQL 14 and Elixir 1.20 / OTP 29 /
PostgreSQL 18, in addition to scheduled failure-injection lanes.

Add Continuum and a Postgres driver to your dependencies:

```elixir
def deps do
  [
    {:continuum, "~> 0.8.1"},
    {:postgrex, "~> 0.19"}
  ]
end
```

Point Continuum at your repo:

```elixir
# config/config.exs
config :continuum, repo: MyApp.Repo, journal: Continuum.Runtime.Journal.Postgres
```

Generate and run the migration:

```bash
mix continuum.gen.migration --repo MyApp.Repo
mix ecto.migrate
```

Add Continuum's runtime children to your supervision tree, **after** your repo:

```elixir
def start(_type, _args) do
  children =
    [
      MyApp.Repo,
      {Phoenix.PubSub, name: MyApp.PubSub}
    ] ++
      Continuum.children() ++
      [MyAppWeb.Endpoint]

  Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
end
```

## Features

### Determinism by construction

- Workflow code is pure-by-construction and re-executed top-to-bottom on every
  wake; only effects produce side-visible work.
- A **compile-time AST scanner** rejects non-deterministic calls
  (`DateTime.utc_now`, `:rand.*`, `:ets.*`, `Process.send`, `Kernel.apply`, …)
  with remediation hints. Helper modules opt in via `use Continuum.Pure` or a
  `config :continuum, trusted_modules: [...]` allowlist.
- Deterministic primitives — `Continuum.now/0`, `today/0`, `uuid4/0`,
  `random/0`, and the `side_effect/1` escape hatch — capture stable cursor
  identity at compile time.

### Durable execution

- **Postgres journal** with lease + fencing-token CAS on every write. A stolen
  lease produces a write failure and terminates the stale engine — it never
  corrupts history.
- **Activity execution** through the built-in worker pool by default, or an
  optional `Continuum.Oban` executor for teams that already operate Oban.
  Continuum keeps retry/timeout policy, idempotency, and fencing-token commits
  in its own durable task table either way.
- **Durable timers and signals** over `pg_notify`/`LISTEN`.
  `await signal(name, timeout: ms)` resolves the signal/timeout race
  deterministically.
- **Crash survival.** Kill the engine pid mid-flight; the dispatcher re-leases
  the run and replay completes from the journaled history. Boot-time recovery
  rescues orphaned runs, tasks, and timers without stealing live remote leases.
- **Cross-run idempotency** keyed on `(activity_module, idempotency_key)`, so
  activities are exactly-once-ish across runs.

### Workflow composition

- **Sagas / compensation** — attach `compensate:` to an activity, then
  `compensate/1` or `compensate_all/0` to roll back completed work in
  deterministic LIFO (or parallel) order.
- **Parent/child workflows** — `await child Mod.run(input)`, `start_child/3`,
  and `await_child/1` for durable fan-out/fan-in.
- **`continue_as_new/1`** — complete the current run and start a successor with
  fresh history for long-running loops.
- **Workflow versioning** — journaled `Continuum.patched?/1` markers for safe
  in-place edits, and content-addressed `(workflow, version_hash)` dispatch that
  leaves a run suspended when code is missing rather than replaying through
  changed logic. A node with the matching version can safely resume it later.

### Operations & observability

- **`Continuum.Observer`** — an optional Phoenix LiveView with a runs index, a
  decoded per-run event timeline, and operator actions for cancelling a run and
  injecting a signal.
- **`Continuum.OpenTelemetry`** — an opt-in bridge that turns Continuum
  telemetry into `run_attempt`/`activity_attempt` spans, linked back through a
  persisted W3C `traceparent`.
- **24+ documented telemetry events** under the `[:continuum, …]` prefix.
- **Operator tooling** — monthly-partitioned events, opt-in history snapshots,
  the read-only `mix continuum.audit`, and dry-run-by-default cleanup tasks.

### Multi-tenancy & clustering

- **Named multi-instance runtimes** via `Continuum.children(name:, repo:)`, each
  bound to its own Ecto repo.
- **Namespaces** — a soft tenant boundary for list/query; single-run operations
  stay keyed by global `run_id`.
- **Search attributes and structured queries** — `attributes:` /
  `Continuum.set_attributes/3` plus `Continuum.list_runs/1,2`.
- **Cluster-aware wake routing** over `:pg` for cross-node wakeups. The Postgres
  lease and fencing token remain the sole authority for writes.

### Testing

`Continuum.Test` provides an in-memory journal for fast unit tests, Postgres
helpers for integration tests, signal/timer injection, golden-history replay,
and an opt-in paranoid re-replay mode that catches divergence.

## Parent/child example

```elixir
defmodule MyApp.BatchFlow do
  use Continuum.Workflow, version: 1

  def run(%{order_ids: ids}) do
    ids
    |> Enum.map(fn id ->
      start_child MyApp.OrderFlow, %{order_id: id}, id: id
    end)
    |> Enum.map(&await_child/1)
  end
end
```

## Observer

The optional `Continuum.Observer` LiveView lists runs, renders the journal
event timeline per run, and includes an operational health panel for
partitions, workflow versions, durable wakes, timers, leases, activities, and
signals. It exposes operator actions for cancelling a run, sending a signal,
and previewing fenced repairs. It is mounted from a host Phoenix router and
ships no authentication of its own — wrap it in your existing admin pipeline.

![Continuum Observer runs index](https://raw.githubusercontent.com/Yyeger/Continuum/main/dev/ui.png)

```elixir
import Continuum.Observer.Router

scope "/admin" do
  pipe_through [:browser, :authenticate_admin]

  continuum_observer "/continuum", instance: :myapp_continuum
end
```

To see the UI locally, run the crash demo's read-only Observer node:

```bash
docker compose up -d
mix continuum.demo --observer     # http://localhost:4000/continuum
```

Leave it running in one terminal and drive `mix continuum.demo` /
`mix continuum.demo --resume` in another: the run appears, stalls with a dead
node's lease on it, and then completes under a different owner. That pane starts
no dispatcher, so it watches rather than resuming the run itself. See
[`guides/observer.md`](https://github.com/Yyeger/Continuum/blob/main/guides/observer.md) for production mount instructions.

## Documentation

Full docs are published on [HexDocs](https://hexdocs.pm/continuum). The guides
cover the entire surface:

- *Your first workflow*
- *Activities, retries, and idempotency* · *Oban activity executor*
- *Determinism rules and replay drift*
- *Sagas and compensation* · *Child workflows* · *Long-running workflows*
- *Patching workflows* · *Workflow versioning*
- *Multi-instance Continuum* · *Clustering* · *Namespaces*
- *Search attributes and structured queries*
- *Operations* · *Auditing* · *Observer* · *Observability (OpenTelemetry)* ·
  *Snapshots*

See [`examples/continuum_example_orders`](https://github.com/Yyeger/Continuum/tree/main/examples/continuum_example_orders)
for a Phoenix app exercising activity → signal/timeout → compensation,
parent/child batches, `continue_as_new`, per-workflow snapshots, namespaces,
the Observer, and OpenTelemetry.

Upgrading? See the [migration guides](https://github.com/Yyeger/Continuum/tree/main/guides/migrations).

## Status

Continuum is **v0.8.1 (pre-1.0)**. The durable engine, determinism enforcement,
workflow composition, observability, and clustering surface are implemented and
covered by tests, including crash-resume, lease-fencing races, and
property-based replay. APIs may still change before 1.0 — pin to a specific
`0.x` in production. See [`CHANGELOG.md`](https://github.com/Yyeger/Continuum/blob/main/CHANGELOG.md) for release history.

## Development

A `docker-compose.yml` brings up Postgres for local development and tests.

```bash
mix deps.get
docker compose up -d                  # Postgres on localhost:5433
mix compile --warnings-as-errors
mix test                              # unit + integration suite
mix test.cluster                      # real :peer cluster tests (run separately)
mix format
mix continuum.demo --help             # the crash-recovery demo
```

## License

Copyright 2026 The Continuum Authors. (yyeger)

Licensed under the [Apache License, Version 2.0](https://github.com/Yyeger/Continuum/blob/main/LICENSE).
