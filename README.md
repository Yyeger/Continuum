# Continuum

**English** | [简体中文](./README.zh-CN.md)

[![CI](https://github.com/Yyeger/Continuum/actions/workflows/ci.yml/badge.svg)](https://github.com/Yyeger/Continuum/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/continuum.svg)](https://hex.pm/packages/continuum)
[![Documentation](https://img.shields.io/badge/hexdocs-docs-8e44ad.svg)](https://hexdocs.pm/continuum)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)

**Continuum is a durable execution engine for Elixir.** Write a multi-step
business process as straight-line Elixir code; failures, restarts, and node
death cause the workflow to resume *exactly where it left off* with identical
state, by replaying its event history through the same pure orchestration code.

It is OTP-native and Postgres-backed — no separate cluster service, no paid
SaaS dependency, no polyglot SDK. Continuum lives in your application's
supervision tree and uses the database you already run.

## Why Continuum

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

Add Continuum and a Postgres driver to your dependencies:

```elixir
def deps do
  [
    {:continuum, "~> 0.6"},
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
  marks missing code `:stuck_unknown_version` rather than replaying through
  changed logic.

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
  `Continuum.set_attributes/3` plus `Continuum.query/1,2`.
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
event timeline per run, and exposes operator actions for cancelling a run and
sending a signal. It is mounted from a host Phoenix router and ships no
authentication of its own — wrap it in your existing admin pipeline.

![Continuum Observer runs index](./dev/ui.png)

```elixir
import Continuum.Observer.Router

scope "/admin" do
  pipe_through [:browser, :authenticate_admin]

  continuum_observer "/continuum", instance: :myapp_continuum
end
```

To see the UI locally, the repo bundles a self-contained demo:

```bash
docker compose up -d
MIX_ENV=test iex -S mix run dev/observer_demo.exs
# then open http://localhost:4000/continuum
```

The demo seeds three runs in different states and prints iex helpers for
spawning more, sending signals, and cancelling. See
[`guides/observer.md`](./guides/observer.md) for production mount instructions.

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

See [`examples/continuum_example_orders`](./examples/continuum_example_orders)
for a Phoenix app exercising activity → signal/timeout → compensation,
parent/child batches, `continue_as_new`, per-workflow snapshots, namespaces,
the Observer, and OpenTelemetry.

Upgrading? See the [migration guides](./guides/migrations/) .

## Status

Continuum is **v0.6.0 (pre-1.0)**. The durable engine, determinism enforcement,
workflow composition, observability, and clustering surface are implemented and
covered by tests, including crash-resume, lease-fencing races, and
property-based replay. APIs may still change before 1.0 — pin to a specific
`0.x` in production. See [`CHANGELOG.md`](./CHANGELOG.md) for release history.

## Development

A `docker-compose.yml` brings up Postgres for local development and tests.

```bash
mix deps.get
docker compose up -d                  # Postgres on localhost:5432
mix compile --warnings-as-errors
mix test                              # unit + integration suite
mix test.cluster                      # real :peer cluster tests (run separately)
mix format
```

## License

Copyright 2026 The Continuum Authors. (yyeger)

Licensed under the [Apache License, Version 2.0](./LICENSE).
