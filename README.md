# Continuum

**English** | [简体中文](./README.zh-CN.md)

OTP-native durable execution engine for Elixir — Postgres-backed, deterministic
replay, single dependency. Write a multi-step business process as straight-line
Elixir code. Failures, restarts, and node death cause the workflow to resume
exactly where it left off.

> **Status:** v0.2 (pre-1.0). v0.1's replay/lease/timer/signal/worker-pool core
> is unchanged. v0.2 adds an optional Phoenix LiveView **Observer**, an opt-in
> **OpenTelemetry** bridge, **named multi-instance** supervision, and
> experimental opt-in **history snapshots**. It also pays down the named v0.1
> debts: monthly partitioning, activity-idempotency side table, ETS-cached
> TimerWheel, per-process repos, helper-module AST scan, persisted trace
> context. APIs may still change before 1.0; pin to a specific 0.x in
> production. Upgrading from v0.1? See
> [`MIGRATING_v0_1_to_v0_2.md`](./MIGRATING_v0_1_to_v0_2.md).

## Quickstart

```elixir
defmodule MyApp.OrderFlow do
  use Continuum.Workflow, version: 1

  def run(%{order_id: id, items: items}) do
    {:ok, validated} = activity Validation.check(items)

    {:ok, charge} =
      activity Payments.charge(id, validated.total),
        retry: [max_attempts: 5, backoff: :exponential]

    case await signal(:fraud_review, timeout: hours(24)) do
      :approved -> activity Fulfillment.ship(id)
      :rejected -> {:error, %{charge: charge, reason: :fraud_rejected}}
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

```elixir
def deps do
  [
    {:continuum, "~> 0.2"},
    {:postgrex, "~> 0.19"}
  ]
end
```

Configure your repo:

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

## What ships in v0.2

The v0.1 core is unchanged:

- **Deterministic replay** with structured cursor identity. Replay drift
  produces `Continuum.ReplayDriftError`, never silent corruption.
- **Compile-time AST scan** rejects non-deterministic calls (`DateTime.utc_now`,
  `:rand.*`, `:ets.*`, `Process.send`, `Kernel.apply`, …) with remediation
  hints. Helpers opt in via `use Continuum.Pure`. v0.2 also warns on calls into
  unmarked helper modules (configurable via
  `config :continuum, untrusted_call_severity: :warn | :error`) and accepts an
  app-env allowlist (`config :continuum, trusted_modules: [...]`).
- **Postgres journal** with lease + fencing-token CAS on every write. Stolen
  leases produce write failures and terminate the stale engine.
- **Built-in activity worker pool** (no Oban dependency). `FOR UPDATE SKIP
  LOCKED` claim, exponential backoff, per-task fencing, atomic
  result-and-task commit.
- **Durable timers** and **durable signals** via `pg_notify` + `LISTEN`.
  `await signal(name, timeout: ms)` resolves the signal/timeout race
  deterministically.
- **Boot-time recovery** rescues orphaned runs, activity tasks, and due timers
  without stealing live remote leases.
- **Crash survival** — kill the engine pid mid-flight; the dispatcher re-leases
  the run and replay completes from the journaled history.
- **Generators**: `mix continuum.gen.{migration,workflow,activity}`.
- **`Continuum.Test`** — in-memory journal for fast unit tests, Postgres
  helpers for integration tests, signal/timer injection, golden-history replay.
- **24+ telemetry events** under the `[:continuum, …]` prefix.

v0.2 adds:

- **`Continuum.Observer`** — optional Phoenix LiveView UI: runs index with
  search and pagination, run detail with decoded event timeline, operator
  actions for cancelling a run and sending a JSON signal. Mounted from your
  router; host app owns auth. See the Observer section below.
- **`Continuum.OpenTelemetry.setup/1`** — opt-in bridge that turns Continuum
  telemetry into short `continuum.run_attempt` and `continuum.activity_attempt`
  spans, linked back to the persisted W3C `traceparent` in
  `continuum_runs.trace_context`. Continuum still compiles without any
  OpenTelemetry packages.
- **Named multi-instance supervision** via `Continuum.children(name: ..., repo: ...)`
  and `instance: ...` on `start/3`, `signal/4`, `cancel/2`, `await/3`. The
  default `Continuum` instance is unchanged.
- **Experimental, opt-in history snapshots** — `continuum_snapshots`,
  `Continuum.Snapshot`, `Continuum.Runtime.Snapshotter`, compacted-prefix
  replay validation. Default `snapshot_threshold: :infinity` (off); opt in with
  a positive integer after reading `guides/snapshots.md`.
- **Monthly partitioning** for `continuum_events`, with operator Mix tasks
  `mix continuum.partitions.{create,list,drop_old}` (`--execute` opt-in).
- **Cross-run activity idempotency** through `continuum_activity_results`,
  keyed on `(activity_module, idempotency_key)`.
- **ETS-cached `TimerWheel`** with `pg_notify`-driven reschedule.
- **Per-process repo threading** through `Continuum.children/1`.
- **Persisted W3C `traceparent`** on `continuum_runs.trace_context`.

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
[`guides/observer.md`](./guides/observer.md) for production mount
instructions.

## What's deliberately out of v0.2

Compensation/saga DSL, parent/child workflows, `continue_as_new`, real
`patched?/1` with journaling, replay-stepping debugger inside the Observer,
search attributes, cluster distribution, `mix continuum.audit`, and the Oban
adapter. Each is on the roadmap; see [`ROADMAP.md`](./ROADMAP.md) for the
phased plan.

## Guides

The ExDoc guides cover the v0.2 surface:

- *Your first workflow*
- *Activities, retries, and idempotency*
- *Idempotency* (cross-run scope, residual crash window)
- *Determinism rules and replay drift* (helper-module warnings and
  `trusted_modules`)
- *Multi-instance Continuum* (named instances with `Continuum.children/1`)
- *Observer*
- *Observability / OpenTelemetry bridge*
- *Experimental snapshots* (opt-in long-history compaction)

Upgrading from v0.1? See
[`MIGRATING_v0_1_to_v0_2.md`](./MIGRATING_v0_1_to_v0_2.md).

See [`examples/continuum_example_orders`](./examples/continuum_example_orders)
for a Phoenix app exercising activity → signal/timeout → activity, with a
manual crash-resume smoke script in `scripts/smoke_test.exs`.

## License

Apache-2.0.
