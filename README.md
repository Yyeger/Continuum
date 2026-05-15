# Continuum

**English** | [简体中文](./README.zh-CN.md)

OTP-native durable execution engine for Elixir — Postgres-backed, deterministic
replay, single dependency. Write a multi-step business process as straight-line
Elixir code. Failures, restarts, and node death cause the workflow to resume
exactly where it left off.

> **Status:** v0.1 (pre-1.0). The full v0.1 surface — replay, lease/fencing,
> durable timers, durable signals with timeout, activity worker pool, recovery,
> generators, guides — is implemented and tested. APIs may still change before
> 1.0; pin to a specific 0.x in production.

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
    {:continuum, "~> 0.1"},
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

## What ships in v0.1

- **Deterministic replay** with structured cursor identity. Replay drift
  produces `Continuum.ReplayDriftError`, never silent corruption.
- **Compile-time AST scan** rejects non-deterministic calls (`DateTime.utc_now`,
  `:rand.*`, `:ets.*`, `Process.send`, `Kernel.apply`, …) with remediation
  hints. Helpers opt in via `use Continuum.Pure`.
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

## Observer (v0.2, in progress)

The optional `Continuum.Observer` LiveView UI lists runs, renders the journal
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

To see the UI locally before it ships in v0.2, the repo bundles a
self-contained demo:

```bash
docker compose up -d
MIX_ENV=test iex -S mix run dev/observer_demo.exs
# then open http://localhost:4000/continuum
```

The demo seeds three runs in different states and prints iex helpers for
spawning more, sending signals, and cancelling. See
[`guides/observer.md`](./guides/observer.md) for production mount
instructions.

## What's deliberately out of v0.1

Compensation/saga DSL, parent/child workflows, `continue_as_new`, search
attributes, cluster distribution, real `patched?/1`, Oban adapter. Each is on
the roadmap; see [`ROADMAP.md`](./ROADMAP.md) for the phased plan.

## Guides

The ExDoc guides cover the v0.1 path plus v0.2 in progress:

- *Your first workflow*
- *Activities, retries, and idempotency*
- *Idempotency* (cross-run scope, residual crash window)
- *Determinism rules and replay drift* (now covers helper-module warnings and
  `trusted_modules`)
- *Multi-instance Continuum* (named instances with `Continuum.children/1`)
- *Observer* (v0.2)
- *Observability / OpenTelemetry bridge* (v0.2)
- *Experimental snapshots* (opt-in long-history compaction)

Upgrading from v0.1? See
[`MIGRATING_v0_1_to_v0_2.md`](./MIGRATING_v0_1_to_v0_2.md).

See [`examples/continuum_example_orders`](./examples/continuum_example_orders)
for a Phoenix app exercising activity → signal/timeout → activity, with a
manual crash-resume smoke script in `scripts/smoke_test.exs`.

## License

Apache-2.0.
