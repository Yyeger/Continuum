# Continuum

**English** | [简体中文](./README.zh-CN.md)

OTP-native durable execution engine for Elixir — Postgres-backed, deterministic
replay, single dependency. Write a multi-step business process as straight-line
Elixir code. Failures, restarts, and node death cause the workflow to resume
exactly where it left off.

> **Status:** v0.4 (pre-1.0). v0.4 stabilizes snapshots, adds workflow-level
> snapshot thresholds, cleanup Mix tasks, parallel compensation, and generated
> version entrypoints. APIs may still change before 1.0; pin to a specific 0.x
> in production. Upgrading from v0.3? See
> [`MIGRATING_v0_3_to_v0_4.md`](./MIGRATING_v0_3_to_v0_4.md).

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

```elixir
def deps do
  [
    {:continuum, "~> 0.4"},
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

## What ships in v0.3

The v0.1/v0.2 core remains:

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

v0.3 adds:

- **Compensation / saga DSL** — attach `compensate:` to an activity, then use
  `compensate/1` or `compensate_all/0` to roll back completed work in a
  deterministic LIFO order. See [`guides/sagas.md`](./guides/sagas.md).
- **Parent/child workflows** — `await child Mod.run(input)`, `start_child/3`,
  and `await_child/1` for durable composition and fan-out/fan-in. See
  [`guides/child-workflows.md`](./guides/child-workflows.md).
- **`continue_as_new/1`** — complete the current run and start a successor with
  fresh history for long-running loops. See
  [`guides/long-running-workflows.md`](./guides/long-running-workflows.md).
- **Journaled `Continuum.patched?/1`** — safe in-place patch markers for
  compatible workflow edits. See [`guides/patching.md`](./guides/patching.md).
- **Content-addressed workflow dispatch** — resumes resolve the run's stored
  `(workflow, version_hash)` through `Continuum.VersionRegistry` and mark
  missing code as `:stuck_unknown_version` instead of silently replaying through
  changed code. See
  [`guides/workflow-versioning.md`](./guides/workflow-versioning.md).

v0.4 adds:

- **Stable snapshot payload format** — snapshots use a versioned envelope and
  store `format_version` in `continuum_snapshots`. Workflows can opt in with
  `snapshot_threshold:` on `use Continuum.Workflow`.
- **Operator cleanup tasks** — `mix continuum.gc_versions` and
  `mix continuum.archive_continued_chains` are dry-run by default and documented
  in [`guides/operations.md`](./guides/operations.md).
- **Parallel compensation** — `compensate_all(mode: :parallel)` schedules all
  pending compensations before suspending. The no-arg form remains sequential
  LIFO.
- **Generated workflow entrypoints** — `use Continuum.Workflow` creates a hidden
  `V_<hash>` module for durable version dispatch while keeping the public module
  as the start target.

## Parent/Child Example

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
[`guides/observer.md`](./guides/observer.md) for production mount
instructions.

## What's deliberately out of v0.3

Replay-stepping debugger inside the Observer, search attributes, cluster
distribution, `mix continuum.audit`, and the Oban adapter. Each is on the
roadmap; see [`ROADMAP.md`](./ROADMAP.md) for the phased plan.

## Guides

The ExDoc guides cover the current surface:

- *Your first workflow*
- *Activities, retries, and idempotency*
- *Idempotency* (cross-run scope, residual crash window)
- *Determinism rules and replay drift* (helper-module warnings and
  `trusted_modules`)
- *Multi-instance Continuum* (named instances with `Continuum.children/1`)
- *Sagas and compensation*
- *Child workflows*
- *Long-running workflows* (`continue_as_new`)
- *Patching workflows*
- *Workflow versioning*
- *Operations*
- *Observer*
- *Observability / OpenTelemetry bridge*
- *Snapshots* (opt-in long-history compaction)

Upgrading from v0.3? See
[`MIGRATING_v0_3_to_v0_4.md`](./MIGRATING_v0_3_to_v0_4.md). Upgrading from
v0.2 first? See
[`MIGRATING_v0_2_to_v0_3.md`](./MIGRATING_v0_2_to_v0_3.md). Upgrading from v0.1
first? See
[`MIGRATING_v0_1_to_v0_2.md`](./MIGRATING_v0_1_to_v0_2.md).

See [`examples/continuum_example_orders`](./examples/continuum_example_orders)
for a Phoenix app exercising activity -> signal/timeout -> compensation,
parent/child batches, `continue_as_new`, per-workflow snapshots, Observer, and
OpenTelemetry.

## License

Apache-2.0.
