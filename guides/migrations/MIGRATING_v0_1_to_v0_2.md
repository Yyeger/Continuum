# Migrating from v0.1 to v0.2

Continuum v0.2 is a feature release that pays down v0.1 debt and adds the
operability story (Observer, OpenTelemetry, snapshots). Public workflow code
written for v0.1 keeps compiling and running unchanged. Operators and host
applications have a small set of mechanical steps.

## TL;DR

1. Run the new migrations.
2. Optionally opt into per-process repos with `Continuum.children/1`.
3. Optionally opt into snapshots by setting `:snapshot_threshold` to a positive
   integer.
4. Read the *Behavior changes* section before scraping `continuum_events`
   directly.

Workflow modules, activity modules, and the public API surface
(`Continuum.start/3`, `signal/3,4`, `cancel/2`, `await/3`, deterministic
primitives) are unchanged.

## Database Migrations

v0.2 ships four new migrations on top of v0.1. They are designed to run cleanly
on a fresh database and on the current local v0.1 dev/test schema. v0.1 had
no public release, so there is no production-data compatibility promise: if
your local v0.1 database has rows you care about, snapshot the database before
running these.

Fresh installs (`mix continuum.gen.migration`) get the v0.2 shape directly and
do not need to run the four delta migrations.

In order:

1. `20260601000000_partition_continuum_events` — renames the existing
   `continuum_events` to `continuum_events_legacy`, creates a parent table
   `PARTITION BY RANGE (inserted_at)`, creates the current month and the next
   three monthly partitions, copies legacy rows into the partitions, then
   drops the legacy table.
2. `20260601000001_create_continuum_activity_results` — side table for
   activity idempotency keys (`(activity_module, idempotency_key)` PK).
3. `20260601000002_create_continuum_snapshots` — opt-in history compaction
   table.
4. `20260601000003_add_trace_context_to_runs` — nullable `bytea` column on
   `continuum_runs` for persisted W3C `traceparent` values.

Run them in order with `mix ecto.migrate`.

## Behavior Changes That May Surprise Operators

### `continuum_events` PK is now `(run_id, seq, inserted_at)`

Postgres partitioned tables require the partition key in the primary key.
v0.2's `continuum_events` is `PARTITION BY RANGE (inserted_at)` with
`PRIMARY KEY (run_id, seq, inserted_at)`. Continuum still preserves the
v0.1 invariant that `(run_id, seq)` is globally unique per run by locking the
`continuum_runs` row before assigning `seq`. The SQL-level uniqueness was
relaxed to keep the partitioning shape clean — application code never relied
on the wider SQL guarantee.

If you have scripts that assert a unique index on `(run_id, seq)` directly,
update them to look for `(run_id, seq, inserted_at)`.

### `signal_awaited` is not journaled when a signal is already pending

In v0.1, `await signal(:x)` always journaled `signal_awaited` and then
`signal_received` when the signal arrived. In v0.2, when the signal is already
in the durable mailbox at the time of the await, Continuum journals only
`signal_received` and skips the timeout timer entirely.

Old runs that already journaled `signal_awaited` replay unchanged. Drift
detection still works. The change is only visible to operators scraping
`continuum_events` directly or aggregating event-type counts.

If a downstream dashboard counts `signal_awaited` rows as a proxy for "signal
arrivals", switch to `signal_received` (which is the actual arrival).

### Helper-module determinism scan now warns

`use Continuum.Workflow` modules that call into helper modules which are not
stdlib-trusted, not marked `use Continuum.Pure`, and not listed in
`config :continuum, trusted_modules: [...]` now emit a compile-time warning
per untrusted module. To turn the warning into a compile error:

```elixir
config :continuum, untrusted_call_severity: :error
```

See `guides/determinism-rules.md` for the three trust mechanisms.

### `mix continuum.gen.migration` generates the v0.2 shape

Newly generated migrations include the partitioned `continuum_events`, the
`trace_context` column on `continuum_runs`, the activity-results side table,
and the snapshots table. Existing migrations are untouched.

## Optional: Per-Process Repos

v0.2 introduces named Continuum *instances*. Each instance has its own
registry, run supervisor, dispatchers, timer wheel, signal router, lease
heartbeater, snapshotter, and recovery process, bound to its own Ecto repo.

You do not have to opt in. The default instance (`Continuum`) reads
`config :continuum, :repo` exactly as before. To run more than one instance:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      MyApp.Repo,
      MyApp.Billing.Repo
    ] ++ Continuum.children(name: :billing_continuum, repo: MyApp.Billing.Repo)

    Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
  end
end
```

Then pass `instance: :billing_continuum` to `Continuum.start/3`, `signal/4`,
`cancel/2`, and `await/3`. See `guides/multi-instance.md` for the full surface.

## Optional: Snapshots

Snapshots are experimental in v0.2 and disabled by default. To opt in for a
long-history workflow:

```elixir
config :continuum,
  snapshot_threshold: 200,        # take a snapshot every ~200 new events
  snapshot_max_size_bytes: 1_000_000
```

`:infinity` (the default) disables snapshots entirely. See
`guides/snapshots.md` for what snapshots are, what they are not, and when to
turn them on.

## Optional: OpenTelemetry

Continuum core compiles and runs without OpenTelemetry installed. To export
spans, add and configure OTel in your application, then call:

```elixir
{:ok, _handler_id} = Continuum.OpenTelemetry.setup()
```

The bridge produces short `continuum.run_attempt` and `continuum.activity_attempt`
spans; resumed run attempts link back to the original trace via the new
`continuum_runs.trace_context` column. See `guides/observability.md`.

## Optional: Observer

Mount the Phoenix LiveView Observer behind your existing authentication
pipeline:

```elixir
import Continuum.Observer.Router

scope "/admin" do
  pipe_through [:browser, :authenticate_admin]
  continuum_observer "/continuum", instance: :myapp_continuum
end
```

Continuum's own dependencies do not transitively pull in Phoenix LiveView for
host apps. Add `:phoenix_live_view` and `:phoenix_html` to your app if you
mount the Observer. See `guides/observer.md`.

## Partition Maintenance

`continuum_events` is now partitioned monthly. v0.2 ships three Mix tasks for
operator-driven partition maintenance — there is no runtime partition manager:

* `mix continuum.partitions.create [YYYY-MM]` — create a single monthly
  partition. Idempotent.
* `mix continuum.partitions.list` — print current partitions and row counts.
* `mix continuum.partitions.drop_old [--execute]` — report partitions older
  than retention by default; drop them only with `--execute`.

`retention_until` on `continuum_runs` is opt-in and remains `NULL` by default.
`drop_old` is a no-op when no run has retention set.

## What Did Not Change

* The public macro surface: `use Continuum.Workflow`, `activity`,
  `await signal`, `timer`, `seconds`/`minutes`/`hours`/`days`, `Continuum.now/0`,
  `today/0`, `uuid4/0`, `random/0`, `side_effect/1`.
* Activity definition (`use Continuum.Activity`, `idempotency_key/1`).
* Run lifecycle, lease/fencing semantics, drift detection, and the engine
  replay loop. v0.1 runs in flight at the time of upgrade resume on v0.2 code
  and complete the same way.
* Telemetry event names. v0.2 adds new events
  (`[:continuum, :snapshot, :taken|:skipped]`,
  `[:continuum, :activity, :idempotency_hit]`); the v0.1 names are unchanged.
