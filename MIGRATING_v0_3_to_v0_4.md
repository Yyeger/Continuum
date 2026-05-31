# Migrating From v0.3 To v0.4

v0.4 stabilizes the v0.3 durability surfaces rather than changing the core
programming model. The release adds snapshot format versioning, workflow-level
snapshot thresholds, cleanup Mix tasks, parallel compensation, and generated
workflow-version entrypoints.

## Database Migration

Generate a fresh migration:

```bash
mix continuum.gen.migration --repo MyApp.Repo
mix ecto.migrate
```

The v0.4 delta is:

- `continuum_snapshots.format_version smallint NOT NULL DEFAULT 1`
- `continuum_runs_correlation_completed_idx` on `(correlation_id, completed_at)`
  where `correlation_id IS NOT NULL`

Existing v0.3 snapshot payloads are unversioned. v0.4 reads them as format
version `1`; new snapshots are written in the versioned envelope and store the
same format in the table column.

## Behavior Changes

Snapshots are now a supported opt-in feature. They still default to off with
`snapshot_threshold: :infinity`, but the payload format is versioned and covered
by compatibility tests.

Workflows can override the app snapshot threshold:

```elixir
defmodule MyApp.SubscriptionFlow do
  use Continuum.Workflow, version: 1, snapshot_threshold: 500
end
```

`mix continuum.gc_versions` and `mix continuum.archive_continued_chains` are
available for operator-controlled cleanup. Both are dry-run-only unless
`--execute` is passed.

`compensate_all(mode: :parallel)` schedules all pending compensations before
suspending. The no-argument `compensate_all()` remains sequential LIFO.

Workflows that call `compensate_all` and contain activities without
`compensate:` now emit a compile-time warning. Add a compensation, or mark the
activity intentionally unrolled-back:

```elixir
activity MyApp.Analytics.record(input), compensate: :none
```

`use Continuum.Workflow` now generates a hidden `V_<hash>` module for the
compiled workflow body. Start runs with your public workflow module as before;
durable Postgres dispatch executes and resumes through the generated
hash-specific entrypoint. Reflection tools may see modules like
`MyApp.Flow.V_<hash>`; they are `@moduledoc false`.

## Verification Checklist

Before deploying v0.4:

- run `mix compile --warnings-as-errors`
- run `mix test`
- run `CONTINUUM_PARANOID=1 mix test`
- run `mix test --seed` across your usual deterministic seed set
- dry-run `mix continuum.gc_versions --repo MyApp.Repo`
- dry-run `mix continuum.archive_continued_chains --repo MyApp.Repo --older-than Nd`

If you use snapshots, replay representative histories with snapshots both
enabled and disabled before enabling the snapshotter in production.
