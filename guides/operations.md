# Operations

Continuum's runtime tables are intentionally append-heavy. v0.4 adds two
dry-run-by-default cleanup tasks for data that is safe to prune after operators
decide their retention policy.

## Workflow Version Registry GC

`continuum_workflow_versions` records loaded workflow hashes so Postgres-backed
runs can resume through the exact entrypoint they started on.

List deletable rows:

```bash
mix continuum.gc_versions --repo MyApp.Repo
```

Delete them:

```bash
mix continuum.gc_versions --repo MyApp.Repo --execute
```

A row is a deletion candidate only when:

- no loaded workflow in the current BEAM has that `(workflow, version_hash)`
- no non-terminal run references that hash

Non-terminal includes `running`, `suspended`, and `stuck_unknown_version`. The
last state is deliberately pinned so a missing-code run is not made harder to
recover.

Run this after deploys once the old release is no longer needed, then again
after long-running old-version runs have completed or been cancelled.

## Continued-Chain Archival

`continue_as_new` keeps each physical run's history bounded, but the chain still
contains one completed row per cycle. `continuum.archive_continued_chains`
deletes expired, non-tail cycles and their dependent rows.

Dry run:

```bash
mix continuum.archive_continued_chains --repo MyApp.Repo --older-than 30d
```

Execute:

```bash
mix continuum.archive_continued_chains --repo MyApp.Repo --older-than 30d --execute
```

A run is eligible only when it is:

- completed
- older than the `--older-than` cutoff
- past `retention_until`
- not the tail of its `continue_as_new` chain
- not part of a child chain whose parent is still non-terminal

The task deletes dependent rows from events, snapshots, timers, signals,
activity tasks, and activity results before deleting the run rows.

## Event Partitions

The partition tasks remain the primary maintenance surface for
`continuum_events`:

```bash
mix continuum.partitions.create --repo MyApp.Repo --months 3 --execute
mix continuum.partitions.list --repo MyApp.Repo
mix continuum.partitions.drop_old --repo MyApp.Repo --older-than 180d --execute
```

Keep partition creation ahead of traffic. Treat `drop_old` like any destructive
retention operation: run without `--execute` first, review the output, then run
with `--execute` from an operator-controlled job.
