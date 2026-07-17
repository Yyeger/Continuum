# Operations

Continuum's runtime tables are intentionally append-heavy. v0.4 adds two
dry-run-by-default cleanup tasks for data that is safe to prune after operators
decide their retention policy.

## Operational Health and Repair

`Continuum.Health.report/1` is the shared health source used by the Mix task
and Observer. It covers event partition horizon, loaded and durable workflow
versions, active-run wait reasons and age, lost durable wakes, overdue timers,
run lease age and heartbeat lag, activity queues/retries/dead letters, and the
undelivered signal backlog.

```elixir
{:ok, report} = Continuum.Health.report(instance: :myapp_continuum)
report.status #=> :ok | :degraded
```

The command supports text and JSON output. `--strict` exits non-zero when the
report is degraded.

```bash
mix continuum.health --repo MyApp.Repo
mix continuum.health --repo MyApp.Repo --instance billing_continuum
mix continuum.health --repo MyApp.Repo --format json
mix continuum.health --repo MyApp.Repo --strict
```

When `--instance` is omitted, the task resolves the running default instance
for that repo (or the only running named instance). Pass `--instance` when
multiple named runtimes share one repo.

Repairs are dry runs by default and must carry the authority observed in the
report. Review the plan, then repeat it with `--execute`:

```bash
# Arm a durable wake without overwriting a newer lease epoch.
mix continuum.health --repo MyApp.Repo --repair wake --target RUN_ID \
  --lease-token EPOCH
mix continuum.health --repo MyApp.Repo --repair wake --target RUN_ID \
  --lease-token EPOCH --execute

# Release only an already-expired run lease.
mix continuum.health --repo MyApp.Repo --repair release --target RUN_ID \
  --lease-owner OWNER --lease-token EPOCH --execute

# Requeue only the observed expired task claim.
mix continuum.health --repo MyApp.Repo --repair requeue --target TASK_ID \
  --lease-owner OWNER --attempt ATTEMPT --execute

# Retry durable registration for every loaded hash of a workflow.
mix continuum.health --repo MyApp.Repo --repair retry \
  --target MyApp.OrderFlow --execute

# Acknowledge exactly the condition fingerprint shown in the report.
mix continuum.health --repo MyApp.Repo --repair review --target SUBJECT_ID \
  --finding-type FINDING --fingerprint SHA256 --reviewed-by oncall \
  --reason "investigated" --execute
```

The `retry` action re-attempts durable registration for a loaded workflow. The
`review` action stores a condition fingerprint, so recurrence with a new lease
epoch, attempt, timestamp, or error becomes unreviewed again. Applied repairs
emit `[:continuum, :health, :repaired]` telemetry.

## Graceful Shutdown

Stopping the supervisor that owns `Continuum.children/1` performs a bounded
lease drain. New run claims pause first, the heartbeater remains alive while
Postgres-backed and in-memory engines finish their current step, and each
durable engine is stopped before its fenced lease is released. This lets
another node claim the run without waiting for the lease TTL.

The default drain deadline is five seconds. Increase it for workflows whose
individual deterministic steps legitimately take longer:

```elixir
Continuum.children(
  repo: MyApp.Repo,
  heartbeater: [drain_timeout_ms: 10_000]
)
```

Your deployment platform's termination grace period must also cover activity
worker shutdown. With the built-in executor defaults, allow at least seven
seconds beyond the run-drain deadline (12 seconds total with the default).
If the VM is killed before supervision can shut down, ordinary lease-expiry
recovery remains the fallback.

For orchestrated deployments, mark the instance unready and begin the same
bounded handoff before stopping its supervisor:

```elixir
Continuum.ready?()
#=> true

{:ok, summary} = Continuum.drain(timeout: 10_000)
Continuum.drained?()
#=> true
```

Named runtimes accept `instance: :billing_continuum`. `readiness/1` returns the
full `:ready | :draining | :drained | :degraded | :not_started` lifecycle state,
active-run and pending-claim counts, and the last drain summary. A drain marks
readiness false before pausing claims, is safe to call repeatedly, and never
waits beyond its deadline plus the bounded engine-stop allowance. Do not route
new service traffic to a node after its readiness becomes false.

## Catch-up Backstop

The signal router periodically recovers notifications missed after durable
signal, activity, compensation, and timer writes. Each node snapshots only the
run IDs in its local engine registry, checks active live leases in bounded
database pages, and leaves unowned runs to the dispatcher. Tune the page size
without changing the polling interval:

```elixir
config :continuum, :signal_router,
  catch_up_interval_ms: 30_000,
  catch_up_batch_size: 500
```

Existing databases should add the active-wakeup index with a concurrent
migration; newly generated Continuum migrations include it automatically:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS continuum_runs_catch_up_idx
ON continuum_runs (next_wakeup_at, lease_expires_at, id)
WHERE state IN ('running', 'suspended')
  AND lease_owner IS NOT NULL
  AND lease_token IS NOT NULL
  AND next_wakeup_at IS NOT NULL;
```

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

Fresh migrations create four monthly partitions plus
`continuum_events_default`. The default partition prevents an exhausted
horizon from turning event appends into an outage, but it is an emergency
buffer rather than permanent storage. Health is degraded whenever it contains
rows.

Maintain a horizon explicitly from a release task or operator job:

```elixir
{:ok, summary} = Continuum.Partitions.ensure(instance: Continuum, months: 6)
```

The operation uses the PostgreSQL database clock, takes a database-scoped
advisory transaction lock, and is safe to invoke concurrently from cluster
nodes. When a missing month already has overflow rows, it locks the event
parent, moves those rows aside transactionally, creates the partition, and
routes them back into it. A failure rolls the entire rollover back.

Applications whose runtime database role has DDL permission can opt into a
scheduled pass:

```elixir
Continuum.children(
  repo: MyApp.Repo,
  partition_maintainer: [months: 6, interval_ms: :timer.hours(6)]
)
```

The maintainer is disabled by default because many production applications
separate migration and runtime roles. In that setup, invoke
`Continuum.Partitions.ensure/1` from a release task using the migration role.

The Mix tasks provide the equivalent dry-run-first operator surface:

```bash
mix continuum.partitions.create --repo MyApp.Repo --months 3 --execute
mix continuum.partitions.list --repo MyApp.Repo
mix continuum.partitions.drop_old --repo MyApp.Repo --older-than 180d --execute
```

`continuum.partitions.create` delegates execution to the same cluster-safe API.
Treat `drop_old` like any destructive retention operation: run without
`--execute` first, review the output, then run with `--execute` from an
operator-controlled job. Retention deletion is intentionally never performed
by the scheduled maintainer.

Alert on `[:continuum, :partition, :maintenance_failed]`, missing horizon
months, or a non-zero default-partition row count. Successful/skipped passes
emit `[:continuum, :partition, :maintained]` with created, moved, and overflow
row counts.
