# Clustering

Continuum v0.5 uses BEAM distribution for low-latency routing between nodes. It
does not start or configure a clustering transport for your application. Use the
tooling you already operate, such as DNS clustering, libcluster, releases with
known node names, or your platform's service discovery, and make sure
`Node.list/0` contains the other application nodes.

## Routing Model

Continuum starts a `:pg` scope named `:continuum`. Every live workflow engine
joins the group `{instance_name, run_id}` while it owns a run. When a signal,
timer, or other wake reaches a node that does not own the local engine,
`Continuum.Runtime.Engine.wake/2` first checks the local `Registry`, then forwards
the wake to a `:pg` member if one is present.

`:pg` is advisory. The Postgres lease and fencing token remain the authority for
journal writes. If a stale node is still listed in `:pg`, its next write fails the
lease check and the heartbeater stops that engine.

## Failure Recovery

Abrupt node failure recovery is lease-expiry based. A run abandoned by a dead
node becomes claimable after its lease TTL expires, then any node's dispatcher
can resume it from the journal. The default TTL is 30 seconds, so worst-case
resume latency after node death is roughly that TTL plus the dispatcher poll
interval.

Planned supervisor shutdowns drain instead of waiting for that TTL. The
heartbeater pauses new run claims, renews tracked leases once, and gives engines
up to five seconds to finish their current step or reach suspension. It then
stops any remaining engine before atomically fencing and releasing its lease,
so another node can take over immediately. Configure the bound per instance:

```elixir
Continuum.children(
  repo: MyApp.Repo,
  heartbeater: [drain_timeout_ms: 10_000]
)
```

Keep the Continuum children under your release's normal application supervisor;
its shutdown sequence is the pre-stop hook. Abrupt VM or machine death still
falls back to lease expiry. With the built-in activity executor, configure the
platform termination grace period to exceed the run-drain deadline by at least
seven seconds so activity workers can stop first.

Orchestrators can initiate the handoff before supervisor termination with
`Continuum.drain/1`, and use `Continuum.ready?/1`, `Continuum.drained?/1`, or the
detailed `Continuum.readiness/1` state for readiness checks. The lifecycle is
per instance; pass `instance: name` for named runtimes.

Activity tasks use the same rule. Boot recovery only requeues leased activity
tasks after their task lease has expired, so a newly booted node does not steal
work from a live worker on another node.

## Observability

Continuum emits `[:continuum, :run, :forwarded]` when a wake is forwarded through
`:pg`, with `:from_node` and `:to_node` metadata. It emits
`[:continuum, :lease, :lost]` when a heartbeater discovers that another owner has
stolen a run lease. Graceful shutdown emits `[:continuum, :lease,
:drain_started]` and `[:continuum, :lease, :drain_completed]`; alert when the
completed event reports a non-zero `unreleased_count`.

Partition maintenance uses a database-scoped advisory transaction lock, so
multiple nodes may schedule the same horizon safely while only the lock holder
performs DDL. Pass `partition_maintainer: [...]` to `Continuum.children/1` only
when the runtime database role has DDL permission. Maintenance emits
`[:continuum, :partition, :maintained]` or
`[:continuum, :partition, :maintenance_failed]`.

## Test Harness

The repository includes `mix test.cluster`, which runs `test/cluster` with real
`:peer` nodes against the test Postgres database. These tests are excluded from
ordinary `mix test` because Ecto SQL Sandbox transactions do not span BEAM nodes.
