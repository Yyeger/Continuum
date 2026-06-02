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

Node failure recovery is lease-expiry based in v0.5.0. A run abandoned by a dead
node becomes claimable after its lease TTL expires, then any node's dispatcher can
resume it from the journal. The default TTL is 30 seconds, so worst-case resume
latency after node death is roughly that TTL plus the dispatcher poll interval.

Activity tasks use the same rule. Boot recovery only requeues leased activity
tasks after their task lease has expired, so a newly booted node does not steal
work from a live worker on another node.

## Observability

Continuum emits `[:continuum, :run, :forwarded]` when a wake is forwarded through
`:pg`, with `:from_node` and `:to_node` metadata. It emits
`[:continuum, :lease, :lost]` when a heartbeater discovers that another owner has
stolen a run lease.

## Test Harness

The repository includes `mix test.cluster`, which runs `test/cluster` with real
`:peer` nodes against the test Postgres database. These tests are excluded from
ordinary `mix test` because Ecto SQL Sandbox transactions do not span BEAM nodes.
