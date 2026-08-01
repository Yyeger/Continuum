# Migrating from v0.6.4 to v0.7.0

v0.7.0 adds product-facing durable ingress, pruning, schedules, activity
progress and queues, signal contracts, replay-safe logging, typed public
results, and centralized configuration validation.

## Deployment order

1. Generate and apply the additive delta while v0.6.4 nodes are still serving.
2. Deploy v0.7.0 and run the workflow-version preflight before accepting work.
3. Roll nodes normally; all new workflow features are opt-in.

```bash
mix continuum.gen.migration --from 0.6.4 --repo MyApp.Repo
mix ecto.migrate
mix continuum.versions.check --strict --repo MyApp.Repo
```

The generated migration adds:

- root-start idempotency and chain-scoped signal delivery columns/indexes
- `continuum_run_ingress_keys`, which can preserve ingress deduplication after
  run history is pruned
- activity heartbeat details, queue, and priority fields, plus the queue-aware
  pickup index
- `continuum_schedules` and its due-work index

The migration disables its DDL transaction and migration lock because the
activity pickup index is rebuilt with `CONCURRENTLY`. Coordinate that migration
with your normal production migration runner so only one copy executes.

## Compatibility notes

Existing calls remain valid unless they relied on silently ignored runtime
configuration. `Continuum.children/1` now rejects unknown, duplicate, and
invalid top-level or child-component options before returning specs. Run
`mix continuum.config.docs --check` to verify the generated reference.

`Continuum.get_run/2`, `Continuum.query/1`, Observer event pages, and
`Continuum.readiness/1` now return public structs. Ordinary map patterns and
dot access remain compatible; exact plain-map key comparisons should migrate
to struct patterns or call `Map.from_struct/1` at the boundary.

Workflows without `signals:` keep open signal delivery. Declaring `signals:`
closes the contract and changes the workflow version hash. Deploy the new
version alongside any old entrypoints still required by nonterminal runs.

Namespace guards are opt-in: omitting `namespace:` preserves global UUID-based
lookup behavior. Queue limits apply to the built-in activity executor; Oban
instances continue to use Oban queue controls.

## Rollback

The schema is additive and v0.6.4 ignores the new tables and columns, so code
can roll back without running the migration `down/0`. Do not remove the schema
while v0.7.0 nodes or jobs may still write idempotent ingress, progress,
schedules, or queue metadata.
