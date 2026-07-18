# Migrating from v0.6.1 to v0.6.2

v0.6.2 is an additive operational release. It adds first-class health and
fenced repairs, automated partition maintenance with a default overflow safety
partition, graceful drain and readiness APIs, and audited activity
dead-letter/manual-retry operations. It does not change the workflow or
snapshot history formats.

## Upgrade order

1. Apply the schema changes below while v0.6.1 is still running. They are
   additive and the old runtime ignores them.
2. Deploy v0.6.2 and restart the Continuum runtime normally.
3. Run `mix continuum.health --repo MyApp.Repo` and resolve any degraded
   findings before enabling automated partition maintenance or executing an
   activity operation.

No configuration change is required. Partition maintenance remains opt-in,
and all repair and activity-operation commands remain dry runs unless their
explicit execution flag is passed.

## Schema changes

Add two nullable columns to `continuum_runs`:

```sql
ALTER TABLE continuum_runs ADD COLUMN lease_acquired_at timestamptz;
ALTER TABLE continuum_runs ADD COLUMN lease_heartbeat_at timestamptz;
```

They record the acquisition time of the current fencing epoch and its latest
successful heartbeat. Existing live leases can be backfilled conservatively:

```sql
UPDATE continuum_runs
SET lease_acquired_at = COALESCE(lease_acquired_at, started_at),
    lease_heartbeat_at = COALESCE(
      lease_heartbeat_at,
      lease_expires_at - interval '30 seconds'
    )
WHERE lease_owner IS NOT NULL;
```

Create the review table used for fingerprinted operator acknowledgements:

```sql
CREATE TABLE continuum_health_reviews (
  finding_type text NOT NULL,
  subject_id text NOT NULL,
  fingerprint text NOT NULL,
  reviewed_by text,
  reason text,
  reviewed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (finding_type, subject_id, fingerprint)
);
```

Fresh `mix continuum.gen.migration` output includes all of these additions.

Activity operations add nullable `lineage_id` and `parent_task_id` columns to
`continuum_activity_tasks`, backfill existing lineage ids from each task id,
and create `continuum_activity_attempts` plus
`continuum_activity_operations`. Apply the generated migration before using
`Continuum.ActivityOperations` or `mix continuum.activities`; workers record
terminal attempt outcomes in the new attempts table.

Add the event overflow safety partition:

```sql
CREATE TABLE IF NOT EXISTS continuum_events_default
PARTITION OF continuum_events DEFAULT;
```

This turns a missed monthly horizon into a visible degraded condition instead
of rejecting new event inserts. After migration, `Continuum.Health.report/1`
reports its row count and `Continuum.Partitions.ensure/1` transactionally moves
overflow rows into newly-created monthly partitions.

## Operational rollout

After migrating and deploying, run:

```bash
mix continuum.health --repo MyApp.Repo
```

Use `--format json` for monitoring and `--strict` for a deployment health gate.
Repairs are dry runs until `--execute` is passed.

If the runtime database role is allowed to create partitions, enable scheduled
horizon maintenance under `Continuum.children/1`:

```elixir
partition_maintainer: [months: 6, interval_ms: :timer.hours(6)]
```

Otherwise run `Continuum.Partitions.ensure/1` from a release task with the
migration role. Runtime retention drops remain disabled and operator-controlled.

## Rollback

The additive tables and columns can remain in place if application code must
roll back to v0.6.1; do not run the down migrations during an ordinary rollback.
Do not schedule a manual activity retry until the v0.6.2 rollout is accepted.
Once an `activity_retry_scheduled` marker has been appended, the affected
history requires v0.6.2-aware replay code and must not be resumed by v0.6.1.
