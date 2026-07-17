# Migrating from v0.6.1 to v0.6.2

v0.6.2 adds the first-class operational health and repair surface.

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

After deploying and migrating, run:

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
