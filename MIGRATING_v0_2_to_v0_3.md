# Migrating from v0.2 to v0.3

Continuum v0.3 is a pre-1.0 feature release. It adds sagas, child workflows,
`continue_as_new`, journaled patch markers, and content-addressed workflow
dispatch. The migration is one schema delta plus a few operator-visible behavior
changes.

## Schema

Run the v0.3 migration after your v0.2 migrations:

```bash
mix ecto.migrate
```

The migration adds nullable lineage columns to `continuum_runs`:

* `parent_run_id`
* `parent_command_id`
* `correlation_id`
* `continued_from_run_id`

It also adds `continuum_workflow_versions`, keyed by
`(workflow, version_hash)`, with the loaded entrypoint module and registration
time. The table is populated by each Continuum instance on boot.

Existing runs are backfilled with `correlation_id = id`; new runs use their own
id as the correlation id until a continuation chain propagates it.

## Behavior Changes

Postgres-backed resumes now resolve the run's journaled workflow hash through
`Continuum.VersionRegistry`. A run whose version is not loaded is marked
`:stuck_unknown_version` and emits `[:continuum, :run, :unknown_version]`.
Deploy old workflow entrypoints until their active runs drain.

`Continuum.patched?/1` is no longer a stub. New runs that hit a patch line
journal `true`; old histories that predate the line return `false` without
consuming an event.

`continue_as_new/1` completes the current run with
`result: {:continued, next_run_id}` and inserts a successor run. Dashboards that
interpret completed-run results should treat that tuple as a continuation
marker, not a business result.

## Adopting `compensate:`

Activities without `compensate:` keep the v0.2 return shape.

A compensated activity must return `{:ok, value}` or `{:error, reason}`. On
success, Continuum returns `{:ok, %Continuum.ActivityRef{}}` so the workflow can
later call `compensate(ref)`.

Before:

```elixir
{:ok, charge} =
  activity Payments.charge(order_id, total),
    idempotency_key: "capture:#{order_id}"

{:ok, %{charge: charge}}
```

After:

```elixir
{:ok, charge} =
  activity Payments.charge(order_id, total),
    idempotency_key: "capture:#{order_id}",
    compensate: {Payments, :refund, [order_id]}

case await signal(:fraud_review) do
  :approved ->
    {:ok, %{charge: charge.result}}

  :rejected ->
    compensate(charge)
    {:error, :fraud_rejected}
end
```

Use `Continuum.unwrap/1` when you need the activity's raw return:

```elixir
Continuum.unwrap(charge) #=> {:ok, value}
```

Make compensation activities idempotent. If they expose
`idempotency_key/1`, Continuum reuses committed results on retry or
crash-resume.

## New Guides

Read these before adopting the new surface:

* [`guides/sagas.md`](./guides/sagas.md)
* [`guides/child-workflows.md`](./guides/child-workflows.md)
* [`guides/long-running-workflows.md`](./guides/long-running-workflows.md)
* [`guides/patching.md`](./guides/patching.md)
* [`guides/workflow-versioning.md`](./guides/workflow-versioning.md)
