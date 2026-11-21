# Workflow schedules

Create a workflow run at a durable UTC timestamp:

```elixir
{:ok, schedule_id} =
  Continuum.schedule_at(MyApp.InvoiceReminder, %{invoice_id: invoice.id}, run_at,
    namespace: "billing",
    attributes: %{invoice_id: invoice.id}
  )
```

The schedule stores its workflow version, input, namespace, attributes, and a
preallocated run ID. The schedule runner claims due rows in bounded pages.
Retries after a node or process crash converge on that same run ID, so one
scheduled occurrence cannot create multiple workflow runs.

Inspect or cancel a schedule before dispatch starts:

```elixir
{:ok, schedule} = Continuum.Schedules.get(schedule_id)
:ok = Continuum.Schedules.cancel(schedule_id)
```

## Commit a reminder with the invoice

When creating an invoice also creates a payment reminder, put both operations
in the same transaction. The schedule runner can see the reminder after commit.
A later failure rolls back the invoice and reminder together.

```elixir
multi =
  Ecto.Multi.new()
  |> Ecto.Multi.insert(:invoice, changeset)
  |> Continuum.Multi.schedule_at(:reminder, MyApp.InvoiceReminder,
    fn %{invoice: invoice} -> %{invoice_id: invoice.id} end,
    fn %{invoice: invoice} -> invoice.remind_at end,
    namespace: "billing"
  )

{:ok, %{invoice: invoice, reminder: schedule_id}} = Repo.transaction(multi)
```

Use the repo belonging to the chosen durable instance. On SQLite call
`Repo.transaction(multi, mode: :immediate)`. The due time is a UTC
`DateTime`. Options may also be computed from prior changes. A caller-supplied
`:id` gives the schedule a stable identifier. A duplicate ID fails the Multi,
so handle transaction retries according to the application's uniqueness rules.

## Scope operations to a namespace

Pass the namespace your application has authorized when inspecting or changing
a schedule by ID. For example, a billing operator can cancel a pending schedule
with `Continuum.Schedules.cancel(schedule_id, namespace: "billing")`. The
database checks the namespace and pending state in the same update.

The same option applies to `get/2`, `get_recurring/2`, `pause_recurring/2`,
`resume_recurring/2`, `list_recurring/1`, and `list_occurrences/2`. A missing
or mismatched record returns `{:error, :not_found}` on single-record operations.
Lists return an empty page when nothing matches. Omit `:namespace` for global
access, or pass a nonempty string to enable the guard. An explicitly invalid
value, including `nil`, returns `{:error, {:invalid_namespace, value}}`.

## When a start keeps failing

A schedule whose run cannot be started — most often because no node has its
workflow version loaded — is returned to `scheduled` with the failure recorded in
`last_error`, and its next attempt is delayed by an exponential, jittered backoff
that grows from five seconds to five minutes. After twelve attempts the schedule
moves to the terminal `failed` state, stops consuming a claim slot, logs, and
emits `[:continuum, :schedule, :failed]`.

Failed schedules are actionable findings in `Continuum.Health`:

```elixir
{:ok, report} = Continuum.Health.report()
report.schedules.failed_count
report.schedules.failed
```

They make the overall report `:degraded` until acknowledged, the same as an
activity dead letter. Deploy the missing version and create a new schedule; a
terminal schedule is never retried automatically.

## Recurring UTC intervals

```elixir
{:ok, definition_id} = Continuum.schedule_every(MyApp.Reconcile, %{}, 60_000,
  starts_at: ~U[2026-10-01 12:00:00Z],
  overlap: :skip,
  missed: :catch_up,
  max_catch_up: 10,
  namespace: "billing")

{:ok, definition} = Continuum.Schedules.get_recurring(definition_id)
{:ok, page} = Continuum.Schedules.list_recurring(namespace: "billing", limit: 50)
{:ok, occurrences} = Continuum.Schedules.list_occurrences(definition_id, limit: 50)
:ok = Continuum.Schedules.pause_recurring(definition_id)
:ok = Continuum.Schedules.resume_recurring(definition_id)
```

Intervals measure elapsed milliseconds (minimum one second). The optional first
occurrence defaults to one interval from now. Only UTC DateTimes are accepted;
calendar/cron expressions, local timezones, and DST are not supported.

Both policies are required, so behavior after downtime is a deliberate choice:

| Policy | Behavior |
|---|---|
| `overlap: :allow` | Occurrences may execute simultaneously. |
| `overlap: :skip` | Record a `skipped` occurrence if earlier work is queued, starting, or has an active run in its continuation chain. |
| `missed: :catch_up` | Generate overdue occurrences oldest first, subject to the catch-up budget. |
| `missed: :skip` | Coalesce overdue times into the most recent due occurrence, then advance beyond now. Older omitted times do not create rows. |

`max_catch_up` is 1–100 (default 10) per definition per poll. The runner batch
size also caps total generated occurrences across definitions. Skipped overlaps
consume budget. Pausing stops generation and retains the cursor; already queued
occurrences keep running. Resume applies the recorded missed policy. Cancel an
individual unstarted occurrence through `Schedules.cancel/2` when needed.

Definitions pin workflow version, input, namespace, attributes, and trace
context. Each occurrence has a unique `(definition_id, occurrence_at)`, its own
stable schedule ID, and a preallocated run ID. The definition cursor and new
occurrences commit in one transaction under a definition lock. A crash before
commit rolls both back; after commit the ordinary runner retries the same
occurrence rather than generating another run. `occurrence_at` remains the
logical UTC time even when `scheduled_at` changes for retry backoff.

Unknown versions use the bounded one-shot retry policy described above. An
occurrence that exhausts retries becomes `failed`; future occurrences retain
their own identity. Pause a definition while repairing a repeatedly missing
version. Deployment preflight and version cleanup include paused definitions
and pending occurrences, so code cannot be classified as unused merely because
no occurrence is running now.

Listing APIs return `Continuum.Page` and accept `cursor: page.next_cursor` for
continuation. Limits are 1–100 and pages are ordered by stable row ID. Definition
and occurrence inspection retains database state/policy strings; the existing
`Schedules.get/2` returns its documented atom state for individual occurrences.

Existing 0.8.1 installations must apply the schema upgrade before starting the
updated runtime: `mix continuum.gen.migration --from 0.8.1 --repo MyApp.Repo`,
then `mix ecto.migrate`. Fresh installs receive this schema in the full generator.
