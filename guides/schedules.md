# One-shot schedules

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

`schedule_at/4` is the one-shot foundation. Recurring definitions, overlap and
missed-fire policies, and timezone-aware calendar schedules are intentionally a
later API slice so those contracts can be introduced without changing one-shot
semantics.
