# Long-Running Workflows

`continue_as_new/1` lets a workflow complete the current run and immediately
start a fresh run of the same workflow with new input. Use it for subscriptions,
cron-style loops, and agents that should run for months without growing one
unbounded event history.

## Basic Pattern

```elixir
defmodule MyApp.SubscriptionFlow do
  use Continuum.Workflow, version: 1

  def run(%{customer_id: customer_id, cycles_done: cycles_done} = state) do
    {:ok, _charge} = activity Billing.charge(customer_id)
    timer(days(30))

    if cycles_done >= 11 do
      {:ok, :year_complete}
    else
      continue_as_new(%{state | cycles_done: cycles_done + 1})
    end
  end
end
```

The current run is marked `completed` with `result: {:continued, next_run_id}`.
The new run receives the new input and starts with a short fresh history.

## Chain Fields

Every continued chain shares a `correlation_id`. The first run uses its own id
as the chain correlation id, and every later run copies it.

Each successor records `continued_from_run_id`, pointing to the immediate prior
run. Operators can follow the chain in the Observer run header or by querying
`continuum_runs`.

`continue_as_new/1` is not a child workflow. It does not create a parent/child
wait relationship. If a child workflow continues as new, it remains linked to
the same parent and `await_child/1` follows the continuation chain to the final
terminal result.

## Retention

`continue_as_new/1` bounds the history of each physical run; it does not delete
older runs in the chain. Use the existing workflow retention settings and your
operator cleanup policy to bound storage.

## Crash Safety

The continuation is written transactionally: Continuum appends
`run_continued_as_new`, completes the current run, and inserts the successor run
as one journal operation. A crash before that transaction leaves the original
run resumable; a crash after it leaves exactly one successor.

## Telemetry

Continuum emits `[:continuum, :run, :continued_as_new]` with `from_run_id`,
`to_run_id`, and `correlation_id`.
