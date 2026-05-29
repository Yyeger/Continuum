# Child Workflows

Child workflows let you compose a process out of smaller runs instead of one
giant `def run/1`. They unlock fan-out/fan-in: a parent starts N children and
awaits them all.

Child workflows require the Postgres journal — children are independent durable
runs claimed by the dispatcher.

## The three forms

```elixir
defmodule MyApp.BatchFlow do
  use Continuum.Workflow, version: 1

  def run(%{batch_id: bid, order_ids: ids}) do
    # Sequential: start a child, suspend, return its result.
    {:ok, _audit} = await child MyApp.AuditFlow.run(%{batch_id: bid})

    # Fan-out: start N children, then await them all.
    results =
      ids
      |> Enum.map(fn id -> start_child MyApp.OrderFlow, %{order_id: id}, id: "order-#{id}" end)
      |> Enum.map(&await_child/1)

    {:ok, results}
  end
end
```

* `await child Mod.run(input)` — start a child synchronously and block on it.
* `start_child Mod, input, opts` — start a child asynchronously; returns a
  `%Continuum.ChildRef{}`. `opts` accepts `id:` for a parent-scoped key.
* `await_child(ref)` — suspend until that child terminates.

`await_child/1` returns the child's result on success, `{:error, error}` if the
child failed, and `{:error, :child_cancelled}` if it was cancelled.

## Deterministic child ids

A child's `run_id` is derived deterministically from the parent run id, the
`start_child` call site, and any `id:` option. A parent at the same cursor never
starts two children on replay, and a re-run picks up the same child. Use a
meaningful `id:` (for example the order id) when you want a stable, greppable
child run id.

## How a parent wakes up

Children carry their own lease and run independently. When a child reaches a
terminal state, the same transaction sets the parent's `next_wakeup_at` and
emits `pg_notify('continuum_run_wake', parent)`. The existing `SignalRouter`
listens on that channel and wakes the parent's local engine; if no engine is
local, the dispatcher poll picks the parent up. Only the parent engine — while
holding the parent lease — writes the `child_completed` / `child_failed` /
`child_cancelled` event into the parent's history.

## Cancellation cascade

Cancelling a parent cancels every in-flight descendant. The cascade is bounded
by `config :continuum, max_child_depth: 10` and clears each descendant's lease,
so a still-running child engine fails its next journal write and stops cleanly —
no events can be appended to a cancelled child after the cascade. Cancelled
children carry the error `:parent_cancelled`.

## Crash safety

If a parent crashes while a child is running, the child is unaffected (its own
lease). On resume the parent replays its already-journaled `child_*` events, or,
if it had not yet recorded the child's outcome, re-checks the child's terminal
state and journals it under the parent lease.

## Composing with `continue_as_new`

If an awaited child uses `continue_as_new`, the parent follows the continuation
chain forward to its terminal run and returns that final result — never an
intermediate `{:continued, _}` marker. See
[`long-running-workflows.md`](long-running-workflows.md).

## Telemetry

* `[:continuum, :child, :started]` — `parent_run_id`, `child_run_id`, `workflow`
* `[:continuum, :child, :completed]`
* `[:continuum, :child, :failed]`
