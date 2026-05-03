# Activities, Retries, And Idempotency

Activities are where side effects belong. Workflow code decides what should
happen; activity code talks to the outside world.

```elixir
defmodule MyApp.Activities.ChargeCard do
  use Continuum.Activity,
    retry: [max_attempts: 5, backoff: :exponential, base_ms: 500],
    timeout: {:seconds, 30}

  @impl true
  def run(%{order_id: order_id, amount: amount}) do
    MyApp.Payments.charge(order_id, amount)
  end

  @impl true
  def idempotency_key([%{order_id: order_id}]) do
    "charge:#{order_id}"
  end
end
```

Call an activity from a workflow with the `activity` macro:

```elixir
{:ok, charge} =
  activity MyApp.Activities.ChargeCard.run(%{order_id: order_id, amount: total}),
    retry: [max_attempts: 5, backoff: :exponential, base_ms: 500],
    idempotency_key: "charge:#{order_id}"
```

The Postgres runtime inserts a row in `continuum_activity_tasks`. The activity
dispatcher leases available tasks with `FOR UPDATE SKIP LOCKED`, starts a
worker, and the worker journals either `activity_completed` or
`activity_failed`.

Retry policy is resolved in this order:

1. The `activity ... retry: ...` option at the call site.
2. The `use Continuum.Activity, retry: ...` module option.
3. A single attempt.

`backoff: :exponential` uses `base_ms * 2 ^ (attempt - 1)`. Any other backoff
value uses constant delay.

Idempotency keys are carried in the durable task payload in v0.1, but they are
not enforced yet. Activities that perform externally visible writes, such as
payments, emails, or third-party API mutations, should still pass their own
idempotency key to the external system. The first release preserves the
Continuum-side plumbing; v0.2 can add a result side-table without changing
workflow code.
