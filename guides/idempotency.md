# Idempotency

Continuum activity idempotency is scoped by activity module and key.

If any run completes `MyActivity` with idempotency key `"k1"`, a later task for
`MyActivity` and `"k1"` receives the same committed result. The later task does
not run the activity body. This is intentionally cross-run: the key represents
the external operation, not the workflow run.

```elixir
defmodule MyApp.Activities.ChargeCard do
  use Continuum.Activity

  @impl true
  def run(%{order_id: order_id, amount: amount}) do
    MyApp.Payments.charge(order_id, amount, idempotency_key: "charge:#{order_id}")
  end

  @impl true
  def idempotency_key([%{order_id: order_id}]) do
    "charge:#{order_id}"
  end
end
```

Returning `nil` opts out for that activity call.

Continuum records committed activity results in `continuum_activity_results`.
Replay still reads from `continuum_events`; the side table only suppresses
future duplicate execution.

There is one remaining crash window: if the activity performs the external side
effect and the worker dies before Continuum commits the result, a retry can run
the activity body again. Pass the same idempotency key to the external system
for operations such as payments, emails, and third-party mutations.
