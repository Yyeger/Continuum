# Idempotent ingress

External callers and brokers commonly retry requests. Continuum can bind those
retries to one durable workflow start and one signal mailbox entry.

## Unique starts

Use a stable key from the upstream request:

```elixir
{:ok, run_id, status} =
  Continuum.start_unique(OrderWorkflow, order,
    namespace: "orders",
    idempotency_key: "checkout:#{order.id}"
  )
```

`status` is `:started` for the caller that inserted the run and `:existing` for
all retries. Keys are scoped by namespace and workflow and remain attached to
the root run for its full lifetime. `Continuum.start/3` also accepts
`:idempotency_key`, returning `{:error, {:already_started, run_id}}` on conflict.

## Unique signals

Pass the message or request ID supplied by the transport:

```elixir
{:ok, status} =
  Continuum.signal_unique(run_id, :payment_received, payment,
    "payments:#{payment.event_id}"
  )
```

`status` is `:delivered` or `:duplicate`. Delivery IDs are scoped to the
logical workflow chain and signal name, so the same signal remains idempotent
when the workflow has continued as new.
