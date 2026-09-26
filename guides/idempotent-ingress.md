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

## Atomic application transactions

Use `Continuum.Multi.enqueue/5` when the business record and workflow must
commit together. The selected Continuum instance must use the transaction's
repo and the PostgreSQL journal.

```elixir
Ecto.Multi.new()
|> Ecto.Multi.insert(:order, Order.changeset(%Order{}, params))
|> Continuum.Multi.enqueue(:workflow, OrderWorkflow,
  fn %{order: order} -> %{order_id: order.id} end,
  fn %{order: order} ->
    [namespace: "orders", idempotency_key: "order:#{order.id}"]
  end)
|> Repo.transaction()
```

The `:workflow` change is `%{run_id: id, status: :enqueued | :existing}`. Input
and options can be computed from earlier Multi changes. Namespace, attributes,
trace context, and the selected workflow version are preserved. A duplicate
returns the original root ID without changing the original input; other Multi
steps still execute and need their own business uniqueness constraints.

No engine starts inside the transaction. A committed run is unleased until a
dispatcher claims it, so keep a dispatcher enabled for the repo. PostgreSQL
visibility prevents other connections from claiming an uncommitted run.
Rollback removes both the new run and its ingress reservation. Once committed,
dispatch can recover the run even if the process that submitted the Multi dies.
Do not call the dispatcher yourself inside the transaction.

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
