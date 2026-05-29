# Sagas and Compensation

A saga is a multi-step process where each step that changed external state has a
matching *compensation* that undoes it. When a later step fails, the saga rolls
back the earlier steps in reverse order.

Continuum gives you two primitives: `compensate/1` runs one activity's
compensation, and `compensate_all/0` runs every pending compensation in LIFO
order (most recent first).

## Attaching a compensation to an activity

Pass a `compensate:` MFA to an `activity` call. The MFA is run as an ordinary
activity (through the worker pool, with retries, timeouts, idempotency, and
lease fencing) when you compensate it.

```elixir
defmodule MyApp.OrderFlow do
  use Continuum.Workflow, version: 1

  def run(%{order_id: id, items: items}) do
    {:ok, validated} = activity Validation.check(items)

    {:ok, charge} =
      activity Payments.charge(id, validated.total),
        compensate: {Payments, :refund, [id]}

    case await signal(:fraud_review, timeout: hours(24)) do
      {:ok, :approved} ->
        activity Fulfillment.ship(id)

      {:ok, :rejected} ->
        compensate(charge)        # run just this one
        {:error, :rejected}
    end
  rescue
    e ->
      compensate_all()            # LIFO over everything still pending
      reraise e, __STACKTRACE__
  end
end
```

## The `ActivityRef` return shape

An activity **without** `compensate:` returns a bare term, exactly as before.

An activity **with** `compensate:` must return the conventional
`{:ok, value}` / `{:error, reason}` shape:

* `{:ok, value}` becomes `{:ok, %Continuum.ActivityRef{}}`. The ref carries the
  unwrapped `result`, the `raw_result` (`{:ok, value}`), the activity `mfa`, the
  `compensate` MFA, and a stable `activity_id`. Pass the ref to `compensate/1`.
* `{:error, reason}` is returned unchanged and does **not** push a compensation.
* Any other return raises `ArgumentError` (the saga path requires the
  `{:ok, _}`/`{:error, _}` contract).

Use `Continuum.unwrap/1` when you only want the raw activity return:

```elixir
{:ok, charge} = activity Payments.charge(id, total), compensate: {Payments, :refund, [id]}
charge.result            # the value from {:ok, value}
charge.raw_result        # {:ok, value}
Continuum.unwrap(charge) # {:ok, value}
```

## `compensate/1` vs `compensate_all/0`

* `compensate(ref)` runs the compensation for one specific activity and removes
  it from the pending set, so a later `compensate_all/0` will not run it twice.
* `compensate_all/0` walks the pending compensations in **LIFO** order. It is
  ideal in a `rescue` clause to unwind everything done so far.

Compensations run **sequentially**, newest first. (Parallel compensation is a
later milestone.)

## Idempotent compensations

A compensation can be retried or replayed after a crash, so make it idempotent.
Add `c:Continuum.Activity.idempotency_key/1` to the compensation's module:
Continuum reuses the committed result instead of re-running the side effect.

```elixir
defmodule Payments do
  use Continuum.Activity, retry: [max_attempts: 5, backoff: :exponential]

  def refund(order_id), do: {:ok, Stripe.refund(order_id)}
  def idempotency_key([order_id | _]), do: "refund:#{order_id}"
end
```

## When a compensation itself fails

If a compensation fails after exhausting its retries, Continuum journals
`compensation_failed` and the run **continues** — a failed rollback does not
crash the workflow. `compensate/1` returns `{:error, reason}` in that case so
your control flow can decide what to do; `compensate_all/0` moves on to the next
pending compensation.

## Determinism

Compensations are journaled (`compensation_scheduled` → `compensation_completed`
/ `compensation_failed`) and replay deterministically. On resume, a partially
completed `compensate_all/0` continues at the next uncompensated entry rather
than re-running completed ones. Tampering with a journaled compensation event
raises `Continuum.ReplayDriftError`.

## Telemetry

* `[:continuum, :compensation, :scheduled]`
* `[:continuum, :compensation, :started]`
* `[:continuum, :compensation, :completed]`
* `[:continuum, :compensation, :failed]`
