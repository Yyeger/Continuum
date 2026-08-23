# Activities, Retries, And Idempotency

Activities are where side effects belong. Workflow code decides what should
happen; activity code talks to the outside world.

## Running activities in parallel

`activity_all/1` schedules several activities at once and waits for all of
them. Results come back in the order you declared them, whatever order they
finish in:

```elixir
def run(order) do
  [price, hold, tax] =
    activity_all([
      Pricing.quote(order),
      Inventory.reserve(order.sku),
      Tax.calculate(order)
    ])

  ...
end
```

Every member is scheduled in one transaction under one run lock, and the run
suspends once — not once per activity. Each member is an ordinary activity
task: it gets its own retries, timeout, queue, priority, and idempotency key
from its `use Continuum.Activity` options.

### Partial failure is a value, not a mode

Each element is exactly what the matching `activity/2` call would have
returned, so an activity that exhausts its retries contributes an
`{:error, reason}` entry and the others are unaffected. There is deliberately
no `mode: :all_or_nothing` option — you pattern-match the list:

```elixir
case activity_all([Pricing.quote(order), Inventory.reserve(order.sku)]) do
  [{:ok, price}, {:ok, hold}] ->
    ship(price, hold)

  results ->
    {:error, Enum.filter(results, &match?({:error, _}, &1))}
end
```

### What the macro requires

The argument must be a **literal list of activity calls** written at the call
site. Each element's `{module, function, arity}` is part of the batch's command
identity, computed at macro expansion, so it cannot come from a variable.
`activity_all(calls)` is a compile error, with a message saying why.

Batch members take no per-activity options, `compensate:` included. Use
sequential `activity/2` calls for a step that needs a compensation.

### How it stays replay-safe

Terminals land in whatever order the workers finish, so the journal cannot rely
on position. Each member's index is part of its command identity, and each
member's arguments are hashed into its schedule event; replay reassociates
results by command id and checks the hash, the way `child_started` does. A
batch whose terminals have not all landed suspends without advancing the
cursor, so it replays from its first member next time rather than from the
middle of itself.

## Progress and cooperative cancellation

Long-running activities can opt into a runtime context:

```elixir
defmodule MyApp.Export do
  use Continuum.Activity, context: true, timeout: {:hours, 1}

  def run(context, records) do
    records
    |> Enum.chunk_every(100)
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {batch, index}, :ok ->
      if Continuum.Activity.Context.cancelled?(context) do
        {:halt, {:error, :cancelled}}
      else
        :ok = upload(batch)
        :ok = Continuum.Activity.Context.heartbeat(context, %{completed_batches: index + 1})
        {:cont, :ok}
      end
    end)
  end
end
```

Heartbeat details must be durable terms and may encode to at most 16 KiB. Only
the latest details are retained. The update is fenced by task owner and attempt;
it returns `{:error, :cancelled}` or `{:error, :lease_lost}` when progress is no
longer authoritative. Health reports and Observer run details expose the latest
heartbeat through the configured payload redactor.

```elixir
defmodule MyApp.Activities.ChargeCard do
  use Continuum.Activity,
    retry: [max_attempts: 5, backoff: :exponential, base_ms: 500, jitter_ms: 250],
    queue: :payments,
    priority: 10,
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
    queue: :payments,
    priority: 50,
    idempotency_key: "charge:#{order_id}"
```

The Postgres runtime inserts a row in `continuum_activity_tasks`. The activity
dispatcher leases available tasks with `FOR UPDATE SKIP LOCKED`, starts a
worker, and the worker journals either `activity_completed` or
`activity_failed`.

The built-in executor runs at most 10 activities concurrently per Continuum
instance by default. Configure the limit globally or for a named instance:

```elixir
config :continuum, activity_max_concurrency: 25

Continuum.children(
  name: :billing,
  repo: MyApp.Repo,
  activity_max_concurrency: 8,
  activity_queues: [payments: 3, exports: 1]
)
```

An activity's module defaults can be overridden at each call site. Higher
integer priorities are leased first within available capacity. The built-in
executor enforces each configured queue limit as well as the instance-wide
limit, so slow export work cannot consume payment capacity. Queue and priority
are durable and appear in Observer output and activity telemetry. Oban-backed
instances continue to use their Oban queue limits.

The dispatcher claims only currently available capacity. Saturated polls use
jittered backpressure and emit queue-age, saturation, and rejected-claim
telemetry.

Retry policy is resolved in this order:

1. The `activity ... retry: ...` option at the call site.
2. The `use Continuum.Activity, retry: ...` module option.
3. A single attempt.

Activity policy is validated before durable work is scheduled. `max_attempts`
must be positive, `backoff` must be `:constant` or `:exponential`, and
`base_ms` and `jitter_ms` must be non-negative. `jitter_ms` adds a uniformly
random delay from zero through the configured value to each retry.
`max_backoff_ms` defaults to one minute and caps the combined backoff and
jitter: the jitter window is reserved below the cap, so a cohort of retries that
has reached maximum backoff stays spread out across
`max_backoff_ms - jitter_ms` through `max_backoff_ms` instead of collapsing onto
the cap. `max_retry_horizon_ms` defaults to 24 hours and must cover the
worst-case execution time plus every maximum delay. Per-attempt timeouts must
be positive and cannot exceed 24 hours.

`backoff: :exponential` uses `base_ms * 2 ^ (attempt - 1)`, capped by
`max_backoff_ms`. Use an explicit policy for longer horizons:

```elixir
retry: [
  max_attempts: 8,
  backoff: :exponential,
  base_ms: 500,
  jitter_ms: 250,
  max_backoff_ms: 60_000,
  max_retry_horizon_ms: 3_600_000
]
```

Idempotency keys are enforced by the Postgres runtime. Once an activity result
is committed for an activity module and key, another task with the same module
and key journals that committed result without running the activity body again.

An idempotency key must be a binary or `nil`, whether it comes from the call
site or `idempotency_key/1`. Invalid keys fail the run before a task is written.

This guarantee starts after Continuum commits success. Activities that perform
externally visible writes, such as payments, emails, or third-party API
mutations, should still pass their own idempotency key to the external system.
That closes the remaining window where a worker can crash after the external
write succeeds but before Continuum commits the result.

See `guides/idempotency.md` for the exact scope.
