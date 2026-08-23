# Testing workflows

Continuum ships two ways to test a workflow, and they answer different
questions.

**In-memory unit tests** answer "does this workflow branch correctly?" They run
the whole workflow — activities, timers, signals, child workflows — inline in
one process, with no database and no worker pool, and they finish in
milliseconds. This is where most of your workflow tests belong.

**Durable tests** answer "does this workflow survive?" They run against
Postgres with the real leasing, fencing, retry, and recovery machinery, and
they can kill an engine mid-flight and watch another one finish the job. You
want a few of these, not hundreds.

`Continuum.Test` covers both. Nothing in this guide reaches for a
`Continuum.Runtime.*` module; if a test of yours has to, that is a gap worth
reporting.

## In-memory unit tests

```elixir
defmodule MyApp.CheckoutTest do
  use ExUnit.Case, async: false

  alias Continuum.Test

  setup do
    Test.reset_in_memory!()
    :ok
  end

  test "an approved order ships" do
    {:ok, run_id} =
      Test.start_synchronous(MyApp.Checkout, %{order_id: "o-1"},
        activities: %{
          {MyApp.Payments, :charge} => fn _order -> {:ok, "ch_test"} end,
          {MyApp.Shipping, :book} => {:ok, "trk_test"}
        }
      )

    :ok = Test.inject_signal(run_id, :fraud_review, :approved)

    assert {:ok, %{state: :completed, result: {:ok, _}}} = Continuum.await(run_id, 1_000)
  end
end
```

The in-memory journal is process-global rather than test-isolated, so call
`Continuum.Test.reset_in_memory!/0` between tests that need a clean slate, and
keep these tests `async: false`.

### Stubbing activities

`:activities` maps `{Module, :function}` — or `{Module, :function, arity}` when
one module exports the same name at several arities, in which case the more
specific key wins — to a stub. A function of the activity's arity is called
with the activity's arguments; any other value is returned as-is:

```elixir
activities: %{
  {MyApp.Payments, :charge} => fn order -> {:ok, "ch_" <> order.id} end,
  {MyApp.Shipping, :book} => {:error, :out_of_stock},
  {MyApp.Pricing, :quote, 2} => {:ok, 1_500}
}
```

Three properties are worth knowing:

- **A stub cannot change command identity.** Command ids are computed at macro
  expansion from the call site, before the effect is dispatched, so a stubbed
  run journals byte-identical command ids to a real one. That is what makes a
  history captured from a stubbed test meaningful.
- **Stub returns are validated.** In-memory writes skip
  `Continuum.DurableTerm` validation, so a stub returning a PID would pass the
  unit test and be rejected in production. Continuum validates them anyway.
- **Stubs are refused on the Postgres journal.** A durable activity runs in a
  worker process out of a claimed task row, which a stub cannot reach; silently
  ignoring one would make a "passing" durable test meaningless.

A stub that *raises* is a legitimate way to drive a failure branch — it
normalizes to `{:error, _}` exactly as a real activity would. A stub that is
itself wrong (bad arity, unjournalable return) raises
`Continuum.ActivityStubError` instead, so a broken test does not masquerade as
a failing activity.

### Child workflows

Child workflows run in memory too, as their own inline engines, and they
inherit the parent's stubs:

```elixir
test "a batch fans out one child per order" do
  {:ok, batch_id} = Test.start_synchronous(MyApp.Batch, %{orders: orders}, activities: stubs())

  assert {:ok, %{state: :completed, result: {:ok, results}}} =
           Continuum.await(batch_id, 2_000)

  assert length(results) == length(orders)
end
```

Signal each child by reading its run id out of the parent's `child_started`
events (`Continuum.Test.history/2`); child run ids are deterministic, not
random.

### Signals and timers

```elixir
Test.inject_signal(run_id, :approved, %{by: "ops"})
Test.fire_timer(run_id)
```

`inject_signal/4` goes through the same delivery path as `Continuum.signal/4`,
so an injected signal is journaled with the consuming await's command identity
and exercises the same drift detection as a production delivery.

## Durable tests

Durable tests need Postgres and an Ecto SQL Sandbox checkout. Test suites
normally start Continuum with its pollers disabled so nothing moves behind a
test's back — `Continuum.Test.drive/2` turns the crank instead:

```elixir
{:ok, run_id} = Continuum.Test.start_postgres(MyApp.Checkout, order)
assert {:ok, %{state: :completed}} = Continuum.Test.drive(run_id)
```

Each tick rescues expired leases, dispatches runnable runs, runs due activity
tasks, and fires due timers, until the run reaches a terminal state or the
timeout passes. `drive_until_state/3` stops earlier, at a state you name.

### Crash and resume

The point of a durable execution engine is that losing the node running a
workflow does not lose the workflow. Testing that takes four public calls:

```elixir
test "a run survives losing its engine mid-flight" do
  {:ok, run_id} = Continuum.Test.start_postgres(MyApp.Checkout, order)

  :ok = Continuum.Test.drive_until_state(run_id, [:suspended])
  :ok = Continuum.Test.crash!(run_id)
  :ok = Continuum.Test.expire_lease!(run_id)
  :ok = Continuum.Test.elapse_timers!(run_id)

  assert {:ok, %{state: :completed}} = Continuum.Test.drive(run_id)
end
```

- `crash!/2` kills the engine the way a node loss would, and returns once it
  has left the registry. Drive the run to a resting state first: killing an
  engine mid-statement is fine in production but takes the SQL Sandbox shared
  connection with it, failing the rest of the test for a reason unrelated to
  the workflow.
- `expire_lease!/2` is needed because recovery deliberately refuses to touch a
  run whose lease is still live. Without it you would be waiting out a real
  TTL, and rescuing earlier would be lease theft.
- `elapse_timers!/2` moves a pending `timer(days(30))` into the past, which is
  the thing you least want to wait for in a test.

## Golden histories

Capture a real history once, commit it, and assert that today's code still
replays it:

```elixir
history = Continuum.Test.history(run_id, journal: :postgres)
Continuum.Test.dump_history!(run_id, "test/fixtures/checkout.journal", journal: :postgres)

# later
history = Continuum.Test.load_history!("test/fixtures/checkout.journal")
assert {:ok, expected} = Continuum.Test.replay(MyApp.Checkout, order, history)
```

`Continuum.Test.replay/4` delegates to `Continuum.Replay.run/4`, which is
read-only: a workflow that steps past the end of the history reports
`{:suspended, {:history_exhausted, _}}` rather than executing the next activity
for real. See [Offline replay](replay.md).

One caveat: a history captured in memory names the workflow module inside its
command ids, while a durable one names the generated `V_<hash>` entrypoint. A
golden history is therefore specific to the mode that produced it. Capture
durable fixtures from durable runs.

## Paranoid mode

`CONTINUUM_PARANOID=1 mix test` re-replays every completed run from its
journaled history and asserts the result is identical, with snapshots disabled.
It is off by default because it is slow; run it in CI, and after any change to
`Continuum.Runtime.Effect`, the journal adapters, or the snapshot format.
