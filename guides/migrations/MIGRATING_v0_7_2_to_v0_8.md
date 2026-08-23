# Migrating from v0.7.2 to v0.8

v0.8 is a feature release. Its theme is exposure: the batch scheduler, the
lease-free replay engine, and in-memory child workflows all existed already, and
only one caller each could reach them. This release turns them into surface you
can type.

There is one data migration, one deprecation, and one behaviour change in
`Continuum.Test.replay/4` that is a correctness fix but will change what some
existing tests return.

## Upgrade order

1. Apply the schema change below. It adds one nullable column and requires no
   backfill, so v0.7.2 keeps running against it unchanged.
2. Deploy v0.8 and restart the Continuum runtime normally.
3. At your convenience, rename `Continuum.query/1,2` call sites (below). The old
   names still work and emit a deprecation warning.

No configuration change is required. Workflow, event, and snapshot formats are
unchanged, and every v0.7.2 history replays as-is.

## Schema changes

```bash
mix continuum.gen.migration --from 0.7.2 --repo MyApp.Repo
```

It adds one nullable column:

```sql
ALTER TABLE continuum_timers ADD COLUMN owner_seq bigint;
```

`owner_seq` is the `seq` of the event that armed the timer — the `timer_started`,
or the `signal_awaited` for a signal timeout. Without it, firing a timer had to
load and decode the run's entire event history to find the owning event, inside
the `FOR UPDATE` transaction. That is quadratic on exactly the workload timers
exist for: a `timer(days(30))` in a loop.

Timers armed before the migration keep `owner_seq = NULL` and fire through the
old full-history lookup, so no backfill is needed and no in-flight timer is
disturbed. Rows written after it take two indexed reads.

The migration is required before deploying v0.8. The runtime writes
`owner_seq` when it arms a timer and reads it when a timer fires, so deploying
the new code against the v0.7.2 schema makes those operations fail with an
undefined-column error. The fallback applies to timer *rows created before the
migration*, not to a database where the column itself is absent.

## New: `activity_all/1`

Schedule several activities at once and wait for all of them. Results come back
in the order you declared them, whatever order they finish in:

```elixir
[price, hold, tax] =
  activity_all([
    Pricing.quote(order),
    Inventory.reserve(order.sku),
    Tax.calculate(order)
  ])
```

Every member is scheduled in one transaction under one run lock, and the run
suspends once — not once per activity. Each member is an ordinary activity task
with its own retries, timeout, queue, priority, and idempotency key.

Each element is exactly what the matching `activity/2` call would have returned,
so an activity that exhausts its retries contributes an `{:error, reason}` entry
and the others are unaffected. There is deliberately no `mode:` option: a
partial failure is a value you pattern-match.

Two constraints worth knowing before you reach for it:

- The argument must be a **literal list of activity calls** written at the call
  site. Each element's `{module, function, arity}` is part of the batch's
  command identity, computed at macro expansion, so it cannot come from a
  variable. `activity_all(calls)` is a compile error that says so.
- Batch members take no per-activity options, `compensate:` included. Use
  sequential `activity/2` calls for a step that needs a compensation.

Batches journal a new `activity_batch_scheduled` event type, one per member,
followed by one ordinary `activity_completed`/`activity_failed` terminal per
member in whatever order they land. `Continuum.Snapshot` compacts the whole
batch into a single step. Existing histories are untouched — a plain `activity`
still journals `activity_scheduled`.

## New: `mix continuum.replay`

Replay a durable run's history against the code on this node, and get back the
terminal result, the suspend reason, or the exact cursor where code and history
disagree:

```bash
mix continuum.replay RUN_ID --repo MyApp.Repo
mix continuum.replay RUN_ID --repo MyApp.Repo --against MyApp.Orders.Checkout
mix continuum.replay RUN_ID --repo MyApp.Repo --no-snapshot --format json
```

Nothing is written: the run keeps its lease, its history, and its pending
signals. See [Offline replay](../replay.md).

## New: the workflow test kit

`Continuum.Test` gained an activity-stubbing option, in-memory child workflows,
and a durable driver. See [Testing workflows](../testing.md).

```elixir
# Unit test: no database, no worker pool.
{:ok, run_id} =
  Continuum.Test.start_synchronous(Checkout, order,
    activities: %{{Payments, :charge} => fn _order -> {:ok, "ch_test"} end}
  )

# Durable test: crash the engine mid-flight and watch another finish the job.
:ok = Continuum.Test.drive_until_state(run_id, [:suspended])
:ok = Continuum.Test.crash!(run_id)
:ok = Continuum.Test.expire_lease!(run_id)
assert {:ok, %{state: :completed}} = Continuum.Test.drive(run_id)
```

Child workflows now run on the in-memory journal as well as on Postgres, behind
two new optional `Continuum.Runtime.Journal` callbacks (`start_child!/4` and
`await_child_terminal!/6`). A custom journal adapter that does not implement
them raises a message naming what is missing, instead of the previous blanket
"child workflows require the Postgres journal".

## New: `Continuum.log/3`

```elixir
Continuum.log(:info, "payment accepted", order_id: order.id, cents: total)
```

Metadata is journaled with the event, merged into the `Logger` metadata, and
included in `[:continuum, :workflow, :log]` telemetry under `:metadata`. Values
go through `Continuum.DurableTerm`, so a PID or reference is rejected at the
call rather than written and rendered meaningless on replay.

A `log/2` call is unchanged on disk: the metadata key is omitted entirely when
the list is empty, so existing histories, snapshots, and golden fixtures still
match. Metadata is part of what replay matches, exactly as the message already
was.

## Deprecated: `Continuum.query/1,2`

Renamed to `Continuum.list_runs/1,2`. The old names delegate and emit a
deprecation warning; they will be removed in v0.9.

```elixir
# Before
{:ok, page} = Continuum.query(where: [{:eq, :state, :suspended}])

# After
{:ok, page} = Continuum.list_runs(where: [{:eq, :state, :suspended}])
```

This is freeze tax rather than a better name. `query/3` is reserved for a
per-run named read alongside `signal/3`; with both arities occupied by a
paginated row search, a post-1.0 `query(run_id, :stage)` could only be
distinguished from `query(instance, opts)` by a first-argument guard,
permanently.

## Behaviour changes worth knowing

- **`Continuum.Test.replay/4` is now read-only.** It delegates to the new
  `Continuum.Replay.run/4`, which carries a journal that refuses every write and
  refuses to compute a live tail. A workflow that steps *past* the end of the
  history now returns `{:suspended, {:history_exhausted, %{cursor: n, effect: e}}}`
  instead of executing the next activity for real and journaling the result.
  That was always the documented contract; the in-memory adapter simply did not
  honour it. Pass `journal:` explicitly to opt back into the old behaviour.

  If a test of yours relied on replay running past the tail, it was executing a
  real activity body against a committed history — the same code pointed at a
  production run would have charged a real card.

- **Journaled terms decode with `:safe`.** Every read path now goes through
  `Continuum.DurableTerm.decode!/1`, which never creates atoms and raises
  `Continuum.DurableTermError` rather than leaking `ArgumentError`. On a cold
  node it loads modules declared by deployed OTP applications and retries, so
  atoms from compiled workflow and activity code remain transparent without
  trusting database text. A dynamically constructed atom (`String.to_atom/1`
  on external input) remains invalid because no receiving node can know it from
  deployed code.

- **A batch activity's terminal event appends at the tail.** `activity_all/1`
  members cannot claim `seq + 1`, which belongs to the next member's schedule
  event, so their terminals take the next free seq the way parallel
  compensations already do. Plain `activity/2` is unchanged.

- **The snapshotter's per-run event counter is bounded** at two generations of
  10,000 entries. It previously kept an entry forever for every path other than
  "a snapshot was taken", including runs that complete below their threshold —
  which is most runs. The counter is an optimisation, so an evicted run simply
  starts counting again.

- **The dispatcher and recovery stop shipping an unbounded exclusion array.**
  Locally-registered run ids are excluded from a claim only while there are at
  most 256 of them; above that the array is dropped. The exclusion was always an
  optimisation — `Engine.adopt_lease/4` hands a rotated token to a live engine,
  so a run claimed despite having a local engine keeps running.

- **The version registry stops rewriting unchanged `persistent_term`s.** A fresh
  durable start performed at least three `:persistent_term.put/2` calls, each
  scheduling a literal-area collection that scans every process which may
  reference the old term. The values are content-addressed and never change, so
  they are now compared before writing.

## Known limitation: golden histories are mode-specific

A history captured from an in-memory run names the workflow module inside its
command ids, while a durable one names the generated `V_<hash>` entrypoint.
Each mode is self-consistent, so nothing breaks, but a golden fixture captured
in memory will not replay against a durable run without normalizing the module.
Capture durable fixtures from durable runs.

This is not new in v0.8; the release's adapter-parity test simply made it
visible, and pins the current behaviour so a fix cannot land silently.
