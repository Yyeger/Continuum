# Determinism Rules And Replay Drift

Continuum's core guarantee is replay: after a crash, the workflow function runs
again from the top and receives the same values for every effect that already
happened. That only works when workflow code is deterministic.

## What Workflow Code May Do

Workflow code may:

* call activities with `activity`
* wait for external input with `await signal(:name)`
* sleep with `timer/1`
* call `Continuum.now/0`, `today/0`, `uuid4/0`, `random/0`, or
  `side_effect/1`
* call pure helper modules that are safe to replay

Each of those operations goes through `Continuum.Runtime.Effect`, which checks
the journal before doing anything live.

## What Workflow Code Must Not Do

Workflow code must not directly call non-deterministic APIs such as:

* `DateTime.utc_now/0` or `Date.utc_today/0`
* `:rand.*`
* `System.*`
* `IO.*`
* `Process.send/2`
* ETS or `:persistent_term` reads
* dynamic code loading or `apply/3`

`Continuum.AstCheck` scans workflow modules at compile time and rejects known
unsafe calls with a remediation hint.

## Replay Drift

Replay drift means the workflow asks for a different effect than the next event
in the committed history. For example, an old history may contain an activity
at cursor 0, while the new code calls a timer at cursor 0. Continuum raises
`Continuum.ReplayDriftError` instead of guessing.

Do not reorder, remove, or change the meaning of effects in a workflow version
that still has active runs. Prefer one of these patterns:

* add new effects after existing effects
* put externally visible work in activities
* bump the workflow version for incompatible state changes
* use `Continuum.Test.assert_replays/3` with committed golden histories before
  shipping workflow edits

## Golden-History Test

Record a known-good history once:

```elixir
{:ok, run_id} = Continuum.Test.start_synchronous(MyApp.OrderFlow, input)
{:ok, _} = Continuum.await(run_id, 5_000)
Continuum.Test.dump_history!(run_id, "test/golden/order_v1.journal")
```

Assert future code still replays it:

```elixir
history = Continuum.Test.load_history!("test/golden/order_v1.journal")
Continuum.Test.assert_replays(MyApp.OrderFlow, input, history)
```

If this test fails, treat it as a compatibility break for in-flight workflows.
