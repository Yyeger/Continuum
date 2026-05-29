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
* call `Continuum.patched?/1` for journaled patch decisions
* use `compensate/1`, `compensate_all/0`, `start_child/3`, `await_child/1`,
  `await child ...`, and `continue_as_new/1`
* call pure helper modules that are safe to replay

Each of those operations goes through `Continuum.Runtime.Effect`, which checks
the journal before doing anything live.

`Continuum.patched?/1` is the one special replay branch: when replaying a
history recorded before a patch line existed, it may return `false` without
advancing the replay cursor. That branch is keyed on command identity, so two
patch markers at different source locations do not consume each other's events.

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

## Helper Modules

Workflow code often calls into helper modules for pure transformations. The
scanner cannot follow into arbitrary helpers, so v0.2 emits a compile-time
warning for every external module a workflow calls that is not known to be
deterministic. A helper module is *trusted* when one of the following holds:

* it is in the stdlib allowlist (`Enum`, `Map`, `String`, `Integer`,
  `Decimal`-shaped collections — see `Continuum.AstCheck.trusted_stdlib/0`)
* it exports `__continuum_pure__/0` because it does `use Continuum.Pure`
* it is listed in `config :continuum, trusted_modules: [MyApp.PriceMath, ...]`

```elixir
# A helper module that should be safe to call from a workflow.
defmodule MyApp.PriceMath do
  use Continuum.Pure

  def total(items), do: Enum.reduce(items, 0, &(&1.price + &2))
end
```

`use Continuum.Pure` runs the same AST scan over every function in the helper
module at its compile time. Non-deterministic calls become a compile error in
the *helper*, not in the workflow.

Unmarked helpers produce a warning by default. To turn the warning into a
compile error:

```elixir
config :continuum, untrusted_call_severity: :error
```

Use `:trusted_modules` for third-party or externally audited modules that you
cannot annotate with `use Continuum.Pure`:

```elixir
config :continuum, trusted_modules: [Decimal, Money]
```

The scanner cannot transitively follow into helper code — that would require a
full compile-time call graph. The trust marker is the boundary; audit the
inside of a trusted helper module the same way you audit workflow code.

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
* use `Continuum.patched?/1` for compatible in-place branches that must keep old
  in-flight histories on the old path
* use `Continuum.Test.assert_replays/3` with committed golden histories before
  shipping workflow edits

## Side Effects

`Continuum.side_effect/1` is the escape hatch for deterministic values that are
not worth a full activity, such as choosing a UUID-like correlation key or
capturing a small derived value. The function runs only at the live tail; its
return value is journaled and later replayed.

Do not use `side_effect/1` for external work. HTTP calls, database writes,
payments, emails, and anything that needs retry/idempotency belong in
activities. `side_effect/1` composes with v0.3 effects (`patched?/1`,
compensations, child waits, snapshots, and `continue_as_new`) because it still
uses the same command identity and replay cursor as every other effect.

## Continuing As New

`continue_as_new/1` journals `run_continued_as_new` and then terminates the
current engine with a dedicated sentinel. Code after it in the same branch is
not executed. Treat it as a tail call.

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
