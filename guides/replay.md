# Offline replay

`mix continuum.replay` answers "what does this run do next, against the code on
this node?" without running it. Nothing is written: the run keeps its lease, its
history, and its pending signals.

Reach for it when a run is wedged and you want to know where it stopped, when a
run failed with a `Continuum.ReplayDriftError` and you want the exact cursor, or
before a deploy when you want to know what a code change would do to runs
already in flight.

```bash
mix continuum.replay RUN_ID --repo MyApp.Repo
mix continuum.replay RUN_ID --repo MyApp.Repo --format json
mix continuum.replay RUN_ID --repo MyApp.Repo --no-snapshot
mix continuum.replay RUN_ID --repo MyApp.Repo --against MyApp.Orders.Checkout
mix continuum.replay RUN_ID --repo MyApp.Repo --strict
```

The outcome is one of four:

| Outcome | Meaning |
|---|---|
| `completed` | The history carries the workflow to a return value. The report says whether it agrees with the stored terminal result. |
| `suspended` | The workflow stops at a pending effect. A `history_exhausted` reason names the cursor and the effect it would have performed next. |
| `continued` | The workflow tail-calls `continue_as_new/1`. |
| `drift` | Code and history disagree, at a named cursor, with the journaled event on one side and the effect the code asked for on the other. |

A version this node cannot load is reported as "this node cannot replay this
version", not as drift — the mismatch is in the deploy, not in the run. Deploy
the release that ran it, or use `--against` to replay through a module you do
have.

`--against` is how you rehearse a code change against a run already in flight:
replay a suspended run through the module you are about to deploy and see
whether it drifts before the deploy makes that everyone's problem.

`--no-snapshot` ignores stored snapshots and replays from events alone, which is
how you confirm that a snapshot and the events it compacted still agree.

## Why replay takes a journal adapter, not a flag

Replay is read-only *structurally*: `Continuum.Replay` hands the context
`Continuum.Runtime.Journal.ReadOnly`, which raises on every callback, and
`Continuum.Runtime.Effect` refuses to compute a live tail against it. Both stock
adapters mutate on the path the code calls replay — the in-memory one executes a
real activity body inline once the workflow steps past the journaled tail, and
the Postgres one resolves a tail `signal_awaited` by consuming a pending signal
row inside a transaction. `Effect` tells them apart by journal module identity,
so a distinct module is what closes those routes; a flag on `Postgres` would not.

`Continuum.Replay.run/4` is the same kernel for a history you already hold, and
`Continuum.Test.replay/4` delegates to it.
