# Migrating from v0.5.1 to v0.6

v0.6 is a hardening release: a full-library logic audit fixed ~25 findings
across activity liveness, replay-path agreement, the determinism scanner,
identity across `continue_as_new` chains and cluster nodes, and
signal/cancel/await consistency. Most fixes are invisible to application code.
This guide covers the migration and the observable behavior changes.

## Database Migration

v0.6 adds one column:

```elixir
alter table(:continuum_runs) do
  add :cancel_requested_at, :utc_datetime_usec
end
```

`mix continuum.gen.migration` includes it for new installs. No backfill is
needed; the column records pending cancel requests for runs whose owning
engine was unreachable, and the owner honors it on its next lease heartbeat.

## Cancellation Has a Real `cancelled` State

Cancelled runs previously ended as `failed` with the error term `:cancelled`.
In v0.6 the run row's state is `cancelled`, and `cancel_run!` is the single
broadcaster of one canonical `{:run_finished, run_id, :cancelled, :cancelled}`
message — including for cascade-cancelled descendant runs, whose awaiters
previously blocked for their full timeout.

What to update:

  * Code matching `{:error, %{state: :failed, error: :cancelled}}` from
    `Continuum.await/3` now receives
    `{:error, %{state: :cancelled, error: :cancelled}}`.
  * Code inspecting run rows directly (`state == "failed"` plus decoding the
    error) should match `state == "cancelled"`.
  * Rows written by earlier versions are still recognized: they display,
    await, and query as cancelled (`Continuum.query(state: :cancelled)`
    matches both encodings).
  * A child run that legitimately *failed* with the user error term
    `:cancelled` is now classified as a failure by its parent's
    `await_child/1`, not as a cancellation.

## `Continuum.signal/3,4` Validates Its Target

Signaling a run that does not exist returns `{:error, :not_found}`, and
signaling a terminal run returns `{:error, :run_terminal}`. Previously both
returned `:ok` while the signal sat in a mailbox nothing could ever consume.
If you signal speculatively (for example, fire-and-forget notifications to
runs that may have finished), handle or ignore the new error tuples.

## Journal Errors Are Structured

Journal write rejections raise `Continuum.Runtime.JournalError` (with `op`
and a structured `reason`) instead of `RuntimeError` with a formatted
message. Code rescuing `RuntimeError` around journal operations — or matching
on message substrings such as `"lease_mismatch"` — must rescue
`Continuum.Runtime.JournalError` and match on `error.reason` instead.

Relatedly, a *transient* database failure while journaling a completion or
suspension no longer marks the run `failed` with the DB exception as its
error: the engine crashes and crash-and-resume replays and finishes the run.

## Cancel Results Are More Specific

`Continuum.cancel/2` on a run it cannot cancel locally now distinguishes:

  * `{:error, :not_found}` — no such run;
  * `{:error, :owned_elsewhere}` — a live engine on another node owns it.
    The cancel was forwarded if the node was reachable; otherwise the request
    was recorded durably and the owner honors it on its next heartbeat — the
    error tells you cancellation is *pending*, not failed;
  * `{:error, {:run_not_active, state}}` — the run is already terminal
    (previously reported as `:not_found`).

## `continue_as_new` Chains Are Transparent

Operations addressed to a chain-root run id now act on the live incarnation:
signals are delivered to the tip's mailbox, cancel cancels the tip, and
`Continuum.await/3` follows the chain to the final terminal result (the
internal `{:continued, run_id}` marker is never returned). When a run
continues, its undelivered signals, live unawaited children, `namespace`, and
`attributes` move to the successor — previously children were orphaned from
the cancel cascade and tenant scoping silently reset to defaults.

Successors are also stamped with the workflow's *currently loaded* version
instead of the predecessor's pin, so long-running chains pick up deploys.

## `stuck_unknown_version` Is No Longer Produced

A node that claims a run whose `(workflow, version_hash)` it does not have
loaded now releases the lease and leaves the run `suspended` for a capable
node, emitting `[:continuum, :run, :unknown_version]` per attempt. Runs
marked `stuck_unknown_version` by earlier versions are flipped back to
`suspended` at boot when a matching version registers. If you alerted on the
stuck state, alert on the telemetry event (or `mix continuum.audit`) instead.

## Activity Execution Liveness

No action required, but worth knowing operationally:

  * Task leases are heartbeated while the activity executes (TTL 30 seconds,
    renewed every 10; tune with `:task_lease_ttl_seconds` and
    `:task_lease_renew_ms`). Activities longer than 30 seconds no longer
    depend on a one-shot lease extension, and a crashed worker's task is
    rescuable within roughly one TTL.
  * **Crash requeues consume an attempt.** An activity with the default
    `max_attempts: 1` whose worker or node dies mid-execution now fails with
    `:attempts_exhausted` instead of silently re-running its side effects on
    every recovery. Raise `max_attempts` (and supply an `idempotency_key/1`)
    for crash-resilient activities.
  * `mix continuum.audit` reports `expired_leased_activity_tasks`; a
    persistently non-zero count means workers are dying between claim and
    completion faster than the sweep rescues them.

## `side_effect/1` Identity in Helper Modules

Producer fingerprints no longer include per-compilation anonymous-function
artifacts, so recompiling a helper module (adding an unrelated function) no
longer drifts every in-flight run replaying through a `side_effect` site in
it. One-time caveat: histories journaled through the *bare-producer*
`Effect.run/2` form (not the `Continuum.side_effect/1` macro, which is what
workflow code uses) replay-break once across this upgrade.

Note the documented caveat on `Continuum.side_effect/1`: command identity
includes the call site's line, and helper modules have no version-hash
protection — prefer keeping `side_effect` calls in the workflow module.

## Determinism Scanner Coverage

Recompiling against v0.6 may surface new compile errors or warnings in
workflow code that previously slipped through — each is a real determinism
hazard:

  * piped banned calls (`x |> send(:msg)`) are checked at their effective
    arity and rejected;
  * chained dynamic receivers (`input.mod.fun(x)`) and captures of dynamic
    modules (`&m.f/1`) warn as unanalyzable;
  * `catch` arms in `Continuum.Pure` helpers warn (same suspend-swallow
    foot-gun as in workflow clauses);
  * the `compensate_all` coverage check sees the whole module, so
    uncompensated activities in other clauses or private helpers now warn —
    and call sites with non-literal opts no longer warn falsely.

## Internal Runtime API Changes

Only relevant if you call `Continuum.Runtime.*` directly (not a supported
surface): `Journal.Postgres.retry_activity_task!/5` takes `backoff_ms`
instead of a timestamp, `Journal.Postgres.deliver_signal!/4` returns
`{:ok, delivered_run_id}` (it may have chain-hopped) or an error tuple, and
`Lease.renew/4` can return `{:ok, :cancel_requested}`, which callers must not
treat as an error.
