# Changelog

## Unreleased

### v0.2 in progress
- Added monthly partitioning support for `continuum_events`, including
  operator Mix tasks for creating, listing, and dropping expired partitions.
- Added enforced activity idempotency through `continuum_activity_results`.
  Committed results are reused per activity module and idempotency key, across
  runs, without re-running the activity body.
- Added nullable `continuum_runs.trace_context` persistence so future OTel
  run-attempt spans can link resumes back to the original trace.
- Added compile-time warnings for workflow calls into helper modules that are
  not stdlib-trusted, marked with `use Continuum.Pure`, or allowlisted through
  `config :continuum, trusted_modules: [...]`.
  Upgraders with existing helper-module calls should add `use Continuum.Pure`
  to audited pure helpers or list externally audited modules in
  `:trusted_modules`.
- Added a Postgres signal-await fast-path: when a signal is already pending in
  the durable mailbox, `await signal(...)` journals `signal_received` directly
  and skips `signal_awaited` plus timeout timer creation.

## v0.1 — "It survives a crash"

The full v0.1 surface from `ROADMAP.md` is implemented, exercised by 97 tests + 2
StreamData properties, and stable across multiple random seeds. ~26 core
modules + 5 schemas + 3 mix tasks.

### Workflow definition & determinism
- `use Continuum.Workflow` — `@on_definition` runs `Continuum.AstCheck` on every
  clause; `@before_compile` computes the AST version hash and registers
  `__continuum_workflow__/0`.
- `use Continuum.Activity` — retry/timeout policy DSL with
  `idempotency_key/1` plumbed through the task struct.
- `use Continuum.Pure` — opt helper modules into the AST-scanned trusted set.
- `Continuum.AstCheck` — compile-time determinism scanner with curated
  denylist (including `Continuum.start/3`, `signal/3`, `cancel/2`, `await/3`,
  which are side effects when called from inside a workflow) and
  remediation hints.
- Workflow DSL: `activity`, `await signal(...)` with optional `timeout: ms`,
  `timer`, `seconds/minutes/hours/days`. Each macro computes a structured
  `command_id = {kind, module, function, line, hash, ordinal}` at expansion
  time.
- Deterministic primitives `Continuum.now/0`, `today/0`, `uuid4/0`,
  `random/0` are macros (not functions) so they capture `__CALLER__` and
  produce stable cursor identity. `Continuum.side_effect/1` is the runtime
  escape hatch.
- `Continuum.ReplayDriftError` raised on type mismatch *or* command-identity
  mismatch — drift is detected even when shapes happen to match.

### Runtime
- `Continuum.Runtime.Engine` — GenServer-per-run with `restart: :temporary`.
  Crashed engines are not restarted by OTP; resume is the dispatcher's job.
- `Continuum.Runtime.Effect.run/2` — canonical replay-or-suspend bridge,
  shared by both journal adapters.
- `Continuum.Runtime.Context` — process-dict cursor + `command_counts` for
  ordinal disambiguation.
- `Continuum.Runtime.Dispatcher` — `FOR UPDATE SKIP LOCKED` poller for
  runnable runs; captures fresh fencing token at claim time.
- `Continuum.Runtime.Recovery` — boot-time orphan rescue, filters on
  `lease_expires_at < now()` so live remote leases are never stolen.

### Journal & leasing
- `Continuum.Runtime.Journal` behaviour with `InMemory` and `Postgres`
  adapters. Both share the engine's replay loop.
- Postgres adapter stores opaque payloads as `bytea`
  (`:erlang.term_to_binary/1`) and gates every write through
  `lock_and_validate_run!` (run lease) and `lock_and_validate_activity_task!`
  (task lease). `lock_and_validate_active_run!` rejects writes against
  cancelled/completed/failed runs as defense-in-depth.
- `Continuum.Runtime.Lease` + `Lease.Heartbeater` — fencing token via
  `nextval('continuum_lease_token_seq')`, owner string `node()/monotonic_int`
  (greppable). Heartbeater monitors engine pids and unsubscribes on DOWN.
- Postgres schemas `Continuum.Schema.{Run, Event, Signal, Timer,
  ActivityTask}` with `bytea` payload columns.

### Activities
- `Continuum.Runtime.ActivityWorker.{Supervisor, Dispatcher, Worker}` —
  claim joins on `continuum_runs` and snapshots `r.lease_token` so the
  worker carries its own authority through to atomic completion.
- `Journal.Postgres.complete_activity_task!/3` does event-append +
  task-update under run-lease + task-lease CAS in one transaction.
- `Journal.Postgres.retry_activity_task!/4` for the retry path with
  exponential backoff.

### Signals & timers
- `Continuum.Runtime.SignalRouter` — Postgres LISTEN consumer; single-strategy
  delivery (Postgres vs in-memory chosen at startup based on
  `Journal.default()`). For in-memory mode, appends `signal_received`
  directly and wakes the engine.
- `Continuum.Runtime.TimerWheel` — Postgres-truth poller. Claim joins runs
  and captures `lease_token`. Handles the signal-await-with-timeout race
  via `timer_winner/2` (`already_resolved` / `already_fired` branches).

### Cancellation
- `Continuum.cancel/2` → `Journal.Postgres.cancel_run!/2` discards pending
  activity tasks, marks pending timers `fired = true`, and fails the run,
  all in one lease-CAS-guarded transaction.

### Public API, telemetry, test helpers
- `Continuum.PubSub` wired up: terminal transitions
  (`completed`/`failed`/`cancelled`) broadcast `{:run_finished, run_id,
  state, payload}`; `Continuum.await/3` subscribes-then-receives with a 5ms
  poll fallback.
- `Continuum.Telemetry` — 24+ named events under the `[:continuum, …]`
  prefix, fired on every state transition.
- `Continuum.Test` — `start_synchronous/3` (in-memory inline-activity mode),
  Postgres helpers, `replay/4` for golden histories, `inject_signal/4`,
  `fire_timer/2`, sandbox checkout, `reset_in_memory!/0`.
- `mix continuum.gen.{migration, workflow, activity}`.

### Documentation & examples
- ExDoc reference, three guides, one example app (`continuum_example_orders`).
- `docker-compose.yml` for local Postgres; `mix test` aliases auto-create +
  auto-migrate the test repo.

### Verification
- Crash-resume integration test (`activity → timer → activity`,
  `Process.exit(engine, :kill)` mid-flight, asserts new pid + full event
  sequence + final result).
- Lease-fencing race test (three variants: `append!`, `cancel_run!`,
  `complete_activity_task!` all reject stale-token writes).
- StreamData property-based replay test on pure-side_effect and mixed
  activity+side_effect histories.
- Postgres-backed replay + drift test, bytea encoding round-trip test,
  cancellation/recovery/timer/signal/dispatcher/lease unit tests.

### Known limitations carried to v0.2
- `continuum_events` is unpartitioned (retention story in v0.2).
- `TimerWheel` is a poller, not the ETS-cached due-queue (perf upgrade).
- AST scan over unmarked helper modules produces no warning until
  `use Continuum.Pure` is added (polish in v0.2).
- `signal_awaited` is journaled even when a signal is already in the
  durable mailbox (cosmetic; two events instead of one).
- `Continuum.Activity`'s `idempotency_key/1` is plumbed but not enforced
  by a side-table (real exactly-once-ish semantics in v0.2).
- `config :continuum, :repo` is a global app-env value (per-process repo
  threading in v0.2).
- `Continuum.VersionRegistry` and `Continuum.patched?/1` are stubs;
  content-addressed module dispatch and journaled patch decisions land in
  v0.3.
- `compensate` macro is not in v0.1; users do `try/rescue` + cleanup
  activity until v0.3.
- `mix continuum.audit` is not implemented; v0.5.

## v0.1-dev-skeleton

- ROADMAP.md (full architecture, phased v0.1→v1.0 plan, market context)
  - CLAUDE.md (orientation for future sessions)
  - Continuum.AstCheck — compile-time determinism scanner with curated
    denylist and remediation hints
  - use Continuum.Workflow / Activity / Pure macros (AST scan via
    @on_definition, AST-hash versioning via @before_compile)
  - Workflow DSL: activity, await signal(...), timer, compensate,
    seconds/minutes/hours/days
  - Runtime: Engine (GenServer-per-run), Effect.run/2 with throw-based
    suspend/replay, Context, Journal behaviour + InMemory adapter
  - Deterministic primitives: now/0, uuid4/0, random/0, today/0,
    side_effect/1
  - Continuum.ReplayDriftError with structured diff
  - Postgres schemas + mix continuum.gen.migration

  22 tests passing across 8 random seeds. Zero compiler warnings.
