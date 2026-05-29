# Changelog

## Unreleased

### New surfaces

- `Continuum.Test.Paranoid` — the `--paranoid` re-replay safety net. Enable it
  for a whole run with `CONTINUUM_PARANOID=1 mix test` (or
  `config :continuum, :paranoid_replay, true`); the default is off so ordinary
  `mix test` stays fast. When enabled, a telemetry handler re-replays every
  completed in-memory run from its journaled history and flags any drift or
  differing result. `verify_run!/4` is the strict, raising contract for
  asserting a specific run re-replays identically; `assert_histories_match!/2`
  compares two histories on `(event_type, decoded_payload, command_id)`,
  excluding DB-stamped fields.
- `Continuum.VersionRegistry` now resolves durable `(workflow, version_hash)`
  pairs to loaded workflow entrypoints. The hot-path registry is backed by
  `:persistent_term`; a short-lived boot task upserts loaded workflow versions
  into Postgres for each Continuum instance.
- `use Continuum.Workflow, workflow: LogicalWorkflow` registers a concrete
  module as a hash-specific entrypoint for a logical workflow. This is the
  v0.3 compromise entrypoint strategy: keep old version modules loaded and
  point new versions at the same logical workflow.

### Migrations

- Added `continuum_workflow_versions`, keyed by `(workflow, version_hash)`,
  with the loaded `entrypoint` module and `registered_at` timestamp.

### Behavior changes operators should know about

- Resuming Postgres-backed runs now dispatches through the run row's journaled
  `workflow` and `version_hash`; it no longer trusts the latest logical module.
- Runs whose journaled workflow version cannot be resolved are marked
  `:stuck_unknown_version` instead of being replayed through possibly changed
  code.
- Starting a durable run now fails loudly if the workflow module does not
  expose `__continuum_workflow__/0`.

### Telemetry additions

- `[:continuum, :run, :unknown_version]`

## v0.2.0 — 2026-05-15 — "I can see what's happening"

v0.2 makes the v0.1 engine operable: a free Phoenix LiveView Observer, an
optional OpenTelemetry bridge, opt-in history snapshots, named multi-instance
runtimes, and six pieces of v0.1 debt paid down (event partitioning, ETS timer
cache, idempotency enforcement, helper-module determinism warnings, the
`signal_awaited` fast-path, per-process repo threading).

See [`MIGRATING_v0_1_to_v0_2.md`](./MIGRATING_v0_1_to_v0_2.md) for the upgrade
path.

### New surfaces

- `Continuum.Observer` — optional Phoenix LiveView observer: runs index, run
  detail with decoded event timeline, operator actions for cancelling and
  sending signals. Mounted via `Continuum.Observer.Router.continuum_observer/2`
  with an optional `:layout` forwarded to `live_session/3`. Continuum core
  compiles without Phoenix LiveView installed; host applications add the
  Phoenix dependencies when they mount the Observer. Self-contained demo
  ships at `dev/observer_demo.exs`. See `guides/observer.md`.
- `Continuum.OpenTelemetry.setup/1` — opt-in bridge that turns
  `[:continuum, :run, ...]` and `[:continuum, :activity, ...]` telemetry into
  short `continuum.run_attempt` and `continuum.activity_attempt` spans. Resume
  spans link back to the original trace via the new
  `continuum_runs.trace_context` column. Continuum still compiles without any
  OpenTelemetry packages. See `guides/observability.md`.
- `Continuum.children/1` — host-supervisor helper for named instances. Each
  instance owns its own registry, run supervisor, dispatchers, timer wheel,
  signal router, lease heartbeater, snapshotter, and recovery process bound
  to a single Ecto repo. Public calls accept `instance: name`. The default
  `Continuum` instance is unchanged. See `guides/multi-instance.md`.
- Experimental, opt-in history snapshots: `continuum_snapshots`,
  `Continuum.Snapshot`, `Continuum.Runtime.Snapshotter`, compacted-prefix
  replay validation, snapshot telemetry, snapshot benchmark harness
  (`bench/snapshot_bench.exs`). Replay-loop cost on a 10k-event side-effect
  workflow drops ~8× when snapshots are enabled. The v0.2 plan's ≥10×
  acceptance target is *not* met; the gap is accepted under E1's
  minimum-acceptance clause because snapshots ship experimental and opt-in
  (default `snapshot_threshold: :infinity`). Closing the remaining 25% is
  tracked for v0.3 once runtime use is dogfooded. See `guides/snapshots.md`.

### v0.1 debt paid down

- Monthly partitioning for `continuum_events` (`PARTITION BY RANGE
  (inserted_at)`), with operator Mix tasks: `mix continuum.partitions.create`,
  `mix continuum.partitions.list`, `mix continuum.partitions.drop_old`
  (`--execute` opt-in). No runtime partition manager in v0.2.
- Activity idempotency is enforced through `continuum_activity_results` keyed
  on `(activity_module, idempotency_key)`. Committed results are reused
  across runs without re-running the activity body. New telemetry
  `[:continuum, :activity, :idempotency_hit]`. See `guides/idempotency.md`.
- ETS-cached `Continuum.Runtime.TimerWheel`: near-term timer cache hydrated
  from Postgres, 30s refresh safety net, and `continuum_timer_armed`
  `pg_notify` reschedules. Replaces the v0.1 1s polling loop. TimerWheel owns
  its own Postgrex notification listener per instance. Benchmark harness
  `bench/timer_wheel_bench.exs` reports a 20.0x DB-query reduction for 1000
  idle long-due timers over a 60s window (60 pre-cache poller queries vs. 3
  cached-wheel timer SELECTs).
- Compile-time warnings for workflow calls into helper modules that are not
  stdlib-trusted, not marked `use Continuum.Pure`, and not allowlisted via
  `config :continuum, trusted_modules: [...]`. Severity is configurable with
  `config :continuum, untrusted_call_severity: :warn | :error` (default
  `:warn`). See the *Helper Modules* section of `guides/determinism-rules.md`.
- Postgres signal-await fast-path: when a signal is already in the durable
  mailbox, `await signal(...)` journals `signal_received` directly and skips
  the `signal_awaited` event plus the timeout-timer write. Old histories
  that did journal `signal_awaited` replay unchanged.
- Per-process repo / multi-instance threading. `Continuum.children/1`
  registers a named instance; `instance:` selects it on `start/3`,
  `signal/4`, `cancel/2`, `await/3`. Lease owner format is now
  `node()/instance/monotonic_int`. `Continuum.InstanceNotRegisteredError`
  surfaces unknown names. Postgres `start_run` accepts `trace_context:` so
  resumed runs can link OTel spans back to the original trace.

### Determinism hardening

- Snapshot compaction fails closed when a source event lacks a `command_id`:
  `Snapshot.compact/4` returns `{:error, {:missing_command_id, seq}}` instead
  of producing a nil-matching step that any effect would replay through.
- In-memory journal now assigns sequence numbers when callers omit `:seq`,
  matching the Postgres `next_seq/1` semantics. Fixes a latent gap where
  `inject_signal/4` and `fire_timer/2` could write `seq: nil` events that
  snapshot compaction would later misorder or drop.

### Behavior changes operators should know about

- `continuum_events` primary key is now `(run_id, seq, inserted_at)` because
  Postgres partitioned tables require the partition key in the PK. Continuum
  still guarantees per-run `(run_id, seq)` uniqueness through the run-row
  write lock; SQL-level uniqueness was relaxed only to satisfy the partition
  shape. Migration notes are in `MIGRATING_v0_1_to_v0_2.md`.
- `signal_awaited` is no longer journaled when a matching signal is already
  pending. Dashboards counting `signal_awaited` rows as a proxy for "signal
  arrivals" should count `signal_received` instead.
- Helper-module calls inside `use Continuum.Workflow` modules now produce a
  compile-time warning unless the module is `use Continuum.Pure`,
  stdlib-trusted, or listed in `config :continuum, trusted_modules: [...]`.

### Migrations

Four delta migrations on top of v0.1, runnable in order on a fresh database
or the current local v0.1 dev/test schema:

1. `20260601000000_partition_continuum_events`
2. `20260601000001_create_continuum_activity_results`
3. `20260601000002_create_continuum_snapshots`
4. `20260601000003_add_trace_context_to_runs`

Fresh installs (`mix continuum.gen.migration`) get the v0.2 shape directly
and do not need the delta migrations. v0.1 had no public release, so there
is no production-data compatibility promise.

### Telemetry additions

- `[:continuum, :activity, :idempotency_hit]`
- `[:continuum, :snapshot, :taken]`
- `[:continuum, :snapshot, :skipped]`

All Continuum telemetry events now include `instance: name` metadata so
dashboards can split correctly when more than one instance is active.

### Documentation

- New: `guides/multi-instance.md`, `guides/snapshots.md`,
  `guides/observer.md`, `guides/observability.md`, `guides/idempotency.md`.
- Updated: `guides/determinism-rules.md` now covers the helper-module warning,
  `use Continuum.Pure`, and `trusted_modules`.
- New: `MIGRATING_v0_1_to_v0_2.md` at the repo root.

### Note on the module-count moat

The ROADMAP's "~25 core modules" target was a v0.1 working principle. v0.2
deliberately revises it: with Observer, OpenTelemetry, snapshots, multi-instance
plumbing, and the Mix-task surface, raw module count is no longer the right
shape of the moat. The replacement target is keeping the **runtime** surface
small and justified — new runtime processes need a written reason — while
allowing optional UI modules (Observer LiveViews, components), Mix tasks, and
schema files to land where they make sense. At tag-prep time the v0.2 tree has
49 `.ex` files under `lib/`, with 19 under `lib/continuum/runtime/`. The v0.2
tree adds the Snapshotter as a runtime child; everything else under
`lib/continuum/observer/` and `lib/mix/tasks/` is optional surface.

### Known limitations carried forward to v0.3+

- Snapshot runtime use is experimental in v0.2. Default
  `snapshot_threshold: :infinity` (off). Public snapshot payload format
  (`:erlang.term_to_binary` of the struct) is not promised stable.
- `Continuum.VersionRegistry` and `Continuum.patched?/1` remain stubs;
  content-addressed module dispatch and journaled patch decisions land in
  v0.3.
- `compensate` macro and parent/child workflows are still v0.3.
- `continue_as_new` is v0.3.
- `mix continuum.audit` is v0.5.
- Cluster distribution and the `:peer`-based multi-node test harness are v0.5.
- No Oban adapter yet — v0.5.
- Observer has no replay-stepping debugger in v0.2 (run detail shows the
  durable timeline only). Replay debugger is v0.3+.

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
