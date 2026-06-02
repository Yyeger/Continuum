# Changelog

## Unreleased

## v0.5.0 — 2026-06-02 — "Production at scale"

### New surfaces

- Cluster-aware wake routing. Continuum starts `:pg` scope `:continuum`, engines
  join by `{instance, run_id}`, and wakes forward to remote owners when the run
  is not local. The lease and fencing token remain the write authority.
- Added `mix test.cluster` and a `:peer`-based cluster harness covering dispatch
  races, lease stealing, and activity worker node death against one Postgres.
- Added namespaces on `continuum_runs`. `Continuum.start/3` accepts
  `namespace:`, query/list paths default to `"default"`, and single-run
  operations stay globally keyed by `run_id`.
- Added search attributes on `continuum_runs`, plus `Continuum.query/1,2`,
  `Continuum.get_run/2`, and `Continuum.set_attributes/3`.
- Added `mix continuum.audit --repo MyApp.Repo [--format json] [--strict]` for
  loaded workflow versions, stale patch marker verdicts, and stuck
  unknown-version runs.
- The determinism scanner now rejects `:pg.*`, `:rpc.*`, and `:erpc.*` in
  workflow code.

### Migrations

- Added `continuum_runs.namespace text NOT NULL DEFAULT 'default'`.
- Added `continuum_runs.attributes jsonb NOT NULL DEFAULT '{}'`.
- Added GIN and namespace/state indexes for attribute and tenant-scoped
  queries.

### Documentation

- Added guides for clustering, namespaces, search/query, and auditing.
- Added `MIGRATING_v0_4_to_v0_5.md`.
- Updated the example orders app with the v0.5 migration and smoke coverage for
  two namespaces plus `Continuum.query/1`.

### v0.5 decisions

- `Continuum.Oban` activity routing is deferred to v0.5.1. The v0.5
  milestone ships the built-in activity runner unchanged so the cluster,
  namespace, query, and audit surfaces can tag without introducing a second
  execution adapter.
- `Continuum.AshAi` is deferred until a lighthouse adopter is engaged.
- The Observer replay-stepping debugger is formally cut from v0.5 rather than
  carried as another release's implicit nice-to-land.

### Benchmarks

- Pre-v0.5 baseline on 2026-06-02 from `MIX_ENV=test mix run
  bench/replay_hot_path_bench.exs`: raw replay 111 ms / 8.88 us per event over
  12,500 events.
- Final v0.5 verification on 2026-06-02: raw replay 86 ms / 6.88 us per event
  over 12,500 events; snapshot replay 78 ms over the compacted prefix.

## v0.4.0 — 2026-05-31 — "Hardening & ergonomics"

### Changed

- Replay contexts now keep an indexed in-process history (`:array`) for cursor
  reads and live-tail appends. This removes the replay hot path's repeated
  `Enum.at/2` scans and `history ++ [event]` list rebuilds while leaving the
  journal append path unchanged.
- Snapshot payloads now use a versioned `{:continuum_snapshot, 1, snapshot}`
  envelope. Legacy unversioned v0.2/v0.3 snapshot blobs still decode as format
  v1, and unsupported future formats raise a clear `ArgumentError` instead of
  failing as a raw term decode.
- `use Continuum.Workflow` accepts `snapshot_threshold: positive_integer |
  :infinity`. The snapshotter resolves per-workflow threshold first, then
  runtime/app config, then `:infinity`.
- Added `mix continuum.gc_versions --repo MyApp.Repo`, a dry-run-by-default
  cleanup task for `continuum_workflow_versions`. It deletes only with
  `--execute`, preserves loaded workflow hashes, and treats running,
  suspended, and stuck-unknown-version runs as pins.
- Added `mix continuum.archive_continued_chains --repo MyApp.Repo --older-than
  Nd`, a dry-run-by-default deletion task for expired non-tail
  `continue_as_new` cycles and their dependent rows.
- `compensate_all(mode: :parallel)` schedules all pending compensation tasks
  before suspending, then resumes once every scheduled compensation has a
  terminal journal event. `compensate: :none` explicitly opts an activity out of
  the new missing-compensation compile warning.
- `use Continuum.Workflow` now generates a hidden `V_<hash>` entrypoint module
  for the compiled workflow body. Public workflow modules stay as the start
  target, while durable Postgres runs execute and resume through the generated
  hash-specific entrypoint.
- Added v0.4 migration and operations documentation, plus an example
  `SubscriptionFlow` that combines `continue_as_new` with a per-workflow
  snapshot threshold.

### Migrations

- Added `continuum_snapshots.format_version smallint NOT NULL DEFAULT 1`, plus
  `continuum_runs_correlation_completed_idx` for the v0.4 continued-chain
  archival task.

### Benchmarks

- `mix run bench/snapshot_bench.exs 10000` on 2026-05-31 reported raw replay
  21 ms, snapshot replay 16 ms, and a 1.3x speedup for 10,000 side-effect
  events after indexed history landed. The old 7.2x snapshot advantage was
  largely measuring inefficient raw replay; v0.4 formally accepts the lower
  speedup because raw replay is now much faster and snapshot payload format
  stability is the user-facing graduation.
- Added `bench/replay_hot_path_bench.exs`. At 10,000 logical mixed operations
  (12,500 events across side effects, activities, patch markers, and saga
  compensations), current raw replay is 89 ms / 7.17 us per event; snapshot
  replay is 89 ms over the compacted prefix.

## v0.3.0 — 2026-05-29 — "Real workflows"

### New surfaces

- **`continue_as_new/1`.** A tail-call continuation for long-running /
  cron-style workflows: completes the current run as
  `result: {:continued, next_run_id}` and starts a fresh run on the same
  workflow with new input, keeping per-run history bounded. The whole chain
  shares a `correlation_id` (the chain root's id) and each run records its
  `continued_from_run_id` predecessor; a continued *child* keeps its
  `parent_run_id`, and a parent's `await_child` follows the chain forward to the
  terminal run's real result (never an intermediate `{:continued, _}`). Throws a
  distinct `:continuum_continued_as_new` sentinel so the engine stops cleanly
  instead of re-entering the workflow. New event `run_continued_as_new`,
  telemetry `[:continuum, :run, :continued_as_new]`. Requires the Postgres
  journal.
- **Parent/child workflows.** Compose workflows out of child runs:
  - `await child Mod.run(input)` — start a child synchronously, suspend, and
    return its result.
  - `start_child Mod, input, opts` — start a child asynchronously, returning a
    `%Continuum.ChildRef{}` (`opts` accepts `id:` for a parent-scoped key).
  - `await_child(ref)` — suspend until that child terminates.

  Child run ids are derived deterministically from the parent run id, the start
  command id, and any `id:` option, so a parent never starts two children on
  replay. Children carry their own lease and run independently; when a child
  reaches a terminal state it sets the parent's `next_wakeup_at` and emits
  `pg_notify('continuum_run_wake', parent)` in the same transaction. The
  existing `SignalRouter` now also `LISTEN`s `continuum_run_wake` and wakes a
  local parent engine — **no new runtime process**. Cancelling a parent cascades
  (bounded by `config :continuum, max_child_depth: 10`) to all in-flight
  descendants, clearing their leases so no post-cancel child events can be
  appended. New events `child_started` / `child_completed` / `child_failed` /
  `child_cancelled`, telemetry `[:continuum, :child, :started | :completed |
  :failed]`, and four nullable `continuum_runs` columns (`parent_run_id`,
  `parent_command_id`, `correlation_id`, `continued_from_run_id`). Child
  workflows require the Postgres journal.
- **Compensation / saga DSL.** `activity/2` accepts a `compensate:` `{m, f, a}`
  option; a successful (`{:ok, value}`) compensated activity returns
  `{:ok, %Continuum.ActivityRef{}}` carrying the compensation handle (activities
  *without* `compensate:` are unchanged and still return a bare term). Two new
  workflow macros roll work back:
  - `compensate/1` — run one activity's compensation (by its `ActivityRef`) and
    drop it from the pending set so `compensate_all/0` can't double-run it.
  - `compensate_all/0` — run every pending compensation in LIFO order (most
    recent first); ideal in a `rescue` clause.

  Compensations flow through the same activity worker, retry policy, timeout,
  idempotency side-table, and lease-fencing path as ordinary activities, and a
  compensation that fails terminally journals `compensation_failed` without
  killing the run. `Continuum.unwrap/1` recovers an activity's raw return from a
  ref. New events `compensation_scheduled` / `compensation_completed` /
  `compensation_failed` and telemetry `[:continuum, :compensation, :scheduled |
  :completed | :failed]`. Compensations are captured by snapshots.
- `Continuum.patched?/1` is now a real, journaled patch marker (was a `false`
  stub). It is a macro (capturing `__CALLER__` for a stable command identity);
  the first call at a source line journals a `patched` event with `value: true`
  and returns `true`, and the value replays on resume. Runs replaying history
  recorded *before* the patch line return `false` without consuming an event,
  keeping in-flight runs on the old branch. `patched?/1` is the only effect that
  may return without advancing the replay cursor, and the non-advance is keyed
  on `command_id` lookahead so independent patch calls don't interfere. Patch
  decisions are captured by snapshots. New telemetry `[:continuum, :patched,
  :hit]`. Modules calling it must `require Continuum` (`use Continuum.Workflow`
  does this); outside a workflow it returns `false`.
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
- `20260801000000_continuum_v0_3` adds four nullable `continuum_runs` columns
  (`parent_run_id`, `parent_command_id`, `correlation_id`,
  `continued_from_run_id`) plus partial indexes on the non-null ids. Old
  `SELECT *` code keeps working; existing rows are backfilled with
  `correlation_id = id`.

### Behavior changes operators should know about

- Resuming Postgres-backed runs now dispatches through the run row's journaled
  `workflow` and `version_hash`; it no longer trusts the latest logical module.
- Runs whose journaled workflow version cannot be resolved are marked
  `:stuck_unknown_version` instead of being replayed through possibly changed
  code.
- Starting a durable run now fails loudly if the workflow module does not
  expose `__continuum_workflow__/0`.

### Observability

- The Observer run-detail timeline now colours `compensation_*`, `child_*`,
  `run_continued_as_new`, and `patched` events, and the run header links to the
  `parent_run_id` and the "continued from / continues to" runs of a
  `continue_as_new` chain.
- `Continuum.OpenTelemetry` adds a `continuum.compensation_attempt` span and
  records child-workflow and `continue_as_new` events as breadcrumbs on the
  originating run-attempt span (a child's own work is captured by its own run
  spans, correlated by run id).

### Benchmarks

- `MIX_ENV=test mix run bench/snapshot_bench.exs` on 2026-05-29 with 10,000
  side-effect events reported raw replay 100 ms, snapshot replay 13 ms, and a
  7.2x replay speedup. The v0.3 re-bench does not close the >=10x snapshot
  target; snapshot payload format and the remaining perf gap stay deferred.

### Telemetry additions

- `[:continuum, :run, :continued_as_new]`
- `[:continuum, :run, :unknown_version]`
- `[:continuum, :child, :started | :completed | :failed]`
- `[:continuum, :compensation, :scheduled | :started | :completed | :failed]`
- `[:continuum, :patched, :hit]`

### Documentation

- Added guides for sagas, child workflows, long-running workflows, patching,
  and workflow versioning.
- Added `MIGRATING_v0_2_to_v0_3.md`.
- Updated `continuum_example_orders` with a refund compensation and a
  parent/child batch workflow.

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
