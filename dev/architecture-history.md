# Historical architecture and roadmap

This is the original planning document, retained as design history. Its API
sketches, schema, guarantees, dates, and milestone statuses are not current.
See [the current roadmap](../ROADMAP.md), [agent guidance](../CLAUDE.md),
and the [supported guides](../guides/) before implementing changes. In particular,
unknown workflow versions are left suspended for capable workers; the old
`stuck_unknown_version` design is historical.

---

## Context

Durable execution stopped being niche in 2025-2026: AI agents need long-lived, crash-resumable orchestration of LLM/tool calls; classical SaaS use cases (payments, onboarding, sagas) want it; every other ecosystem now has an answer (Temporal, Restate, DBOS, Inngest, Hatchet, LangGraph). Elixir does not. The closest contenders leave the slot unfilled:

- `temporal_sdk` — third-party, BUSL-1.1, ~440 lifetime downloads, requires running a Temporal cluster (architecturally wrong for BEAM).
- `wavezync/durable` — v0.0.0-alpha, 86 downloads, single-author, pre-1.0.
- `Runic` — in development, web UI not production-ready per its author.
- `Commanded` — different mental model (CQRS/ES), not a workflow engine.
- `Oban` — best-in-class job queue, but no deterministic replay, signals, multi-day timers, or parent/child workflows. Per its maintainer (Mar 2025), even Oban Web has no workflow view.

The opportunity is to be the **Phoenix-equivalent answer to durable execution** — not a Temporal SDK, but the Elixir-native answer to the same category, borrowing the good ideas, not the architecture. The hardest engineering problem (deterministic replay) maps unusually well onto BEAM: immutability, pattern matching, cheap processes, hot code reload, compile-time AST access.

Today is 2026-04-30. Project root: `/home/yeger/new_lib_elixir/` (currently empty).

### Decisions confirmed by the user

1. **Name:** `Continuum`.
2. **v0.1 scope:** Tight MVP. No LiveView observer in v0.1. Single-node. Postgres-only.
3. **Activity execution:** Built-in worker (Continuum-owned). Oban adapter shipped later as optional.
4. **Licensing:** Pure OSS, no commercial tier. Revisit post-1.0.

---

## Vision and positioning

**Continuum is to durable execution what Phoenix is to web and Oban is to job queues.** It targets Elixir-first teams that want one obvious answer to "how do I run a multi-step business process that survives a crash?" — not a polyglot cluster, not a paid SaaS dependency, not a hand-rolled GenServer state machine.

**Core promise:** write your workflow as straight-line Elixir code. Effects go through `activity/2`. Failures, restarts, and node death cause the workflow to resume *exactly where it left off* with identical state, by replaying its event history through the same pure orchestration code.

**Out of scope, deliberately:** polyglot SDKs, cross-language activities, separate cluster service, Kubernetes operators. Continuum lives in your Elixir app's supervision tree.

---

## Top-level module surface

```
Continuum                     # facade: start/3, signal/3, query/3, cancel/2, list/1
Continuum.Workflow            # `use Continuum.Workflow` — the defworkflow DSL
Continuum.Activity            # `use Continuum.Activity` — retry policy DSL
Continuum.Repo                # Ecto repo (BYO via config; defaults to Continuum.Repo)
Continuum.Telemetry           # documented event names
Continuum.Test                # in-memory journal, signal/timer injection, golden replay
Continuum.Pure                # `use Continuum.Pure` — opt helpers into the trusted set
```

Internal (prefixed `Continuum.Runtime.*`, considered private):

```
Continuum.Runtime.Engine          # GenServer-per-run, the heart of replay
Continuum.Runtime.Context         # process-dict-backed effect context (cursor, run_id)
Continuum.Runtime.Journal         # append/load/CAS-write of event history
Continuum.Runtime.Effect          # the :continuum_suspend token + handlers
Continuum.Runtime.Dispatcher      # polls DB, leases work to local nodes
Continuum.Runtime.Registry        # local {run_id -> pid}
Continuum.Runtime.RunSupervisor   # DynamicSupervisor for workflow GenServers
Continuum.Runtime.Lease           # advisory-lock + heartbeat owner enforcement
Continuum.Runtime.SignalRouter    # Postgres LISTEN consumer, routes signals
Continuum.Runtime.TimerWheel      # in-memory due-queue, hydrated from DB
Continuum.Runtime.ActivityWorker  # built-in activity executor (DynamicSupervisor + queue)
Continuum.Runtime.Recovery        # boot-time orphan scan
Continuum.AstCheck                # compile-time determinism scanner
Continuum.VersionRegistry         # AST hash → versioned module name
Continuum.Schema.{Run,Event,Signal,Timer,ActivityTask}
```

### Supervision tree (`Continuum.Application`, `:one_for_one`)

```
├── Continuum.Repo                          (Ecto, BYO-configurable)
├── Phoenix.PubSub (continuum_pubsub)       (in-process fanout for signals/observers)
├── :pg scope :continuum                    (cluster-wide presence; v0.5+ uses, no-op in v0.1)
├── Continuum.Runtime.Registry              (local Registry, :unique keys)
├── Continuum.Runtime.SignalRouter          (Postgres LISTEN connection)
├── Continuum.Runtime.TimerWheel            (gen_server with :ets due-index)
├── Continuum.Runtime.Dispatcher            (poller for unleased runs)
├── Continuum.Runtime.Lease.Heartbeater     (renews leases for owned runs)
├── Continuum.Runtime.RunSupervisor         (DynamicSupervisor — workflow GenServers)
├── Continuum.Runtime.ActivityWorker.Supervisor  (DynamicSupervisor + dispatcher for activities)
└── Continuum.Runtime.Recovery              (transient — scans on boot, then exits)
```

**Note on Oban:** Continuum is *not* supervised by Oban and does not require it. The optional `Continuum.Oban` adapter (post-v0.1) lets users route activity execution to an Oban queue they already operate, but the default is Continuum's own activity worker pool.

---

## Programming model

### Workflow definition

```elixir
defmodule MyApp.OrderFlow do
  use Continuum.Workflow, version: 1, retention: {:days, 30}

  def run(%{order_id: id, items: items}) do
    {:ok, validated} = activity Validation.check(items)
    {:ok, charge}    = activity Payments.charge(id, validated.total),
                                retry: [max_attempts: 5, backoff: :exponential]

    case await signal(:fraud_review, timeout: hours(24)) do
      {:ok, :approved} -> activity Fulfillment.ship(id)
      {:ok, :rejected} -> compensate(charge); {:error, :rejected}
      :timeout         -> activity Fulfillment.ship(id)
    end
  end
end
```

`use Continuum.Workflow`:
1. Imports macros: `activity/1,2`, `await/1`, `signal/1,2`, `timer/1`, `compensate/1`, `hours/1`, `minutes/1`, `seconds/1`.
2. Generates `__continuum_workflow__/0` returning module metadata (`version`, `retention`, AST hash).
3. Workflows are started through `Continuum.start/3`; generated workflow modules are not direct supervision children.
4. Performs an AST scan over `def run/1` (and any private `defp` in the same module) using `Continuum.AstCheck`. Determinism violations are compile errors with file/line and a remediation hint (see *Determinism enforcement*).

### Activity definition

```elixir
defmodule Payments.Charge do
  use Continuum.Activity,
    retry: [max_attempts: 5, backoff: :exponential, base_ms: 500],
    timeout: {:seconds, 30}

  @impl true
  def run(order_id, amount) do
    Stripe.charge(order_id, amount)   # ordinary Elixir; can raise; can do anything
  end
end
```

Activities are ordinary modules. They are not subject to the determinism scanner — that's their entire purpose.

### The two layers

- **Workflow code** is pure orchestration. All effects flow through journaled primitives (`activity`, `await`, `signal`, `timer`, `Continuum.now/0`, `Continuum.uuid4/0`, `Continuum.random/0`, `Continuum.side_effect/1`). Compile-time scan rejects raw `DateTime.utc_now`, `:rand.*`, `IO.*`, `Process.send`, `apply/3`, `Code.*`, and reads from ETS / `:persistent_term`.
- **Activity code** is unconstrained. It does HTTP, DB, NIFs, anything. Its return value is journaled on first success and replayed thereafter.

---

## Runtime: how `defworkflow` actually executes

A workflow run is a `GenServer` whose state is `{history, cursor, lease, run_id, workflow_module}`. The user's `def run/1` is a **continuation** that gets re-executed from the top every time the engine wakes the run.

### The core idea

Continuations are not first-class on BEAM (no `call/cc`, no serializable coroutine pause). So we re-run `run/1` from the top, with effectful primitives reading from the journal instead of the world. This works because:

1. Workflow code is *enforced* deterministic by the AST scanner.
2. Each effect call consults `ctx.history` at the current cursor; if a result is already journaled, return it instantly without external side effects; if past the journal's tail, **suspend** by `throw {:continuum_suspend, reason}` to unwind back to the engine loop.
3. The engine, on suspend, journals an `*_scheduled` event, schedules the work (activity dispatch, timer arming, signal waiter registration), and parks the GenServer.
4. When the work completes (activity result arrives, timer fires, signal received), the engine re-enters the replay loop. The replay is fast because activity bodies don't re-execute — only the orchestration code runs.

### Replay loop sketch

```elixir
def handle_continue(:run, state) do
  history = Journal.load(state.run_id)
  ctx = %Context{
    history: history,
    cursor: 0,
    run_id: state.run_id,
    lease_token: state.lease_token
  }
  Process.put(:continuum_ctx, ctx)

  try do
    result = state.workflow_module.run(state.input)
    Journal.append!(state.run_id, :run_completed, %{result: result}, state.lease_token)
    {:stop, :normal, state}
  catch
    {:continuum_suspend, reason} ->
      handle_suspend(reason, state)
  end
end
```

`activity Mod.fun(args)` is a macro that captures `{Mod, :fun, args}` literally (we cannot evaluate args at macro time; we record the call shape). It expands to:

```elixir
Continuum.Runtime.Effect.run({:activity, {Validation, :check, [items]}, opts}, __ENV__.line)
```

`Continuum.Runtime.Effect.run/2`:
- Reads `ctx = Process.get(:continuum_ctx)`.
- If `ctx.history[cursor]` exists and matches the expected effect shape → return its recorded result, advance cursor.
- If `ctx.history[cursor]` exists but doesn't match → `raise Continuum.ReplayDriftError` with detailed diff (expected `{:activity, {Validation, :check, _}}`, found `{:timer, ...}`).
- If past the journal tail → append `*_scheduled` (CAS-guarded by lease token), then `throw {:continuum_suspend, ...}`.

### Effect markers (event types)

| event_type             | payload                                                     |
|------------------------|-------------------------------------------------------------|
| `run_started`          | input, workflow_module, version_hash, parent_run_id         |
| `activity_scheduled`   | activity_id, mfa, attempt, retry_policy                     |
| `activity_completed`   | activity_id, result                                         |
| `activity_failed`      | activity_id, error, retry_at, attempt                       |
| `timer_started`        | timer_id, fires_at                                          |
| `timer_fired`          | timer_id                                                    |
| `signal_awaited`       | signal_name, timeout_at                                     |
| `signal_received`      | signal_name, payload, sender                                |
| `compensation_scheduled` | target_activity_id, mfa                                   |
| `side_effect`          | command_id, payload (e.g. recorded `now()`/`uuid4()`)       |
| `version_marker`       | workflow_version_hash (first event of every run)            |
| `patched`              | patch_name, value (Continuum.patched? returns)              |
| `run_completed`        | result                                                      |
| `run_failed`           | error                                                       |

Cursor identity uses **structured keys**, not flat counters: `command_id = {:activity, line, MFA_hash, ordinal}`. Adding a `Continuum.now/0` between two activities does not silently shift sequence — replay validation matches the AST position and produces a precise error.

---

## Postgres schema

```sql
-- Per-run row
CREATE TABLE continuum_runs (
  id                uuid PRIMARY KEY,
  workflow          text NOT NULL,
  version_hash      bytea NOT NULL,
  state             text NOT NULL,  -- 'running' | 'suspended' | 'completed' | 'failed' | 'cancelled'
  input             jsonb NOT NULL,
  result            jsonb,
  error             jsonb,
  started_at        timestamptz NOT NULL DEFAULT now(),
  completed_at      timestamptz,
  -- leasing
  lease_owner       text,                -- "node@host:pid"
  lease_token       bigint,              -- monotonic; the fencing token
  lease_expires_at  timestamptz,
  -- scheduling
  next_wakeup_at    timestamptz,         -- earliest moment Dispatcher should pick up
  retention_until   timestamptz
) WITH (fillfactor = 70);  -- HOT updates for lease heartbeat

CREATE INDEX continuum_runs_dispatch_idx
  ON continuum_runs (next_wakeup_at NULLS LAST)
  WHERE state = 'suspended' AND lease_owner IS NULL;

CREATE INDEX continuum_runs_lease_idx
  ON continuum_runs (lease_expires_at)
  WHERE lease_owner IS NOT NULL;

-- Append-only event history, partitioned monthly
CREATE TABLE continuum_events (
  run_id      uuid NOT NULL,
  seq         bigint NOT NULL,
  event_type  text NOT NULL,
  payload     jsonb NOT NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, seq)
) PARTITION BY RANGE (inserted_at);

-- Durable signal mailbox until consumed
CREATE TABLE continuum_signals (
  id          bigserial PRIMARY KEY,
  run_id      uuid NOT NULL,
  name        text NOT NULL,
  payload     jsonb NOT NULL,
  delivered   boolean NOT NULL DEFAULT false,
  inserted_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON continuum_signals (run_id, name) WHERE delivered = false;

-- Timers indexed by due time
CREATE TABLE continuum_timers (
  id        uuid PRIMARY KEY,
  run_id    uuid NOT NULL,
  fires_at  timestamptz NOT NULL,
  fired     boolean NOT NULL DEFAULT false
);
CREATE INDEX continuum_timers_due_idx
  ON continuum_timers (fires_at) WHERE fired = false;

-- Activity tasks (Continuum-owned worker; not Oban)
CREATE TABLE continuum_activity_tasks (
  id              uuid PRIMARY KEY,
  run_id          uuid NOT NULL,
  seq             bigint NOT NULL,
  mfa             jsonb NOT NULL,
  attempt         int  NOT NULL DEFAULT 1,
  state           text NOT NULL,          -- 'available' | 'leased' | 'completed' | 'discarded'
  scheduled_at    timestamptz NOT NULL DEFAULT now(),
  available_at    timestamptz NOT NULL DEFAULT now(),
  lease_owner     text,
  lease_expires_at timestamptz,
  result          jsonb,
  error           jsonb
);
CREATE INDEX continuum_activity_tasks_pickup_idx
  ON continuum_activity_tasks (available_at)
  WHERE state = 'available';
```

**Bloat strategy:**
- `continuum_events` partitioned monthly; drop old partitions when retention expires.
- `fillfactor = 70` on `continuum_runs` so HOT updates avoid index bloat.
- Suspended runs awaiting a long signal have `next_wakeup_at = NULL`; the partial index excludes them, so they cost nothing to query past.
- Completed runs older than `retention_until` are archived to `continuum_runs_archive` (cold, no leasing indexes).

---

## Determinism enforcement

This is the load-bearing technical claim: that workflows in Continuum are deterministic by construction, not by user discipline.

### Layer 1 — compile-time AST scan

`Continuum.AstCheck` walks the `def run/1` body (and same-module `defp`s) via `Macro.prewalk/2`, rejecting calls in a curated denylist:

```elixir
@forbidden %{
  {DateTime, :utc_now}      => "use Continuum.now/0",
  {DateTime, :now!}         => "use Continuum.now/0",
  {Date, :utc_today}        => "use Continuum.today/0",
  {:rand, :uniform}         => "use Continuum.random/0",
  {:rand, :uniform_real}    => "use Continuum.random/0",
  {System, :os_time}        => "use Continuum.now/0",
  {System, :system_time}    => "use Continuum.now/0",
  {System, :monotonic_time} => "monotonic time is non-deterministic on replay",
  {System, :get_env}        => "read env at workflow start; pass as input",
  {Node, :list}             => "cluster topology is non-deterministic",
  {Node, :self}             => "use workflow_info().node_at_start",
  {Process, :send}          => "use Continuum.signal_child/2",
  {Process, :send_after}    => "use Continuum.timer/1",
  {IO, :puts}               => "use Continuum.log/1 (journaled)",
  {File, :read}             => "wrap in Continuum.activity/2",
  {:ets, :lookup}           => "ETS bypasses the journal — wrap in an activity",
  {:persistent_term, :get}  => "persistent_term bypasses the journal",
  {Kernel, :apply}          => "dynamic dispatch forbidden in workflows",
  {Code, :eval_string}      => "code evaluation is non-deterministic"
}
```

Diagnostic format:

```
== Determinism violation in MyApp.OrderFlow.run/1 ==
  lib/my_app/order_flow.ex:42

      shipped_at = DateTime.utc_now()
                   ^^^^^^^^^^^^^^^^^^^

  DateTime.utc_now/0 is non-deterministic on replay.
  Use Continuum.now/0 instead — it journals the timestamp on first
  execution and replays the recorded value.
```

### Layer 2 — transitive helper validation (`use Continuum.Pure`)

Workflows often call into helper modules. The scanner cannot check those at the workflow's compile time. Resolution:

- Helper modules call `use Continuum.Pure`. The same AST scan is applied to all functions in that module at compile time.
- Stdlib pure-by-construction modules (`Enum`, `Map` minus `to_list/iterable order`, `String`, `Integer`, `Decimal`) are an allowlist baked in.
- Calls into unmarked modules from workflow code produce a *warning* listing every external module reference. Users opt-in via `config :continuum, trusted_modules: [Decimal, Money]` after auditing. Configurable to error.

### Layer 3 — runtime trap

Deterministic primitives (`Continuum.now/0`, `Continuum.uuid4/0`, `Continuum.random/0`, `Continuum.side_effect/1`) check `Process.get(:continuum_in_workflow)` and journal-or-replay. The process dictionary is set in the engine's `handle_continue(:run, ...)` and unset on suspend/exit.

`Continuum.side_effect/1` is the general escape hatch: runs a function once, journals the result, replays thereafter. Validates that the return value is `:erlang.term_to_binary`-roundtrippable (refuses pids/refs/ports).

### The map-ordering trap

Elixir maps lose insertion order past 32 keys. The scanner flags `Enum.map/2`, `Enum.reduce/3`, `for ... <- map`, `Map.to_list/1`, `:maps.iterator/1` whose argument is statically inferable as a map. The macro **auto-rewrites** `for {k, v} <- some_map` inside `defworkflow` to use `:maps.iterator(m, :ordered)` — the most ergonomic fix. Users opt out per-call with `Continuum.unsafe_unordered/1` if they're sure.

### Drift detection at replay

Every journaled payload is type-stamped with the struct module hash on write. On replay, struct-shape drift produces `Continuum.StructDriftError` with file/line and field diff — never a silent `MatchError`. Each event's payload is CRC'd; the journal as a whole is chain-hashed; replay validates the chain before starting.

---

## Activity execution

**v0.1 ships a built-in activity worker pool. Oban is not required.**

### Lifecycle

1. Workflow hits `activity Payments.charge(...)`. Engine, in one DB transaction:
   - Appends `activity_scheduled` to `continuum_events` (CAS-guarded by lease token).
   - Inserts a row in `continuum_activity_tasks`.
   - Throws `:continuum_suspend`.
2. `Continuum.Runtime.ActivityWorker.Dispatcher` polls `continuum_activity_tasks`:
   ```sql
   SELECT id, run_id, seq, mfa, attempt FROM continuum_activity_tasks
   WHERE state = 'available' AND available_at <= now()
   ORDER BY available_at LIMIT $1 FOR UPDATE SKIP LOCKED;
   ```
   Each picked task is leased (CAS update), dispatched to a worker process under `Continuum.Runtime.ActivityWorker.Supervisor`.
3. The worker invokes the MFA inside `try/rescue` with the activity's `timeout`.
4. **On success**, in one transaction: write `activity_completed` to `continuum_events`, mark task `completed`, then `pg_notify` the run's owner node. The run's GenServer wakes via the same dispatch path as a signal, replays from history, and proceeds.
5. **On failure**, the worker re-raises. Retry policy (read from the activity module or call-site override) determines `available_at = now() + backoff`. After `max_attempts`, mark `discarded`, write `activity_failed`, wake the run.

### Idempotency

Activities are not magically idempotent — that's the user's responsibility in v0.1. Continuum provides:
- `Continuum.Activity` callback `idempotency_key/1` (optional). v0.1 stores the resolved key in the durable task payload; v0.2 adds side-table enforcement so duplicate executions can consult the prior result instead of rerunning the side effect.
- Planned `Continuum.Activity.exactly_once/2` helper backed by a side-table that records `(activity_id, attempt) -> result_hash`.
- Documentation guide: *Activities, retries, and idempotency* — flagged as the most important page in the docs.

### Why not Oban for v0.1

The user's brief originally suggested Oban for activity execution. Discussion with the user confirmed building our own worker is the right call:
- Forcing Oban as a hard dep limits adoption and couples our release cadence to Oban's.
- The leasing/CAS mechanics for activity tasks are essentially identical to those for runs — building it once is straightforward.
- Post-v0.1 we ship `Continuum.Oban` as an optional adapter for teams that already operate Oban and want to consolidate queues.

---

## Signals, timers, distribution

### Signals

External code calls `Continuum.signal(run_id, :fraud_review, payload)`. This:
1. Inserts a row in `continuum_signals`.
2. Issues `pg_notify('continuum_signal', run_id::text)`.
3. `UPDATE continuum_runs SET next_wakeup_at = now() WHERE id = run_id` (so a poll-based fallback wakes the run even if NOTIFY drops).

`Continuum.Runtime.SignalRouter` holds a dedicated Postgres LISTEN connection. On notification: check the local `Registry`; if found, send `{:signal, ...}` to the pid. If not local, in v0.1 (single-node) this is a no-op — the dispatcher's poll picks it up. (v0.5 distribution adds `:pg`-based forwarding to the owner node.)

### Timers

`timer(hours(24))` desugars to: append `timer_started`, insert `continuum_timers` row with `fires_at = now() + 24h`, set `next_wakeup_at = fires_at` on the run, suspend.

`Continuum.Runtime.TimerWheel`:
- On boot, loads timers due in the next 60s into ETS.
- Every 30s, refreshes the window.
- On due, marks `fired=true`, NOTIFYs, the run wakes via the same path as signals.

Surviving node restart: timers are durable in Postgres. The wheel is only a cache. After downtime, missed timers (`fires_at < now()`) fire immediately.

### Combined signal + timer (`await signal(_, timeout: ...)`)

Desugars to a parallel wait: whichever event arrives first wins. Exactly one of `signal_received` or `timer_fired` is journaled, so replay is deterministic.

### Distribution (v0.5+)

In v0.1, single-node. The leasing model already works correctly across nodes; `:pg` registration of run pids and Distributed Erlang forwarding of signals are added in v0.5. The architecture does not need to change to gain distribution — only the SignalRouter and Dispatcher gain a "if not local, forward via `:pg`" branch.

### Lease & fencing

> At any instant, at most one BEAM process in the cluster runs a given run_id's GenServer.

Enforcement (works the same whether single-node or clustered):

1. **Postgres lease.** When a node wants to run a suspended run:
   ```sql
   UPDATE continuum_runs
   SET lease_owner = $1, lease_token = nextval('continuum_lease_token_seq'),
       lease_expires_at = now() + interval '30 seconds'
   WHERE id = $2 AND (lease_owner IS NULL OR lease_expires_at < now())
   RETURNING id, lease_token;
   ```
   Atomic CAS. If empty, someone else owns it; back off. The `lease_token` is the **fencing token**.
2. **`Lease.Heartbeater`** renews every 10s by re-CAS with `WHERE lease_owner = $current AND lease_token = $current_token`.
3. **Every event-write** (`Journal.append!`) carries the lease token in its `WHERE` clause. If the lease was stolen (network partition), the write fails, the engine forcibly terminates the run process — no further effects can be journaled by a stale owner.

Dispatcher poll uses `FOR UPDATE SKIP LOCKED` so multiple nodes poll concurrently without contention.

---

## Versioning workflows

In-flight workflows must finish on the code they started on. New starts pick up the latest. Achieved by **content-addressed code**:

1. The `defworkflow` macro computes `workflow_version_hash = sha256(normalized_AST(run/1) ++ transitive_pure_helpers)`. Normalization strips line metadata, `@moduledoc`/`@doc`, variable counters; preserves clause order, literals, referenced module attributes.
2. At deploy: each compiled hash is registered in `Continuum.VersionRegistry` (ETS + durable table). The compiled module is registered as `MyApp.OrderFlow.V_<hash>`. The plain `MyApp.OrderFlow` is a one-line `defdelegate` alias to the latest hash.
3. The first journaled event of every run is `version_marker` carrying the hash.
4. When a worker picks up a suspended run, it dispatches into `MyApp.OrderFlow.V_<journaled_hash>.run/1`. If that module is not loaded *and* not in the registry → loud failure: `Continuum.UnknownVersionError`; the run is marked `:stuck_unknown_version` and an operator alert fires. **No silent corruption.**
5. Old version modules are not unloaded. Memory cost is accepted; an opt-in `mix continuum.gc_versions` task removes versions with zero in-flight runs.

### Patched / opt-in branching

```elixir
if Continuum.patched?(:add_fraud_check_v2) do
  activity FraudCheck.v2(input)
else
  activity FraudCheck.v1(input)
end
```

`Continuum.patched?/1` journals on first call; replays the journaled value thereafter. Old runs without the journaled patch take the old branch; new runs take the new. Equivalent in spirit to Temporal's `workflow.patched()` — same downside (cruft in code), but `mix continuum.audit` surfaces "patch X has been live N days; all in-flight runs have moved past it; safe to remove."

### Breaking-change escape hatch

For changes the patched-branch trick can't express (incompatible state shape), the user bumps `@workflow_version`:

```elixir
use Continuum.Workflow, version: 2
def run(input, version: 2), do: ...
def run(input, version: 1, deprecated: true), do: ...   # kept around
```

In-flight v1 runs continue against the v1 clause; new starts get v2.

### Honest comparison to Temporal

- **Win:** AST-hash + version-keyed module name is cleaner than Temporal's "workers register support for task queue X" indirection. Compile-time AST access lets `mix continuum.audit` surface stale patches in a way Python's runtime sandbox can't match cleanly.
- **Not a win:** `Continuum.patched?/1` is structurally identical to `workflow.patched()` and shares its downside. Anyone claiming Elixir solves versioning entirely is hand-waving.

---

## File layout

```
/home/yeger/new_lib_elixir/
├── mix.exs
├── README.md                                   (5-minute-to-running-workflow tutorial)
├── CHANGELOG.md
├── LICENSE                                     (Apache-2.0)
├── config/
│   └── config.exs
├── lib/
│   ├── continuum.ex                            # facade
│   ├── continuum/
│   │   ├── application.ex                      # supervision tree
│   │   ├── workflow.ex                         # `use` macro, defworkflow desugaring
│   │   ├── activity.ex                         # `use` macro, retry policy DSL
│   │   ├── pure.ex                             # `use Continuum.Pure` for helper modules
│   │   ├── repo.ex
│   │   ├── telemetry.ex
│   │   ├── test.ex                             # in-memory journal, golden-replay helpers
│   │   ├── ast_check.ex                        # compile-time determinism scanner
│   │   ├── version_registry.ex                 # AST hash → versioned module
│   │   ├── runtime/
│   │   │   ├── engine.ex                       # GenServer-per-run, replay loop
│   │   │   ├── context.ex                      # process-dict effect context
│   │   │   ├── journal.ex                      # append/load/CAS-write
│   │   │   ├── effect.ex                       # :continuum_suspend token + handlers
│   │   │   ├── dispatcher.ex                   # polls runs awaiting wakeup
│   │   │   ├── registry.ex                     # local {run_id -> pid}
│   │   │   ├── run_supervisor.ex               # DynamicSupervisor for runs
│   │   │   ├── lease.ex                        # CAS lease + fencing token
│   │   │   ├── lease/heartbeater.ex
│   │   │   ├── signal_router.ex                # Postgres LISTEN consumer
│   │   │   ├── timer_wheel.ex                  # ETS-cached due queue
│   │   │   ├── activity_worker/
│   │   │   │   ├── supervisor.ex               # DynamicSupervisor + dispatcher
│   │   │   │   ├── dispatcher.ex               # polls continuum_activity_tasks
│   │   │   │   └── worker.ex                   # invokes the MFA, journals result
│   │   │   └── recovery.ex                     # boot-time orphan scan
│   │   └── schema/
│   │       ├── run.ex
│   │       ├── event.ex
│   │       ├── signal.ex
│   │       ├── timer.ex
│   │       └── activity_task.ex
│   └── mix/
│       └── tasks/
│           ├── continuum.gen.migration.ex      # generates schema migration
│           ├── continuum.gen.workflow.ex       # generates a workflow module
│           ├── continuum.gen.activity.ex       # generates an activity module
│           └── continuum.audit.ex              # determinism + stale-patch audit
├── priv/
│   └── repo/
│       └── migrations/                         # generated by mix continuum.gen.migration
└── test/
    ├── support/
    │   ├── fake_clock.ex
    │   ├── test_repo.ex
    │   └── test_workflows.ex
    ├── continuum_test.exs
    ├── workflow/
    │   ├── replay_test.exs                     # property-based replay on StreamData
    │   ├── ast_check_test.exs
    │   ├── versioning_test.exs
    │   └── struct_drift_test.exs
    ├── runtime/
    │   ├── engine_test.exs
    │   ├── journal_test.exs
    │   ├── lease_fencing_test.exs              # critical: simulates partition
    │   ├── activity_worker_test.exs
    │   ├── signal_routing_test.exs
    │   ├── timer_wheel_test.exs
    │   └── recovery_test.exs
    └── integration/
        ├── crash_resume_test.exs               # spawn run, kill mid-flight, assert resume
        ├── retry_with_backoff_test.exs
        └── golden_history_test.exs
```

---

## Phased roadmap

> **SUPERSEDED — do not plan from this section.** See
> `AUDIT_FINDINGS_v0_7_1.md` §3. Six shipped releases (v0.6.0 → v0.7.2) have no
> entry here; the current milestone is v0.8 "Finish the foundations".

**v0.1 — "It survives a crash" — DONE (local; not yet pushed/published)**
- ✅ `use Continuum.Workflow` + `use Continuum.Activity` macros with AST scan.
- ✅ Postgres event-sourced history (`continuum_events`, append-only, `bytea` payloads).
- ✅ Built-in activity worker pool (Continuum-owned, Oban-free) with `FOR UPDATE SKIP LOCKED` claim and per-task fencing.
- ✅ Deterministic replay from history with structured `command_id` cursor identity (catches drift even when shapes match).
- ✅ Activity retries with exponential backoff, idempotency-key plumbing.
- ✅ Durable timers, durable signals via `pg_notify` + Postgrex LISTEN, `await signal(_, timeout: _)` (signal vs timer race resolved deterministically).
- ✅ Telemetry events on every state transition (24+ named events).
- ✅ Lease + fencing token; CAS-guarded writes; lease loss terminates the stale engine.
- ✅ `mix continuum.gen.{migration,workflow,activity}`.
- ✅ ExDoc reference + 3 guides + 1 example app (`continuum_example_orders`).
- ✅ `Continuum.Test` with in-memory journal, Postgres helpers, golden-history replay, and signal/timer injection.
- ✅ Crash-resume integration test, lease-fencing race test, StreamData property-based replay test (side_effect + mixed flows).

**v0.1 explicitly cuts** (each replaceable in user code at the cost of some ugliness):
- LiveView observer (use IEx + Telemetry).
- Compensation/saga DSL (use `try/rescue` + cleanup activity).
- Parent/child workflows (start a child workflow from inside an activity, signal back).
- `continue_as_new` (defer until long-running workflows are real).
- Search attributes/queries (read `continuum_runs` directly).
- Cluster distribution (single node + Postgres-as-truth handles ~1k workflows/sec).
- Real `patched?/1` (ships as a stub returning `false`; real journaling in v0.3).
- Oban adapter.

**v0.1 known limitations carried forward to v0.2:**
- `continuum_events` is unpartitioned (retention story lands in v0.2).
- TimerWheel is a poller, not the ETS-cached due-queue (perf upgrade in v0.2).
- AST scan over unmarked helper modules produces no warning (v0.2 polish).
- `signal_awaited` is journaled even when a signal is already waiting (cosmetic; two events instead of one).
- `Continuum.Activity`'s `idempotency_key/1` is plumbed but not enforced by a side-table (real exactly-once-ish semantics in v0.2).
- `config :continuum, :repo` is a global app-env value (per-process repo threading in v0.2).
- `Continuum.VersionRegistry` is a stub; content-addressed module dispatch and real `patched?/1` land in v0.3.
- **No multi-node test harness.** Every "node death" scenario in the v0.1 suite is process-death on a single BEAM (`Process.exit(engine, :kill)`, then `Recovery.recover_once` + `Dispatcher.dispatch_once` in the same VM). Lease-theft tests simulate the contention by writing a different owner directly into `continuum_runs`. The fencing-token *protocol* is exercised; the *transport* (two BEAMs racing over one Postgres, network partition, node up/down churn) is not. v0.1 is single-node by design and the `:peer`-based multi-BEAM tests land alongside the actual cluster work in v0.5 — see that milestone for the three scenarios in scope.

**v0.2 — "I can see what's happening — and the engine pays the rent in production" — DONE (2026-05-17)**

New surfaces:
- ✅ `Continuum.Observer` (optional Phoenix LiveView): runs index with state/workflow/run-id search + simple pagination; run detail with decoded event timeline; operator actions for cancelling a run and sending a JSON signal payload. Mounted via `Continuum.Observer.Router.continuum_observer/2` — host app owns auth. Continuum compiles cleanly without `:phoenix_live_view` installed; a self-contained dev demo lives at `dev/observer_demo.exs`. **Replay-stepping debugger is deferred to v0.3+.**
- ✅ `Continuum.OpenTelemetry.setup/1` — opt-in bridge that turns `[:continuum, :run, ...]` and `[:continuum, :activity, ...]` telemetry into short OpenTelemetry spans (`continuum.run_attempt`, `continuum.activity_attempt`), linked back to the persisted W3C `traceparent` in `continuum_runs.trace_context`. Continuum still compiles without any OpenTelemetry packages — host app opts in.
- ✅ `Continuum.children/1` — host-supervisor helper that supports running multiple named instances of Continuum in one node (`name: ..., repo: ...`), with runtime calls disambiguated via `instance: ...` on `start/3`, `signal/4`, `cancel/2`, `await/3`. In-memory journal namespaces state by instance.
- ✅ Experimental, opt-in **history snapshots**: `continuum_snapshots` table, `Continuum.Snapshot` compacted-prefix serializer, `Continuum.Runtime.Snapshotter`, compacted-prefix replay validation in `Effect.run/2`. Default `snapshot_threshold: :infinity` (off); users opt in with a positive integer after reading `guides/snapshots.md`. Bench reports ~8× replay-loop cost reduction on a 10k-event side-effect workflow (≥10× target deferred to v0.3; gap is accepted under the original minimum-acceptance clause because runtime use is experimental).

v0.1 debt paid down:
- ✅ Monthly partitioning for `continuum_events` (`PARTITION BY RANGE (inserted_at)`, `PRIMARY KEY (run_id, seq, inserted_at)`), with operator Mix tasks `mix continuum.partitions.{create,list,drop_old}` (`--execute` opt-in). No runtime partition manager in v0.2.
- ✅ Activity idempotency is enforced through `continuum_activity_results` keyed on `(activity_module, idempotency_key)`. Committed results are reused across runs.
- ✅ `signal_awaited` fast-path: when a matching signal is already in the durable mailbox, `await signal(:x)` no longer journals a `signal_awaited` event before the `signal_received`.
- ✅ ETS-cached `TimerWheel` with `pg_notify`-driven reschedule. `bench/timer_wheel_bench.exs` asserts ≥10× DB-query reduction on the idle-timer workload.
- ✅ Per-process repo threading — runtime is no longer keyed off the global `config :continuum, :repo`; each instance carries its own repo through `Continuum.children/1`. Backwards-compat: a single global instance still works.
- ✅ AST scan reaches helper modules: unmarked-helper calls from workflow code warn (or error, configurable via `config :continuum, untrusted_call_severity: :warn | :error`), and an `app-env` allowlist (`config :continuum, trusted_modules: [...]`) bridges third-party modules without `use Continuum.Pure`.
- ✅ `continuum_runs.trace_context` (bytea) persists the W3C `traceparent` of the originating request so resumed run attempts remain correlated without one multi-day span.

Behavior changes (operator-visible):
- ✅ `continuum_events` is now partitioned; the primary key shape changed to `(run_id, seq, inserted_at)`. Global `(run_id, seq)` uniqueness is preserved through run-row locking, not a global SQL unique constraint.
- ✅ `signal_awaited` is no longer journaled when a signal is already pending. Operators reading the events table directly will see one event instead of two.
- ✅ Lease owner format is now `node()/instance/monotonic_int` (was `node()/monotonic_int`) — the middle segment is the instance name. Grep filters that assumed a 2-segment owner need updating.
- ✅ `config :continuum, :repo` is no longer the canonical repo source. Apps using a single global instance keep working; new code threads repos through `Continuum.children/1`.

Tooling and docs:
- ✅ Six guides: `guides/observer.md`, `guides/observability.md`, `guides/snapshots.md`, `guides/multi-instance.md`, `guides/determinism-rules.md`, `guides/idempotency.md`. Linked from `README.md`.
- ✅ Root-level `MIGRATING_v0_1_to_v0_2.md`: migration order (`partition → activity_results → snapshots`), three behavior changes called out above, opt-ins for per-process repos / snapshots / OTel / Observer, partition Mix-task usage, schema invariant note.
- ✅ Example app `continuum_example_orders` refreshed: Observer mounted at `/admin/continuum` under a dedicated `:admin_auth` pipeline (hardcoded `admin/admin` basic auth), OTel exporter wired to a docker-compose Jaeger (OTLP HTTP `localhost:4318`, UI `localhost:16686`), no snapshot demo. End-to-end smoke verified on 2026-05-17.

Module-count moat — revised:
- The ROADMAP's original "~25 core modules" target was a v0.1 working principle. v0.2 deliberately revises it: with Observer, OpenTelemetry, snapshots, multi-instance plumbing, and the Mix-task surface, raw module count is no longer the right shape of the moat. The replacement target is keeping the **runtime** surface small and justified — new runtime processes need a written reason — while allowing optional UI modules (Observer LiveViews, components), Mix tasks, and schema files to land where they make sense. At tag-prep the v0.2 tree has 49 `.ex` files under `lib/`, with 19 under `lib/continuum/runtime/`. The Snapshotter is the only new runtime child in v0.2; everything else under `lib/continuum/observer/` and `lib/mix/tasks/` is optional surface.

**v0.2 explicitly cuts** (still in user code or deferred to v0.3+):
- Replay-stepping debugger in the Observer (v0.3+).
- Built-in Observer authentication / authorization callback — host app owns access control via router pipelines.
- Per-workflow `@snapshot_threshold` attribute (app-env only in v0.2; per-workflow is a v0.3 ergonomics nit if anyone asks).
- Per-workflow `trusted:` option for the AST scan — app-env `trusted_modules` only.
- Runtime Partitioner — partition lifecycle is Mix-task only in v0.2.
- CI matrix workflow in `.github/workflows/` — local six-seed sweep is the verification of record for v0.2; CI lands as a post-tag PR.

**v0.2 known limitations carried forward to v0.3+:**
- Snapshot runtime use is **experimental** in v0.2. Default `snapshot_threshold: :infinity` (off). Public snapshot payload format (`:erlang.term_to_binary` of the struct) is not promised stable.
- `bench/snapshot_bench.exs` reports ~8× replay-loop cost reduction at 10k events; the original ≥10× target is deferred to v0.3 along with runtime dogfooding. Acceptable here because snapshots are opt-in.
- ✅ Resolved in v0.3: `Continuum.VersionRegistry` (content-addressed dispatch) and real journaled `Continuum.patched?/1`.
- ✅ Resolved in v0.3: `compensate` / saga DSL and parent/child workflows (`await child(...)`).
- ✅ Resolved in v0.3: `continue_as_new`.
- ✅ Shipped in v0.3 as `Continuum.Test.Paranoid` (`CONTINUUM_PARANOID=1`): re-replays every terminal history and asserts an identical `(event_type, decoded_payload, command_id)` sequence.
- `mix continuum.audit` is still v0.5.
- Cluster distribution and the `:peer`-based multi-node test harness are still v0.5.

**v0.3 — "Real workflows" — DONE (2026-05-31)**

Programming-model surface:
- ✅ **Saga / compensation DSL.** `activity ..., compensate: {Mod, :fun, args}` returns `%Continuum.ActivityRef{}` on `{:ok, value}` (bare-term return preserved for activities *without* `compensate:`). `compensate/1` runs one activity's compensation; `compensate_all/0` runs all pending ones in sequential LIFO order. Both schedule through the ordinary worker / retry / timeout / idempotency-side-table / lease-fencing path. `Continuum.unwrap/1` recovers the raw activity return.
- ✅ **Parent/child workflows.** `await child Mod.run(input)`, `start_child/3` (returns `%Continuum.ChildRef{}`, accepts `id:`), `await_child/1`. Child run ids are deterministic from `(parent_run_id, command_id, opts[:id])`; only the parent engine writes `child_*` events under the parent lease; child terminal transitions wake the parent via `pg_notify('continuum_run_wake', ...)` consumed by the **existing** `SignalRouter` (no new runtime process). Parent cancellation cascades to descendants, bounded by `max_child_depth` (default 10).
- ✅ **`continue_as_new/1`.** Tail-call continuation with a `correlation_id` (set to the run's own id at creation, propagated down the chain) and `continued_from_run_id`. A distinct `:continuum_continued_as_new` sentinel stops the current engine cleanly; the Dispatcher picks up the new run. Continued children stay children; a parent's `await_child` follows the continued chain forward to the *final* terminal result.
- ✅ **Real `Continuum.patched?/1`.** Promoted to a macro; journals `true` at the live tail, replays the journaled decision, and returns `false` *without advancing the cursor* when replaying pre-patch history. The event-replay and snapshot-replay paths agree (see watch-out in `CLAUDE.md`).
- ✅ **Content-addressed `Continuum.VersionRegistry` + versioned dispatch.** `(workflow, version_hash)` resolved to a loaded entrypoint via `:persistent_term`; per-instance durable upsert into `continuum_workflow_versions` (a one-shot boot `Task`, not a long-running process). Resume dispatch invokes the entrypoint for the run's journaled hash; unresolvable hashes mark the run `:stuck_unknown_version`, fire `[:continuum, :run, :unknown_version]`, and are excluded from the dispatcher claim set (no re-claim loop). PR-1 compromise: keep old concrete modules loaded, mark with `use Continuum.Workflow, workflow: LogicalFlow`.
- ✅ **`Continuum.side_effect/1` composition coverage** with patched branches, compensations, child awaits, snapshots, and continue-as-new (the primitive itself shipped in v0.1).

Determinism / testing:
- ✅ `Continuum.Test.Paranoid` — opt-in re-replay (`CONTINUUM_PARANOID=1`) asserting an identical `(event_type, decoded_payload, command_id)` sequence between original and replay (DB-stamped fields excluded).

Schema:
- ✅ One delta migration (`20260801000000_continuum_v0_3`) adds four nullable `continuum_runs` columns (`parent_run_id`, `parent_command_id`, `correlation_id`, `continued_from_run_id`; existing rows backfilled `correlation_id = id`) plus partial indexes, and the `continuum_workflow_versions` table.

Observability:
- ✅ Nine new telemetry events (child `started/completed/failed`, compensation `scheduled/started/completed/failed`, `run.continued_as_new`, `run.unknown_version`, `patched.hit`). Observer renders the new event types with labels + colours and shows parent / continues-to-from links. OTel emits `compensation_attempt` spans; child and continue-as-new are recorded as breadcrumbs on the parent `run_attempt` span (children carry their own `run_attempt` spans).

Docs:
- ✅ Six new/updated guides (`sagas`, `child-workflows`, `long-running-workflows`, `patching`, `workflow-versioning`, `determinism-rules`), `MIGRATING_v0_2_to_v0_3.md`, README saga + child examples, and a saga + parent/child demo in `continuum_example_orders`.

Behavior changes (operator-visible):
- ✅ `continuum_runs.state` gains `:stuck_unknown_version`. Dashboards counting distinct states must add it.
- ✅ `continuum_runs` gains four nullable columns; every run now carries a `correlation_id`.
- ✅ `continuum_workflow_versions` accumulates one row per `(workflow, hash, entrypoint)` per boot; cleanup is operator-driven (`mix continuum.gc_versions` lands in v0.4).
- ✅ `patched?/1` returns journaled values (was a `false` stub); code relying on the stub takes the other branch now.
- ✅ Activities with `compensate:` wrap their return in `%Continuum.ActivityRef{}`; activities without it are unchanged.

**v0.4 — "Hardening & ergonomics" — DONE (2026-05-31)**
- ✅ Snapshot payload format is promised stable via the versioned
  `{:continuum_snapshot, 1, snapshot}` envelope and
  `continuum_snapshots.format_version`. Legacy v0.2/v0.3 unversioned payloads
  decode as format 1. The ≥10× benchmark target is formally accepted as missed:
  indexed raw replay reduced the apparent snapshot advantage to 1.3× on the
  10k side-effect bench, which is a better runtime outcome than preserving a
  larger relative speedup.
- ✅ Per-workflow `snapshot_threshold:` on `use Continuum.Workflow`, resolved
  before runtime/app config.
- ✅ Parallel `compensate_all(mode: :parallel)` and the compile-time
  missing-`compensate:` warning heuristic, with `compensate: :none` as the
  explicit opt-out.
- ✅ `mix continuum.gc_versions` and
  `mix continuum.archive_continued_chains --older-than Nd`, both dry-run by
  default and mutating only with `--execute`.
- ✅ Auto-generated version-keyed entrypoint modules (`LogicalFlow.V_<hash>`)
  retire the v0.3 "keep old module + `workflow:` marker" compromise while
  preserving content-addressed dispatch.
- ✅ OTel child/continue correlation is formally documented as breadcrumbs/events
  plus each child's own `continuum.run_attempt` spans; no dedicated
  `child_attempt` span ships in v0.4.
- ✅ The example app smoke test covers approved/rejected saga orders and a
  subscription-style `continue_as_new` workflow with a per-workflow snapshot
  threshold.

**v0.4 explicitly cuts / deferred (carried to v0.5+):**
- Replay-stepping debugger in the Observer.
- Per-workflow `trusted:` AST option unless a real user asks before v0.5.

**v0.5 — "Production at scale" — DONE (2026-06-02)**
- ✅ BEAM cluster distribution: `:pg`-based pid registration, Distributed Erlang forwarding, and lease-expiry work stealing. The lease/fencing token remains authoritative; `:pg` is advisory routing.
- ✅ Namespaces / multi-tenancy as a soft query/list isolation axis inside one Continuum instance.
- ✅ Search attributes + structured query API (`Continuum.query/1,2`, `get_run/2`, `set_attributes/3`).
- `Continuum.Oban` adapter for activity execution — deferred from v0.5 to v0.5.1 so v0.5 could tag with one activity runner and the must-land distribution/query/audit surface. It is now shipped in v0.5.1.
- `Continuum.AshAi` adapter — deferred until an AI-agent lighthouse adopter is engaged.
- ✅ `mix continuum.audit` — loaded workflow versions, stale-patch verdicts, strict CI mode, and unknown-version run reporting.
- ✅ **`:peer`-based multi-node test harness.** Three scenarios, all running two BEAMs against one Postgres:
  1. Two dispatchers, one runnable run → assert exactly one claims it (the other gets `nil` from `FOR UPDATE SKIP LOCKED`); no double-execution.
  2. Node A leases a run and partitions away; node B's recovery picks it up after lease expiry; node A reconnects mid-flight → assert node A's next journal write is rejected (stolen-lease branch) and the heartbeater stops the stale engine cleanly.
  3. Activity worker on node A claims a task, node A dies before completion, node B's recovery requeues the task, node A reboots → assert no double-execution (captured-lease-token CAS catches the stale completion).
  These come bundled with the cluster work because that's when they become load-bearing — protocol correctness across two real BEAMs is the foundation the rest of the milestone (registration, forwarding, work stealing) builds on.
- Replay-stepping debugger in the Observer — formally cut from v0.5; revisit only with a concrete debugger design and UI budget.

**v0.5.1 — Oban activity executor — DONE (2026-06-04)**
- `Continuum.Oban` ships as an optional activity executor for teams that already operate Oban.
- Continuum keeps `continuum_activity_tasks` as the queue of record. Oban jobs carry only stable task identifiers, and the worker claims at perform-time so queue delay does not consume task lease TTL.
- Oban retries are disabled; Continuum remains responsible for retry/backoff, timeout handling, idempotency, compensation, and fencing-token completion CAS.

**v1.0 — "API freeze"**
- Six months of dogfooding against lighthouse adopters.
- Documented upgrade path; performance benchmarks vs Temporal; migration guides from raw Oban chains and from Commanded.
- Long-term support branch.

### Validation strategy

Recruit 3-5 lighthouse adopters before v0.5 freezes the API:
- A Phoenix-shop SaaS doing onboarding/billing flows (validates the GenServer-replacement narrative).
- A fintech/healthcare team (validates durability + audit story).
- An AI-agent company building on AshAi (validates long-running, signal-heavy use case).
- An Elixir consultancy (DockYard / Dashbit / Erlang Solutions) for credibility.
- An OSS reference user (Ash / Bonfire) embedding it visibly.

Private Slack channel + weekly office hours + public RFC repo for breaking changes between 0.x releases. Cut a 0.x every 6-8 weeks. Soft-launch v0.2 on ElixirForum before HN.

### Risks and mitigations

| Risk                                                        | Mitigation                                                                                                                                  |
|-------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| **Determinism bug corrupts production state** (catastrophic) | Property-based tests on the replay engine from day one. `--paranoid` mode re-replays every run on every event and asserts equality. External audit before v1.0. |
| **Bus factor of one**                                       | Recruit a co-maintainer by v0.3. Document engine internals as we build. Cap core module count at ~25.                                       |
| **API churn between 0.x burns adopters**                    | Be loud about `0.x = breaking`. Lighthouse adopters get private upgrade scripts per release. Don't pre-1.0 on HN.                           |
| **Dashbit/José ships a competitor**                         | Reach out *first*. Get a quote in v0.2 launch. They're far more likely to bless than to compete if we ask.                                  |
| **Temporal Cloud ships a real Elixir SDK**                  | Their SDK won't be OTP-native, won't have a free LiveView observer, won't avoid the cluster service. Compete on those three.                |
| **AI-agent tailwind cools**                                 | Library is also valuable for non-AI durable work (payments, onboarding, ETL). Don't market AI-only.                                         |
| **Solo maintainer burns out around v0.4**                   | Hire/recruit help by v0.3, not v0.6. Plan the funding/sponsorship conversation during v0.2 launch.                                          |

---

## Verification (end-to-end testing)

> **SUPERSEDED.** See `AUDIT_FINDINGS_v0_7_1.md` §0 rule 5 and each release's
> "done" definition in §3.

Each milestone gets concrete verification. For v0.1 (all landed):

1. ✅ **Hello-world** in `continuum_example_orders` — exercised by `scripts/smoke_test.exs` and the controller flow:
   ```elixir
   {:ok, run_id} = Continuum.start(MyApp.OrderFlow, %{order_id: "o1", items: [...]})
   :ok = Continuum.signal(run_id, :fraud_review, :approved)
   {:ok, %{state: :completed, result: result}} = Continuum.await(run_id)
   ```
2. ✅ **Crash-resume integration test** (`test/continuum/integration/crash_resume_test.exs`) — workflow `activity → timer → activity`, `Process.exit(engine_pid, :kill)` mid-timer, `Recovery.recover_once` + `Dispatcher.dispatch_once`, asserts new pid, full event sequence, and `{:ok, 42}` final result.
3. ✅ **Property-based replay test** (`test/continuum/replay_property_test.exs`, StreamData) — two properties: pure side-effect histories and mixed activity+side_effect histories. Each: forward run → dump history → replay → assert identical result and event-type sequence.
4. ✅ **Lease fencing test** (`test/continuum/runtime/lease_test.exs` "journal fencing" describe block) — three variants: `append!`, `cancel_run!`, and `complete_activity_task!` all reject stale-token writes after another owner steals the lease. Asserts no partial writes (`Postgres.load == []`).
5. ✅ **AST scan test** (`test/continuum/ast_check_test.exs`) — denylist entries refuse to compile with useful diagnostics, including `Continuum.{start, signal, cancel, await}`.
6. ✅ **Activity retry test** (`test/continuum/runtime/activity_worker_test.exs` "retries failed activities with backoff") — flaky activity raises once then succeeds, asserts retry record and final completion.
7. **Golden history test** — partial: `Continuum.Test.dump_history!/2` and `assert_replays/4` ship; `test/continuum/test_helpers_test.exs` exercises both. Committing actual `*.journal` golden files is deferred to v0.2 when the format is post-1.0-stable.

### Manual smoke test before each 0.x release

In `continuum_example_orders`:
1. `mix ecto.create && mix ecto.migrate && mix continuum.gen.migration && mix ecto.migrate`.
2. `mix phx.server`; submit an order via UI → workflow starts.
3. Watch logs / Telemetry events stream by; verify activity execution.
4. `kill -9` the BEAM mid-workflow; restart; verify the run resumes from the last journaled event.
5. Send a signal via `Continuum.signal/3` from IEx; verify branch taken.
6. Inspect `continuum_events` table; verify event sequence matches the replay model.
