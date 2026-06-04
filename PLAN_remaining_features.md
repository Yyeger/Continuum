# Continuum — Remaining Features Plan (post-v0.5.0)

**Date:** 2026-06-04
**Current state:** `mix.exs` `@version "0.5.1"`. v0.1 → v0.5.1 shipped. `Continuum.Oban` is implemented and covered by tests. Next published milestone is the **v1.0 API freeze**.

This document is the working backlog of what is *still missing feature-wise*, prioritized, with step-by-step implementation notes and the concrete problems each one hits. It is a plan file — per `CLAUDE.md` it is **not** to be committed unless you explicitly ask.

---

## 1. Inventory — everything still missing

Pulled from `ROADMAP.md` "Still deferred after v0.5" + the v1.0 milestone, cross-checked against the actual tree (`grep -rni "oban\|ash_ai" lib` → nothing; no replay-step/debugger in `lib/continuum/observer/`; no per-workflow `trusted:` option in `ast_check.ex`/`workflow.ex`).

| # | Feature | Roadmap slot | Size | Blocker / gate |
|---|---------|--------------|------|----------------|
| **A** | **`Continuum.Oban` adapter** — route activity execution to a host-operated Oban queue instead of the built-in worker pool | **v0.5.1** | **Done** | Shipped in v0.5.1. |
| B | `Continuum.AshAi` adapter — long-running, signal-heavy AI-agent integration | post-v0.5 | L | **Gated on a lighthouse adopter.** Do *not* build speculatively (Working Principle: "Don't add config knobs / surface for hypothetical needs"). |
| C | Replay-stepping debugger in the Observer | cut from v0.5 | M–L | Gated on "a concrete debugger design and UI budget." Needs a design doc first. |
| D | Per-workflow `trusted:` AST option on `use Continuum.Workflow` | deferred | S | **Gated on "a real user asks."** Cheap when wanted; do not pre-build. |
| E | **v1.0 freeze prerequisites** — dogfooding, Temporal benchmarks, migration guides (from Oban chains, from Commanded), LTS branch, external determinism audit | v1.0 | XL | Mostly process/validation, not code. Needs lighthouse adopters (3–5). |

**Status:** (A) the Oban adapter has shipped in v0.5.1. Everything else is either gated on an external trigger (B, D), needs a design doc before code (C), or is validation work (E).

The rest of this plan is: **§2** a deep step-by-step + problems list for (A); **§3** lighter sketches and entry-criteria for (B)–(E); **§4** cross-cutting risks.

---

## 2. DONE — `Continuum.Oban` adapter (v0.5.1)

### 2.1 Goal & non-goals

**Goal:** let a team that already runs Oban execute Continuum *activities* on an Oban queue instead of Continuum's built-in `ActivityWorker` pool — without weakening the lease/fencing determinism guarantees. Opt-in, per-instance.

**Non-goals (state them in the guide so scope doesn't creep):**
- Oban does **not** run *workflow engines* — only activities. The `Engine` replay loop, lease, dispatcher, and recovery stay exactly as they are.
- No change to the journal, the event model, or replay. An activity executed via Oban must produce the *identical* `activity_completed` / `activity_failed` event as the built-in worker, or replay drifts.
- Not a hard dependency. `Continuum` must still `mix compile --warnings-as-errors` and run with `:oban` absent (mirror how `Observer`/`OpenTelemetry` compile without Phoenix/OTel).

### 2.2 The integration point (what the code actually looks like today)

Read these before touching anything:
- `lib/continuum/runtime/activity_worker/dispatcher.ex` — polls `continuum_activity_tasks` with `FOR UPDATE SKIP LOCKED`, **snapshots `r.lease_token` into the claimed task** (`run_lease_token`), starts a `Worker` per task.
- `lib/continuum/runtime/activity_worker/worker.ex` — runs the MFA under a `spawn_monitor` + timeout, then commits via `Journal.Postgres.complete_activity_task!/5` / `fail_activity_task!/4` / `retry_activity_task!/5` / the `complete_compensation_task!` + `fail_compensation_task!` compensation variants, then `Engine.wake/2`. Also does idempotency-side-table reuse (`get_activity_result/3`).
- `lib/continuum/runtime/instance.ex` — the `%Instance{}` (repo, registry, supervisors, dispatcher names) that every runtime path threads through.

**Key invariant to preserve (from `CLAUDE.md` watch-outs):** *the worker carries the run's lease token captured at task-claim time and CAS's on it when committing.* Looking up the run's *current* token at completion defeats fencing. Whatever Oban does, the completion write must CAS on a token captured **before** the MFA ran, against `continuum_runs`.

### 2.3 The core design decision — where does Oban sit?

This is the load-bearing choice and the first thing to lock down. Two options:

**Option 1 — Oban as executor, `continuum_activity_tasks` stays the queue of record.**
The engine still journals `activity_scheduled` + inserts the task row exactly as today. Continuum still owns retry, recovery, idempotency, and completion CAS. Oban is only an execution pool.

There are two possible shapes inside Option 1:

**Option 1a — claim in the Continuum dispatcher, then enqueue Oban.**
The existing `ActivityWorker.Dispatcher` claims tasks with `FOR UPDATE SKIP LOCKED`, snapshots `r.lease_token`, and enqueues an Oban job carrying the claim authority.

- ✅ Smallest code diff from the built-in worker.
- ❌ Bad queue-delay semantics: the task is already leased while it waits in `oban_jobs`. The current completion path rejects expired task leases, so ordinary Oban queue latency can produce stale jobs, recovery requeues, duplicate jobs, or completion failures.
- ❌ Requires lease renewal while a job is not yet executing, which would add a new long-running process or couple tightly to Oban internals.

**Option 1b — enqueue due task IDs, claim inside the Oban worker at perform-time.**
The Continuum activity dispatcher does **not** lease the task when the executor is Oban-backed. It only finds due `available` tasks and inserts Oban jobs carrying stable JSON-safe identifiers. The Oban worker, when it actually starts executing, atomically claims the task row, snapshots the run lease token at that moment, reconstructs the full task map, runs the same shared activity execution code as the built-in worker, and commits through the same `Journal.Postgres.*` functions.

- ✅ Zero schema change. Recovery, partitioning, idempotency, compensation all keep working untouched.
- ✅ Fencing preserved: the worker snapshots the run lease token immediately before running the MFA, then completion CASes against that token and the task lease owner.
- ✅ No leased task waits in `oban_jobs`; task lease TTL measures execution time, not queue latency.
- ⚠️ Two queues (Continuum's task table *and* `oban_jobs`). Slight redundancy; the value Oban adds is its worker pool, backoff, telemetry, and Web UI — not its queue.
- ⚠️ Oban's own retry/`max_attempts` must be **disabled** (set to 1) so it doesn't double-count against Continuum's `retry_activity_task!` attempt accounting. Two retry brains = drift risk.
- ⚠️ Duplicate Oban jobs are possible under races or stale uniqueness windows. This is acceptable only if the perform-time claim is idempotent: one job claims; the rest no-op.

**Option 2 — Oban as the queue of record.**
The engine, when the instance is Oban-backed, skips the `continuum_activity_tasks` insert and inserts an `Oban` job directly. Recovery/orphan-requeue is delegated to Oban (stuck/`available` jobs). The Oban worker reads the run's lease token at job-execution start and CAS's on completion.

- ✅ One queue. Cleaner conceptually; matches "use the Oban you already run."
- ❌ Forks the activity-scheduling path in the engine/journal (violates Working Principle #3 "don't fork per adapter"). The `complete_activity_task!` CAS is currently keyed on a task row; removing the row means rewriting the completion transaction.
- ❌ Recovery story splits: `Recovery` (`lib/continuum/runtime/recovery.ex`) requeues leased activity tasks after `lease_expires_at < now()`. With Oban-as-queue that guard no longer covers activities — you inherit Oban's orphan semantics, which are different. `CLAUDE.md` explicitly warns "never remove that guard for multi-node deployments."
- ❌ Bigger blast radius on the determinism-critical path.

**Recommendation: Option 1b.** Keep `continuum_activity_tasks` as the source of truth, but move the lease claim to Oban perform-time. This keeps the journal/lease/recovery model identical, avoids holding Continuum task leases while work waits in Oban, and isolates Oban to the executor role. The only real cost (a second queue) is acceptable because Oban's value here is the *worker pool*, not the *queue*.

### 2.4 Step-by-step (Option 1b)

1. **Add `:oban` as an optional dep.** `mix.exs`: `{:oban, "~> 2.x", optional: true}`. Verify `mix compile --warnings-as-errors` still passes with Oban *uninstalled* — guard every `Oban.*` reference behind `Code.ensure_loaded?(Oban)` / `Application.get_env`, the same pattern `OpenTelemetry`/`Observer` use. Add a compile-time check that raises a clear error if a user sets `executor: :oban` without `:oban` in their deps.

2. **Thread `activity_executor` through `%Instance{}` and supervision.** Add `:activity_executor` to `Continuum.Runtime.Instance`, defaulting to `:builtin`. Accept `activity_executor: :builtin | {:oban, queue: atom(), ...}` in both:
   - `Continuum.children/1` for named instances.
   - `Continuum.Application` / default instance via app env.
   Validate at boot that `:oban` is loaded when an instance asks for Oban. Keep `ActivityWorker.Dispatcher` running for Oban-backed instances; skip `ActivityWorker.Supervisor` only when the executor is Oban-backed.

3. **Introduce a thin activity-executor seam.** Today `Dispatcher.start_worker/1` is the only execution entry point. Split the dispatcher into two paths:
   - `:builtin` → `DynamicSupervisor.start_child(Worker, task)` (unchanged).
   - `:oban` → enqueue task IDs without claiming them.
   Resist building a full behaviour with many callbacks (Working Principle #4 — "three similar lines beats a premature behaviour"). One dispatch-fork function is enough for two executors.

4. **Add an Oban enqueue scan that does not lease tasks.** For Oban-backed instances, the dispatcher should select due rows from `continuum_activity_tasks` where:
   - `state = 'available'`
   - `available_at <= now()`
   - run is active and currently leased (`r.state IN ('running', 'suspended')`, `r.lease_token IS NOT NULL`, `r.lease_expires_at > now()`)
   It should return `task_id`, `attempt`, and enough metadata for telemetry. It must **not** update `state`, `lease_owner`, or `lease_expires_at`.

5. **`Continuum.Oban.enqueue/1`.** Oban args are JSON, and the task blob is not. The task row's `mfa` column is a `bytea` blob containing the full encoded task map (`mfa`, retry, timeout, idempotency key, compensation metadata). Do not put that blob or `%Instance{}` into Oban args. Carry only stable, re-lookup-able keys:
   - `instance` → the **name** (atom → string), re-resolved via `Instance.lookup/1` in the worker.
   - `task_id` → re-load and claim the row from `continuum_activity_tasks` at perform-time.
   - `attempt` → expected task attempt at enqueue time. The perform-time claim must require this attempt so stale jobs cannot claim a later retry.
   Set the Oban worker's `max_attempts: 1`. Use Oban uniqueness on `{instance, task_id, attempt}` with a bounded period and active states only. Do not rely on uniqueness for correctness; correctness comes from the perform-time SQL claim. Duplicate jobs must no-op cleanly.

6. **Add a perform-time claim function.** Add a public/internal function near the dispatcher or Postgres journal layer, e.g. `ActivityWorker.Dispatcher.claim_one(instance, task_id, expected_attempt, owner, ttl_seconds)`, that atomically:
   - locks/updates the matching activity task from `available` to `leased`;
   - requires `attempt = expected_attempt`;
   - joins the active run row and requires a live run lease;
   - snapshots `r.lease_token` into the returned task map;
   - decodes the `mfa` bytea task blob and merges `id`, `run_id`, `seq`, `attempt`, `lease_owner`, and `run_lease_token`.
   Return `{:ok, task}` when claimed, `:not_available` / `:stale` when another job won or the attempt has moved on, and `{:error, reason}` for real DB failures.

7. **`Continuum.Oban.Worker` (`use Oban.Worker, queue: ..., max_attempts: 1`).** Its `perform/1`:
   - Resolve `Instance.lookup(name)`.
   - Claim the task by `{task_id, expected_attempt}` at perform-time.
   - If claim returns `:not_available` or `:stale`, return `:ok` without side effects.
   - Reuse the existing logic — ideally call into the *same* functions `Worker` uses. Refactor `ActivityWorker.Worker`'s `handle_continue(:run, task)` body (idempotency check → run MFA → complete/fail/retry → `Engine.wake`) into a plain module function `Continuum.Runtime.ActivityWorker.execute(task)` so **both** the GenServer worker and the Oban worker call it. This is the cleanest way to honor Working Principle #3 (one execution loop, two triggers).
   - **Problem: timeout.** The built-in worker enforces `timeout_ms` via `spawn_monitor` + `Process.exit(pid, :kill)`. Decide: keep that exact mechanism inside `execute/1` (preferred — identical semantics), or map to Oban's job timeout (`@impl Oban.Worker; def timeout(_), do: timeout_ms`). Keeping the spawn_monitor is safer because it produces the *same* `{:error, :timeout}` → `fail_or_retry` path and therefore the same journaled event. Mixing in Oban's timeout would produce an Oban-level discard with no `activity_failed` event = drift.
   - **Problem: retries.** Continuum owns retry via `retry_activity_task!` (sets `available_at = now + backoff`, bumps `attempt`, leaves task `available`). After a retry, the dispatcher enqueues a fresh Oban job for the next attempt. Oban must NOT retry (`max_attempts: 1`); an MFA failure handled by Continuum returns `:ok` from `perform/1`, never `{:error, _}`. If `execute/1` raises because the completion CAS failed, decide deliberately whether the Oban worker returns `:ok` and logs or lets the job fail for operator visibility. Do not allow Oban to re-run the MFA automatically.

8. **Compensation tasks.** They flow through the same task table with `kind: :compensation` and commit via `complete_compensation_task!`/`fail_compensation_task!`. Because step 7 reuses the shared `execute/1`, compensation works for free — but add an explicit test (a saga whose compensation runs on Oban).

9. **Idempotency.** `execute/1` already consults `Journal.Postgres.get_activity_result/3` before running. Free under the refactor — but test it (an idempotent activity that hits on a second run, executed via Oban, must emit `[:continuum, :activity, :idempotency_hit]` and not re-run the side effect).

10. **Do not disable the dispatcher.** In Option 1b the dispatcher still exists, but its Oban path is "enqueue due task IDs" rather than "claim and start a GenServer worker." Disable only the built-in `ActivityWorker.Supervisor` for Oban-backed instances. Make sure this is done for both `Continuum.children/1` and the default instance in `Continuum.Application`.

11. **Config surface.** Per-instance, threaded through `%Instance{}`. Something like `Continuum.children(name: MyApp.Flows, repo: MyApp.Repo, activity_executor: {:oban, queue: :continuum_activities})`. For the default instance, use an explicit app env key such as `config :continuum, activity_executor: {:oban, queue: :continuum_activities}` because the default instance is built by `Continuum.Application`, not `Continuum.children/1`. Avoid a second global fallback for named instances.

12. **Telemetry.** The shared `execute/1` already emits `[:continuum, :activity, :started|completed|failed|retried]`. Add `meta.executor` (`:builtin | :oban`) so operators can tell where an activity ran. Consider also adding `meta.oban_job_id` in the Oban path. No new event names.

13. **Tests** (`test/continuum/runtime/` + a new `test/continuum/oban/`):
    - Activity completes via Oban → identical event sequence to the built-in path (golden compare).
    - Failure + retry: assert the task returns to `available`, gets re-enqueued with `attempt + 1`, and Oban does **not** itself retry (assert one Oban execution per Continuum attempt).
    - Timeout produces `activity_failed` (not an Oban discard).
    - **Queue-delay safety:** enqueue an Oban job, let it sit longer than the task TTL would have been, then perform it; because claim happens at perform-time, it should still execute normally if the run lease is live.
    - **Duplicate/stale job safety:** enqueue two jobs for the same `{task_id, attempt}`; assert one claims and the other no-ops. Then retry to `attempt + 1`; assert an old attempt job cannot claim the newer attempt.
    - **Fencing across executors:** steal the run lease mid-execution, assert the Oban worker's completion CAS is rejected and no terminal activity event lands. The history should still contain `activity_scheduled`; do not assert an empty history.
    - Idempotency hit via Oban.
    - Compensation via Oban.
    - Compile + run with `:oban` absent (executor defaults to `:builtin`, suite unaffected).
    - Run the six-seed sweep (`for seed in 0 1 42 100 1000 99999; do mix test --seed $seed; done`) — the CLAUDE.md mandate after non-trivial runtime changes.

14. **Docs:** new `guides/oban-executor.md` (when to use it, the `max_attempts: 1` requirement, recovery semantics, the "Oban runs activities not workflows" boundary, and why task claim happens at perform-time). Update `CHANGELOG.md` `## Unreleased`, README "activity execution" section, and add a `MIGRATING_v0_5_to_v0_5_1.md` if anything is operator-visible (the new `activity_executor:` option; `meta.executor` on telemetry).

### 2.5 Concrete problems / risks for (A), summarized

1. **Two retry brains.** Oban's `max_attempts` vs Continuum's `retry_activity_task!`. Mitigation: Oban `max_attempts: 1`, `perform/1` always returns `:ok`, Continuum owns all retry/backoff. *This is the single highest-drift-risk item.*
2. **Leased task waiting in Oban.** Avoid this entirely: enqueue IDs first, claim at perform-time. Task lease TTL must measure execution time, not queue latency.
3. **Non-JSON job args.** MFA terms + `%Instance{}` can't ride in Oban args. Mitigation: carry `{instance_name, task_id, attempt}` only; claim, re-load, and decode the task in the worker.
4. **Duplicate/stale Oban jobs.** Oban uniqueness is best-effort operational hygiene, not correctness. Mitigation: claim by `{task_id, expected_attempt}`; stale and duplicate jobs no-op.
5. **Fencing must survive the Oban hop.** Token captured at perform-time claim, CAS at completion — never read after the MFA ran.
6. **Timeout semantics.** Keep the `spawn_monitor` + kill inside the shared `execute/1` so timeouts journal `activity_failed`; don't delegate to Oban's job timeout (would discard with no event = drift).
7. **Forking the execution loop.** Refactor `ActivityWorker.Worker`'s body into a shared `execute/1` so built-in and Oban share one loop (Working Principle #3). Do *not* copy-paste the complete/fail/retry/idempotency logic into the Oban worker.
8. **Default vs named supervision.** The default instance is built by `Continuum.Application`; named instances are built by `Continuum.children/1`. The executor config and "skip built-in worker supervisor" logic must cover both.
9. **Optional-dep compilation.** Must compile + test clean with `:oban` absent (the `Observer`/`OpenTelemetry` pattern is the template).
10. **Recovery boundary.** In Option 1b recovery is unchanged (good). If anyone pushes for Option 2, the activity-orphan recovery guard (`lease_expires_at < now()`) is at stake — flag it.

---

## 3. LATER — gated / lighter sketches

### B. `Continuum.AshAi` adapter — **gated on a lighthouse adopter**
Do not build speculatively. Entry criterion (from ROADMAP validation strategy): an AI-agent company building on AshAi is engaged. When that happens: model an AI agent loop as a long-running workflow (signals = tool results, `continue_as_new` for context-window rollover, `side_effect`/activities for LLM calls). The long-running + signal-heavy primitives already exist (v0.3); the adapter is mostly ergonomics + a guide. **Action now: none. Note it as adopter-gated.**

### C. Replay-stepping debugger in the Observer — **needs a design doc first**
Formally cut from v0.5; revisit "only with a concrete debugger design and UI budget." It would let an operator step the replay loop event-by-event in the Observer LiveView and inspect cursor/command_id at each step. The replay machinery (`Effect.run/2`, `Context` cursor, `command_id`) already produces everything needed; the work is a read-only re-replay harness + a LiveView surface. **Action now: write a 1-page design (UI sketch + which replay hooks it taps) before any code.** Don't start implementation under this plan.

### D. Per-workflow `trusted:` AST option — **gated on a real user asking**
Currently trust is global: `config :continuum, trusted_modules: [...]` + `use Continuum.Pure`. A per-workflow `use Continuum.Workflow, trusted: [Decimal]` is a small change in `workflow.ex` + `ast_check.ex`. The roadmap is explicit: **don't pre-build; wait for a real request.** Cheap when wanted. **Action now: none.**

### E. v1.0 — "API freeze" prerequisites — **mostly validation, not code**
Not a coding task you start today, but the things that gate it:
- **3–5 lighthouse adopters** dogfooding (Phoenix SaaS, fintech/healthcare, AI-agent, consultancy, OSS reference) — recruiting, not code.
- **Benchmarks vs Temporal** — needs a harness + writeup.
- **Migration guides** from raw Oban chains and from Commanded — docs.
- **External determinism audit** before freeze (named in Risks table).
- **LTS branch** + documented upgrade path.
- API surface review: the public macro surface (`use Continuum.Workflow`, `activity`, `await`, etc.) is frozen at 1.0 — last chance for breaking changes. Audit every `@doc`'d public function/macro and the `@since` tags.

**Action now: none code-wise.** Track as the release gate; the Oban adapter (A) is itself one of the validation pieces (consolidating queues is a common adopter ask).

---

## 4. Cross-cutting risks (apply to everything below)

- **Determinism is the load-bearing claim.** Every change asks "could this let replay diverge?" The Oban adapter touches the activity-completion write — the highest-stakes spot. Bias to loud `ReplayDriftError` over silent corruption.
- **Never fork the replay/execution loop per adapter** (Working Principle #3). The Oban adapter must share `execute/1`; an AshAi adapter must not introduce a second engine.
- **Runtime moat** (Working Principle #4): new runtime processes need a written reason. The Oban adapter should add *zero* new long-running Continuum processes (Oban supplies the pool); it reuses the existing dispatcher for claiming.
- **No speculative config knobs.** B and D are explicitly request-gated. Honor that.
- **Compile clean + six-seed sweep** after any runtime change.
- **Never commit without explicit permission** (`CLAUDE.md` hard rule; the 2026-05-24 incident). This plan file itself stays uncommitted unless you say so.

---

## 5. Execution status

1. **Option 1b** is implemented: `continuum_activity_tasks` remains the queue of record; Oban jobs carry task IDs; claim happens at perform-time.
2. **`ActivityWorker.Worker` body → shared `Continuum.Runtime.ActivityWorker.execute/1`** is implemented.
3. **`activity_executor` is threaded through `Instance`, `Continuum.children/1`, and `Continuum.Application`.**
4. **The Oban enqueue scan + perform-time claim** are implemented and test duplicate/stale claim behavior.
5. **`Continuum.Oban.Worker`** is implemented on top of shared `execute/1`.
6. **Tests + six-seed sweep + docs** are complete.
7. **v0.5.1 release metadata** is prepared. The local `v0.5.1` tag already exists and points at older work; do not move it without explicit user approval.
8. B/C/D/E remain parked behind their gates; revisit when their triggers fire.
