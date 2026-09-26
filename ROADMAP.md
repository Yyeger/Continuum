# Continuum roadmap

Current release: **0.8.5**. The version in `mix.exs` and the published sections
of [CHANGELOG.md](CHANGELOG.md) define release status. Work under Unreleased is
not yet a release. This roadmap records the bounded scope accepted for the
current task; it does not assign new release numbers or feature deadlines.

## Completed correctness milestone

The user-visible problem is that invalid workflow outcomes, orphan activity
bodies, replay inconsistencies, stale helper code, and slow lease renewal can
undermine durable execution. The accepted scope is **C1–C5 and K1–K2**, listed
below. Each fix has its own commit.

| ID | Required behavior | State |
|---|---|---|
| C1 | Invalid results and errors persist as durable terminal failures; database outages remain recoverable | Implemented; targeted tests passed |
| C2 | Activity bodies stop when their worker dies | Implemented; targeted tests passed |
| C3 | Rejected snapshots cannot leave replay using an incomplete history | Implemented; targeted tests passed |
| C4 | Offline replay detects swallowed suspension and invalid returns | Implemented; targeted tests passed |
| C5 | Unsupported manual retries of batch activities fail before any mutation | Implemented; targeted tests passed |
| K1 | Static Pure helpers participate in identity and retain historical implementations | Implemented; targeted tests passed |
| K2 | Lease renewal batches preserve fencing, cancellation, and transient-failure handling | Implemented; targeted tests passed |

Acceptance requires focused regression coverage, the regular suite across the
six seeds listed in CLAUDE.md, separate cluster tests, clean compilation, formatting,
and strict lint. Documentation must distinguish committed external results
from possibly repeated activity attempts, and describe helper upgrade behavior.
Verification passed: 658 checks per seed, all 9 cluster tests, compilation,
formatting, strict lint, and generated documentation links.

Compatibility: K1 changes hashes of workflows with Pure dependencies. Preserve
the complete old release for legacy runs or drain them before upgrading; see
[workflow versioning](guides/workflow-versioning.md). C5 adds an explicit error
for an operation whose replay was unsupported. These fixes are included in
v0.8.5 together with the feature milestone below.

## Completed feature milestone

F1–F4 were authorized, implemented, tested, and committed as separate steps.
All changes below are included in **v0.8.5**.

| ID | Implemented behavior | Commit |
|---|---|---|
| F1 | `Continuum.Multi.enqueue/5` atomically inserts work and ingress keys with business writes; dispatch follows commit and survives producer exit | `dc66c0f` |
| F2 | `activity_map/3` records input fingerprints, schedules bounded windows, preserves result order, and supports crash recovery and snapshots | `6ebe1a2` |
| F3 | UTC recurring definitions with distinct occurrences, explicit overlap/missed policies, bounded catch-up, and list/pause/resume/inspection APIs | `090c959` |
| F4 | Asynchronous read-only Observer replay reports with drift details, version resolution, snapshot use, result agreement, redaction, host authorization hooks, and work limits | `a808bc7` |

An integration follow-up (`a789e3c`) fixes PostgreSQL encoding of `nil` workflow
inputs, including Multi ingress and recurring occurrence starts.

Acceptance verification:

- The regular suite passed **688 checks (11 properties and 677 tests)** on each
  seed **0, 1, 42, 100, 1000, 99999**. All **9** cluster tests passed separately.
- Compilation with warnings as errors, formatting, strict Credo, and generated
  documentation links passed. ExDoc still reports the two existing hidden
  references in configuration and activity-context documentation.
- Generated fresh migrations passed up/down; the generated `--from 0.8.1`
  upgrade passed up/down/up over the historical schema in an isolated database.
- Four independent database connections generated one occurrence and advanced
  its definition cursor once. Multi ingress was also checked across independent
  connections for pre-commit invisibility and recovery after producer exit.

Compatibility and scope:

- Existing installations must run
  `mix continuum.gen.migration --from 0.8.1 --repo MyApp.Repo` and
  `mix ecto.migrate` before starting the updated runtime. Fresh installs use the
  current generator. Version preflight and GC account for pending occurrences
  and paused recurring definitions.
- Dynamic maps wait for a complete window before scheduling the next. Manual
  batch retries and batch compensation remain unsupported.
- Recurrence is based on UTC intervals; calendar/cron and timezone rules remain
  excluded. Missed `:skip` coalesces overdue times into the latest due slot;
  overlap `:skip` records an occurrence without starting another run.
- Observer replay is a bounded report, not an interactive debugger. Host apps
  must supply their existing authorization hooks. Larger offline investigations
  can use the replay CLI.
- Payload encryption, workflow queries, heartbeat timeout enforcement, and
  other deferred candidates remain excluded from v0.8.5.

A future milestone should be selected using adopter feedback and name its user
problem, compatibility cost, observable acceptance checks, and exclusions.
Keep at most one substantial replay-semantics change in a feature milestone.
Adopter evaluation can start before API freeze; it does not require waiting for
all candidate features or committing to the older roadmap's dates.

## Sources and history

- [Agent guidance](CLAUDE.md): setup, verification, and runtime invariants.
- [Supported guides](guides/): current APIs and operational behavior.
- [Changelog](CHANGELOG.md): released history and Unreleased changes.
- [Original architecture and roadmap](dev/architecture-history.md): historical
  rationale and obsolete API/schema sketches, clearly separated from current
  guarantees. Its old unknown-version terminal state is not current behavior.

The earlier local audit register is historical input, not a competing current
plan. Its previously fixed entries must not be copied forward as open defects
without fresh verification. The completed v0.8 scope cap is likewise history.
