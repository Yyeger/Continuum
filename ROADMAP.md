# Continuum roadmap

Current release: **0.8.1**. The version in `mix.exs` and the published sections
of [CHANGELOG.md](CHANGELOG.md) define release status. Work under Unreleased is
not yet a release. This roadmap records the bounded scope accepted for the
current task; it does not assign new release numbers or feature deadlines.

## Current milestone: correctness fixes

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
for an operation whose replay was unsupported. No release bump, tag, or publish
is authorized by this milestone.

## Feature scope

**No new feature milestone is accepted by this task.** F1–F4 in the review
(atomic application ingress, bounded dynamic fan-out, recurring schedules,
and Observer replay diagnostics) remain proposals and are excluded from this
implementation. So are other deferred features such as payload encryption,
workflow queries, and heartbeat timeout enforcement.

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
