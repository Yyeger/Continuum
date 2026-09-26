# CLAUDE.md

Orientation for agents working on Continuum. Current release: **0.8.1**.
The correctness fixes on this branch are **Unreleased**; see [CHANGELOG.md](CHANGELOG.md).

[ROADMAP.md](ROADMAP.md) is the current scope record. Setup and verification
commands are below, and the [guides](guides/) document supported behavior.
Earlier release narratives live in the changelog; historical architecture is in
[dev/architecture-history.md](dev/architecture-history.md).

## Git authorization

Read-only git inspection is always fine. Do not commit, push, reset, discard,
stash, rebase, merge, or delete branches without explicit user authorization.
Honor the scope of that authorization: “commit after each step” authorizes
those commits for the task, but does not authorize a push or destructive cleanup.
A plan file, changelog entry, or general “looks good” is not authorization.

## Working rules

- Keep workflow effects behind `Continuum.Runtime.Effect`; adapters share replay
  semantics. Live execution and offline replay both use `Context.validate_return!/1`.
- Invalid workflow results become durable terminal failures; journal outages
  remain recoverable. Keep terminal journal writes outside the workflow catch-all.
- Activity execution belongs to its worker. Killing the worker must terminate
  its body, including bodies that trap exits.
- Use the instance's repo and journal and preserve captured fencing tokens.
- Pure helper versions are pinned transitively in generated workflow code.
  Preserve historical helper BEAMs and follow the migration guidance in
  [workflow versioning](guides/workflow-versioning.md).
- Before changing effects or event shapes, check live and offline replay,
  snapshots, cancellation, manual retry, and in-memory parity. Do not infer
  support for manual batch retry from automatic retry.
- New runtime processes need a written reason. Avoid new configuration or public
  API changes without a concrete need and explicit task scope.
- Public functions and macros need documentation. Keep history compatibility
  deliberate; do not regenerate golden histories merely to hide a mismatch.
- Run relevant tests after changes. For substantial runtime changes, use seeds
  0, 1, 42, 100, 1000, and 99999 and run cluster tests separately.

## Current scope

C1–C5 and K1–K2 from the review are correctness fixes. Feature proposals F1–F4
are excluded. The old v0.8 scope cap is a completed historical decision, not
an instruction to implement its deferred items now. No next feature milestone
has been approved by this task.

## Setup and verification

Local PostgreSQL uses **localhost:5433**, as configured by `docker-compose.yml`
and `config/test.exs`. Start it with `docker compose up -d` or
`podman-compose up -d` (start your Podman machine first if required).

```sh
mix deps.get
mix compile --warnings-as-errors
mix format --check-formatted
mix credo --strict
mix test
mix test.cluster
mix docs.check
```

`mix test` creates and migrates the test database. Run cluster tests separately
from the regular suite because real peer nodes share PostgreSQL. Record which
checks actually ran and their results; do not regenerate golden histories to
hide a failure.
