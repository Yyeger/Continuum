# Changelog

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
