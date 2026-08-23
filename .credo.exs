%{
  configs: [
    %{
      name: "default",
      plugins: [{ExSlop, []}],
      checks: %{
        extra: [
          # The replay engine is intentionally branch-heavy. Keep its current
          # measured ceilings as regression gates while larger extractions stay
          # release-planned work rather than lint-driven rewrites.
          {Credo.Check.Refactor.Nesting, [max_nesting: 4]},
          {Credo.Check.Refactor.CyclomaticComplexity, [max_complexity: 17]}
        ],
        disabled: [
          # Continuum uses fully qualified names at adapter, optional-library,
          # and generated-workflow boundaries. Rewriting those sites en masse
          # harms provenance and can perturb replay-sensitive source.
          {Credo.Check.Design.AliasUsage, []},

          # These calls deliberately dispatch through runtime-selected journal
          # adapters and optional integrations; tests also exercise `apply/3`
          # as workflow input for the determinism checker.
          {Credo.Check.Refactor.Apply, []}
        ]
      }
    }
  ]
}
