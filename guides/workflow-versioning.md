# Workflow Versioning

Continuum stores a content hash for every workflow run. On resume, the engine
dispatches through the run row's `(workflow, version_hash)` pair instead of
blindly calling the latest module with that name.

## The v0.3 Entrypoint Pattern

v0.3 uses an explicit entrypoint pattern. Keep old concrete workflow modules
loaded while runs that started on them are still active, and point newer
concrete modules at the same logical workflow:

```elixir
defmodule MyApp.OrderFlow.V1 do
  use Continuum.Workflow, version: 1, workflow: MyApp.OrderFlow

  def run(input), do: ...
end

defmodule MyApp.OrderFlow.V2 do
  use Continuum.Workflow, version: 2, workflow: MyApp.OrderFlow

  def run(input), do: ...
end
```

Start new runs with the concrete entrypoint you want:

```elixir
{:ok, run_id} = Continuum.start(MyApp.OrderFlow.V2, input)
```

Both versions register as the logical workflow `MyApp.OrderFlow`, each with its
own hash-specific entrypoint. A suspended V1 run resumes on V1 even after V2 is
loaded.

## Durable Registry

Each Continuum instance upserts loaded workflow versions into
`continuum_workflow_versions` on boot. The hot path uses an in-memory registry
backed by `:persistent_term`; the table gives operators a durable view of known
workflow hashes.

Configure boot-time registration explicitly when your app can:

```elixir
Continuum.children(
  name: :orders_continuum,
  repo: MyApp.Repo,
  workflow_modules: [MyApp.OrderFlow.V1, MyApp.OrderFlow.V2]
)
```

If `workflow_modules:` is omitted, Continuum falls back to
`config :continuum, :workflow_modules` and then to loaded modules that expose
`__continuum_workflow__/0`.

## Unknown Versions

If a Postgres run references a `(workflow, version_hash)` that is no longer
loaded, Continuum marks it `:stuck_unknown_version` and emits
`[:continuum, :run, :unknown_version]`. The dispatcher excludes that state so
the run does not enter a reclaim loop.

To recover, deploy the missing entrypoint again or cancel the stuck run if it is
no longer needed.

## Relationship To `patched?/1`

Versioned entrypoints are for incompatible workflow-code changes. `patched?/1`
is for in-place compatible branches that can safely live together in one
entrypoint until old histories drain.
