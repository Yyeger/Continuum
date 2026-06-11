# Workflow Versioning

Continuum stores a content hash for every workflow run. On resume, the engine
dispatches through the run row's `(workflow, version_hash)` pair instead of
blindly calling the latest module with that name.

## Generated Entrypoints

`use Continuum.Workflow` generates a hash-keyed entrypoint module for the
workflow body that was compiled:

```elixir
defmodule MyApp.OrderFlow do
  use Continuum.Workflow, version: 2

  def run(input), do: ...
end
```

Compiling that module also defines a hidden entrypoint named like
`MyApp.OrderFlow.V_<hash>`. Start runs with the public workflow module:

```elixir
{:ok, run_id} = Continuum.start(MyApp.OrderFlow, input)
```

The run row stores the logical workflow and the content hash. Fresh durable runs
delegate to the current generated entrypoint, and resumed durable runs resolve
the journaled `(workflow, version_hash)` back to the generated entrypoint that
matches the old body. A suspended run therefore resumes on old code even after a
new version of the public module is loaded.

`__continuum_entrypoint__/0` returns the generated module for the currently
loaded public module. The generated modules are hidden from ExDoc with
`@moduledoc false`; keep old releases or generated beam files available until
runs that need those hashes have completed or been cancelled.

You can still use `workflow: MyApp.LogicalFlow` when several public modules
should share a logical workflow identity, but ordinary version upgrades no
longer require hand-written `V1`/`V2` wrapper modules.

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
  workflow_modules: [MyApp.OrderFlow]
)
```

If `workflow_modules:` is omitted, Continuum falls back to
`config :continuum, :workflow_modules` and then to loaded modules that expose
`__continuum_workflow__/0`.

## Unknown Versions

If a node claims a Postgres run whose `(workflow, version_hash)` is not loaded
on that node, it emits `[:continuum, :run, :unknown_version]`, releases the
lease, and leaves the run `suspended`. An unknown version is a fact about one
node, not the run: another node in the cluster that has the version loaded can
claim and resume it on its next dispatcher poll (the common case during rolling
deploys).

If no node has the version, the run stays suspended and the telemetry event
fires on each claim attempt. To recover, deploy the missing entrypoint again or
cancel the run if it is no longer needed. Runs marked `stuck_unknown_version`
by older releases are flipped back to `suspended` at boot when a matching
version registers (`Continuum.VersionRegistry.upsert_instance/2`).

## Relationship To `patched?/1`

Versioned entrypoints are for incompatible workflow-code changes. `patched?/1`
is for in-place compatible branches that can safely live together in one
entrypoint until old histories drain.
