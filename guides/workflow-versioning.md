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

## Pure Helper Versions

Static calls to modules using `Continuum.Pure` are pinned to generated helper
entrypoints before the workflow is hashed. Pure helpers also pin their own
static Pure dependencies. Changing a helper therefore changes the caller's
workflow hash, including through transitive dependencies, and an old generated
workflow continues calling its original helper implementation.

Keep the generated helper BEAMs alongside the generated workflow BEAMs needed
by in-flight runs. Helper metadata is a compile-time dependency, so changing
a helper causes its callers to be recompiled by Mix. Define same-file helpers
before their callers, or put them in separate files. An unresolved static
helper is a compile error; it is never silently omitted from version identity.
Dynamic calls and unmarked/allowlisted helpers remain outside Pure version
pinning and retain their existing determinism diagnostics.

### Upgrading from 0.8.1

Workflows with no Pure calls retain their existing hashes. Workflows that call
Pure helpers acquire new hashes on recompilation. Old hashes are not aliased
to the new implementation: doing so would silently reinterpret old history.

Pre-fix workflow entrypoints do not pin helpers. Drain their runs on the old
release before upgrading, or keep workers running the complete old release
(including its original helper modules) until those runs finish. Retaining
only an old workflow BEAM alongside changed public helper modules is not
sufficient for those legacy runs. New pinned versions can coexist when all
their generated workflow and helper BEAMs are retained.

## Durable Registry

Each Continuum instance supervises a registrar that upserts loaded workflow
versions into `continuum_workflow_versions` on boot. Transient database failures
are retried with bounded exponential backoff. The first time an engine resolves
a workflow that was not in the boot scan, it also waits for that version to be
registered durably before executing it. The hot path uses an in-memory registry
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

`mix continuum.audit --repo MyApp.Repo` reports the registrar state and lists
any loaded workflow hashes missing from the durable table.

Before deploying, check the inverse requirement: every version referenced by a
live run must exist in the release being deployed.

```console
mix continuum.versions.check --repo MyApp.Repo --strict
```

The task joins distinct non-terminal run versions with the entrypoints loaded
in the current BEAM. It exits non-zero in strict mode if a version is absent;
use `--format json` when a deployment controller needs structured output.

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
