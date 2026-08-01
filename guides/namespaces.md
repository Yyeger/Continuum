# Namespaces

Namespaces are soft tenancy inside one Continuum instance and one repo. They are
row metadata on `continuum_runs`, not a separate process tree.

```elixir
{:ok, run_id} =
  Continuum.start(MyApp.OrderFlow, input,
    namespace: "tenant-a",
    attributes: %{region: "eu"}
  )
```

`Continuum.query/1` and `Continuum.Observer.list_runs/1` default to the
`"default"` namespace. Pass `namespace: "tenant-a"` to list or search another
namespace.

Run-id keyed operations remain global when `:namespace` is omitted because run
ids are UUIDs. Service layers can add an expected-value guard to every access:

```elixir
Continuum.get_run(run_id, namespace: "tenant-a")
Continuum.await(run_id, 5_000, namespace: "tenant-a")
Continuum.signal(run_id, :approved, %{}, namespace: "tenant-a")
Continuum.cancel(run_id, namespace: "tenant-a")
Continuum.set_attributes(run_id, %{region: "eu"}, namespace: "tenant-a")
```

Every guarded operation returns `{:error, :not_found}` when the run does not
exist or its namespace differs, without revealing which case occurred and
without mutating the run. Signal, cancel, and await retain their existing
behavior of following `continue_as_new` to the live tip.

The host application still owns authentication and authorization; the guard
prevents an already-authorized service path from accidentally crossing tenant
boundaries. For hard isolation, use separate Continuum instances and repos;
namespaces are a softer filter inside one instance.
