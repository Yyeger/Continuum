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

Run-id keyed operations remain global because run ids are UUIDs:

```elixir
Continuum.await(run_id)
Continuum.signal(run_id, :approved, %{})
Continuum.cancel(run_id)
Continuum.get_run(run_id)
```

The host application owns authorization. If a user should only see one
namespace, enforce that in your router pipeline or service layer before calling
Continuum query helpers. For hard isolation, use separate Continuum instances and
repos; namespaces are a softer filter inside one instance.
