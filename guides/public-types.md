# Public Result Types

Continuum returns stable structs for its primary read and lifecycle surfaces:

```elixir
{:ok, %Continuum.Run{} = run} = Continuum.get_run(run_id)
{:ok, %Continuum.Page{entries: runs}} = Continuum.query(per_page: 25)
%Continuum.Readiness{ready?: ready?} = Continuum.readiness()
```

`Continuum.Observer.list_events/2` also returns `%Continuum.Page{}`. Cursors are
opaque and are valid only for the API and ordering that produced them.

This change is source-compatible with ordinary map patterns and dot access:
`%{state: :completed}` still matches a `%Continuum.Run{}`. Code that requires
an exact plain map or enumerates keys should update before 1.0; structs include
the standard `:__struct__` key. Convert explicitly with `Map.from_struct/1`
only at boundaries that truly require a plain map.
