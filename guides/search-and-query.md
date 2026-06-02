# Search and Query

Continuum stores operator-facing metadata on `continuum_runs.attributes`, a JSONB
map set when a run starts or updated later by surrounding application code.

```elixir
{:ok, run_id} =
  Continuum.start(MyApp.OrderFlow, %{order_id: "ord_123"},
    attributes: %{region: "eu", order_type: "subscription"}
  )

:ok = Continuum.set_attributes(run_id, %{customer_tier: 4})
```

Attributes are not journaled workflow history. They are metadata for dashboards,
operators, and application read paths, and workflow code cannot read them during
replay.

## Structured Queries

Use `Continuum.query/1` with a closed `:where` spec:

```elixir
{:ok, page} =
  Continuum.query(
    where: [
      {:eq, :state, :suspended},
      {:eq, [:attributes, "region"], "eu"},
      {:gte, :started_at, ~U[2026-06-01 00:00:00Z]}
    ],
    order_by: {:desc, :started_at},
    page: 1,
    per_page: 50
  )
```

Supported run fields are `:id`, `:run_id`, `:state`, `:workflow`, `:started_at`,
and `:completed_at`. Supported operators are `:eq`, `:neq`, `:lt`, `:lte`, `:gt`,
`:gte`, and `:in` for run fields. Attribute filters support `:eq` and `:neq` on
paths like `[:attributes, "region"]`.

`per_page` is capped at 100. `Continuum.get_run/2` loads a single run by id.

## Indexing

The v0.5 migration adds a GIN index on `continuum_runs.attributes`. Attribute
updates merge JSON-encodable map data into the existing metadata and do not append
events.
