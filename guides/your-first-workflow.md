# Your First Workflow

Continuum workflows are ordinary Elixir modules that use
`Continuum.Workflow`. The workflow function is pure orchestration code:
it may call activities, wait for signals, set timers, and use Continuum's
deterministic primitives.

```elixir
defmodule MyApp.OrderFlow do
  use Continuum.Workflow, version: 1

  def run(%{order_id: order_id, items: items}) do
    {:ok, validated} = activity MyApp.Activities.ValidateOrder.run(%{items: items})
    {:ok, charge} = activity MyApp.Activities.ChargeCard.run(%{order_id: order_id})

    case await signal(:fraud_review) do
      :approved -> activity MyApp.Activities.ShipOrder.run(%{order_id: order_id})
      :rejected -> {:error, {:rejected, charge}}
    end

    {:ok, validated}
  end
end
```

Start the run from application code:

```elixir
{:ok, run_id} =
  Continuum.start(MyApp.OrderFlow, %{
    order_id: "order_123",
    items: [%{sku: "sku_1", qty: 1}]
  })
```

Send a signal when outside input arrives:

```elixir
:ok = Continuum.signal(run_id, :fraud_review, :approved)
```

Wait for completion in tests or synchronous scripts:

```elixir
{:ok, %{state: :completed, result: result}} = Continuum.await(run_id, 5_000)
```

For Postgres-backed execution, configure a repo and generate the schema:

```elixir
config :continuum,
  repo: MyApp.Repo,
  journal: Continuum.Runtime.Journal.Postgres
```

```bash
mix continuum.gen.migration --repo MyApp.Repo
mix ecto.migrate
```

The engine persists every effect in `continuum_events`. If the BEAM process
dies, the dispatcher re-leases suspended work and replays the history through
the same workflow module.
