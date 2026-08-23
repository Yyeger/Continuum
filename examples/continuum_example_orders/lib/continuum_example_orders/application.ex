defmodule ContinuumExampleOrders.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    setup_open_telemetry()

    children = [{Phoenix.PubSub, name: ContinuumExampleOrders.PubSub}] ++ infrastructure()

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: ContinuumExampleOrders.Supervisor
    )
  end

  # `mix test` runs in-memory unit tests that need neither Postgres nor a web
  # server, so the test environment supervises neither. Everything else runs the
  # full tree, with the named Continuum instance started after the Repo — the
  # required order for Postgres-backed runtime pollers.
  defp infrastructure do
    if Application.get_env(:continuum_example_orders, :start_infrastructure?, true) do
      [ContinuumExampleOrders.Repo] ++
        Continuum.children(
          name: :continuum_example_orders,
          repo: ContinuumExampleOrders.Repo,
          workflow_modules: [
            ContinuumExampleOrders.OrderFlow,
            ContinuumExampleOrders.BatchOrders,
            ContinuumExampleOrders.SubscriptionFlow
          ]
        ) ++
        [ContinuumExampleOrdersWeb.Endpoint]
    else
      []
    end
  end

  @impl true
  def config_change(changed, _new, removed) do
    ContinuumExampleOrdersWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  defp setup_open_telemetry do
    case Continuum.OpenTelemetry.setup() do
      {:ok, _handler_id} -> :ok
      {:error, :already_exists} -> :ok
      {:error, :opentelemetry_not_loaded} -> :ok
    end
  end
end
