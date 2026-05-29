defmodule ContinuumExampleOrders.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    setup_open_telemetry()

    children =
      [
        ContinuumExampleOrders.Repo,
        {Phoenix.PubSub, name: ContinuumExampleOrders.PubSub}
      ] ++
        Continuum.children(
          name: :continuum_example_orders,
          repo: ContinuumExampleOrders.Repo,
          workflow_modules: [
            ContinuumExampleOrders.OrderFlow,
            ContinuumExampleOrders.BatchOrders
          ]
        ) ++
        [
          ContinuumExampleOrdersWeb.Endpoint
        ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: ContinuumExampleOrders.Supervisor
    )
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
