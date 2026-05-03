defmodule ContinuumExampleOrders.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      ContinuumExampleOrders.Repo,
      {Phoenix.PubSub, name: ContinuumExampleOrders.PubSub},
      ContinuumExampleOrdersWeb.Endpoint
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ContinuumExampleOrders.Supervisor)
  end

  @impl true
  def config_change(changed, _new, removed) do
    ContinuumExampleOrdersWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
