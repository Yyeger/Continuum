defmodule Continuum.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Phoenix.PubSub, name: Continuum.PubSub},
      {Registry, keys: :unique, name: Continuum.Runtime.Registry},
      Continuum.VersionRegistry,
      Continuum.Runtime.Journal.InMemory,
      Continuum.Runtime.Lease.Heartbeater,
      Continuum.Runtime.RunSupervisor,
      Continuum.Runtime.ActivityWorker.Supervisor
    ]

    opts = [strategy: :one_for_one, name: Continuum.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
