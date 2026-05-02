defmodule Continuum.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      repo_children() ++
        [
          {Phoenix.PubSub, name: Continuum.PubSub},
          {Registry, keys: :unique, name: Continuum.Runtime.Registry},
          Continuum.VersionRegistry,
          Continuum.Runtime.Journal.InMemory,
          Continuum.Runtime.Lease.Heartbeater,
          Continuum.Runtime.RunSupervisor,
          Continuum.Runtime.Dispatcher,
          Continuum.Runtime.SignalRouter
        ]

    opts = [strategy: :one_for_one, name: Continuum.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp repo_children do
    if Code.ensure_loaded?(Continuum.Test.Repo) and
         Application.get_env(:continuum, Continuum.Test.Repo) != nil do
      [Continuum.Test.Repo]
    else
      []
    end
  end
end
