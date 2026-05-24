defmodule Continuum.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    instance =
      Continuum.Runtime.Instance.new(
        name: Continuum,
        repo: Application.get_env(:continuum, :repo),
        workflow_modules: Application.get_env(:continuum, :workflow_modules)
      )
      |> Continuum.Runtime.Instance.register()

    children =
      [
        {Phoenix.PubSub, name: instance.pubsub},
        {Registry, keys: :unique, name: instance.registry},
        child(Continuum.VersionRegistry, instance),
        Continuum.Runtime.Journal.InMemory,
        child(Continuum.Runtime.RunSupervisor, instance)
      ] ++ postgres_children(instance)

    opts = [strategy: :one_for_one, name: Continuum.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp postgres_children(%{repo: nil}), do: []

  defp postgres_children(instance) do
    [
      child(Continuum.Runtime.Lease.Heartbeater, instance),
      child(Continuum.Runtime.ActivityWorker.Supervisor, instance),
      child(Continuum.Runtime.Recovery, instance),
      child(Continuum.Runtime.Snapshotter, instance),
      child(Continuum.Runtime.Dispatcher, instance),
      child(Continuum.Runtime.ActivityWorker.Dispatcher, instance),
      child(Continuum.Runtime.TimerWheel, instance),
      child(Continuum.Runtime.SignalRouter, instance)
    ]
  end

  defp child(module, instance) do
    Supervisor.child_spec({module, instance: instance}, id: {module, instance.name})
  end
end
