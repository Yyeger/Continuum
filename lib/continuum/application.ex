defmodule Continuum.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    instance =
      Continuum.Runtime.Instance.new(
        name: Continuum,
        repo: Application.get_env(:continuum, :repo),
        activity_executor: Application.get_env(:continuum, :activity_executor, :builtin),
        workflow_modules: Application.get_env(:continuum, :workflow_modules)
      )
      |> Continuum.Runtime.Instance.register()

    children =
      [
        pg_child(),
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
      activity_supervisor_child(instance),
      child(Continuum.Runtime.Recovery, instance),
      child(Continuum.Runtime.Snapshotter, instance),
      child(Continuum.Runtime.Dispatcher, instance),
      child(Continuum.Runtime.ActivityWorker.Dispatcher, instance),
      child(Continuum.Runtime.TimerWheel, instance),
      child(Continuum.Runtime.SignalRouter, instance)
    ]
    |> Enum.reject(&is_nil/1)
  end

  defp child(module, instance) do
    Supervisor.child_spec({module, instance: instance}, id: {module, instance.name})
  end

  defp activity_supervisor_child(%{activity_executor: :builtin} = instance) do
    child(Continuum.Runtime.ActivityWorker.Supervisor, instance)
  end

  defp activity_supervisor_child(_instance), do: nil

  defp pg_child do
    %{
      id: {:pg, :continuum},
      start: {:pg, :start_link, [:continuum]},
      type: :worker
    }
  end
end
