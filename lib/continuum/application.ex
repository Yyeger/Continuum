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
        activity_max_concurrency: Application.get_env(:continuum, :activity_max_concurrency, 10),
        workflow_modules: Application.get_env(:continuum, :workflow_modules)
      )
      |> Continuum.Runtime.Instance.register()

    opts = [strategy: :one_for_one, name: Continuum.Supervisor]
    Supervisor.start_link(child_specs(instance), opts)
  end

  @doc false
  def child_specs(instance) do
    [
      pg_child(),
      {Phoenix.PubSub, name: instance.pubsub},
      {Registry, keys: :unique, name: instance.registry},
      Continuum.Runtime.Journal.InMemory,
      child(Continuum.Runtime.RunSupervisor, instance)
    ]
  end

  defp child(module, instance) do
    Supervisor.child_spec({module, instance: instance}, id: {module, instance.name})
  end

  defp pg_child do
    %{
      id: {:pg, :continuum},
      start: {:pg, :start_link, [:continuum]},
      type: :worker
    }
  end
end
