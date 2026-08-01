defmodule Continuum.Runtime.ActivityWorker.Supervisor do
  @moduledoc """
  Dynamic supervisor for activity worker processes.
  """

  use DynamicSupervisor

  @doc false
  def start_link(opts \\ []) do
    instance = Continuum.Runtime.Instance.lookup(Keyword.get(opts, :instance, Continuum))
    DynamicSupervisor.start_link(__MODULE__, opts, name: instance.activity_supervisor)
  end

  @impl true
  def init(opts) do
    opts = Continuum.Config.validate_component!(:activity_supervisor, opts)
    instance = Continuum.Runtime.Instance.lookup(Keyword.get(opts, :instance, Continuum))

    DynamicSupervisor.init(
      strategy: :one_for_one,
      max_children: instance.activity_max_concurrency
    )
  end
end
