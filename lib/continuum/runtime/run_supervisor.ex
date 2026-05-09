defmodule Continuum.Runtime.RunSupervisor do
  @moduledoc """
  DynamicSupervisor that owns one `Continuum.Runtime.Engine` process per
  active run.
  """

  use DynamicSupervisor

  def start_link(opts \\ []) do
    instance = Continuum.Runtime.Instance.lookup(Keyword.get(opts, :instance, Continuum))
    DynamicSupervisor.start_link(__MODULE__, opts, name: instance.run_supervisor)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
