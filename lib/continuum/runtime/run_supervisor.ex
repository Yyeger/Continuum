defmodule Continuum.Runtime.RunSupervisor do
  @moduledoc """
  DynamicSupervisor that owns one `Continuum.Runtime.Engine` process per
  active run.
  """

  use DynamicSupervisor

  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
