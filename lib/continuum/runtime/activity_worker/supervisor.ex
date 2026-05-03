defmodule Continuum.Runtime.ActivityWorker.Supervisor do
  @moduledoc """
  Dynamic supervisor for activity worker processes.
  """

  use DynamicSupervisor

  @doc false
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
