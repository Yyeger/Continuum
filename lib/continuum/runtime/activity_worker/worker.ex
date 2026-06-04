defmodule Continuum.Runtime.ActivityWorker.Worker do
  @moduledoc """
  Executes one leased activity task.
  """

  use GenServer

  alias Continuum.Runtime.ActivityWorker

  @doc false
  def start_link(task) do
    GenServer.start_link(__MODULE__, task)
  end

  @doc false
  def child_spec(task) do
    %{
      id: {__MODULE__, task.id},
      start: {__MODULE__, :start_link, [task]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(task) do
    {:ok, task, {:continue, :run}}
  end

  @impl true
  def handle_continue(:run, task) do
    :ok = ActivityWorker.execute(task)
    {:stop, :normal, task}
  end
end
