defmodule Continuum.Runtime.Journal.InMemory do
  @moduledoc """
  In-memory journal backed by a single GenServer.

  Used by tests and by `Continuum.Test`. Not durable — process death loses
  all journaled events.
  """

  @behaviour Continuum.Runtime.Journal

  use GenServer

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def start_run(run_id, workflow, input) do
    GenServer.call(__MODULE__, {:start_run, run_id, workflow, input})
  end

  @impl true
  def append!(run_id, event, _lease_token) do
    GenServer.call(__MODULE__, {:append, run_id, event})
  end

  @impl true
  def load(run_id) do
    GenServer.call(__MODULE__, {:load, run_id})
  end

  @impl true
  def suspend!(run_id, _lease_token) do
    GenServer.call(__MODULE__, {:suspend, run_id})
  end

  @impl true
  def complete!(run_id, result, _lease_token) do
    GenServer.call(__MODULE__, {:complete, run_id, result})
  end

  @impl true
  def fail!(run_id, error, _lease_token) do
    GenServer.call(__MODULE__, {:fail, run_id, error})
  end

  @doc "Return the full state of all runs known to the in-memory journal."
  def dump, do: GenServer.call(__MODULE__, :dump)

  @doc "Wipe all journals. Test helper only."
  def reset, do: GenServer.call(__MODULE__, :reset)

  @impl true
  def get_run(run_id), do: GenServer.call(__MODULE__, {:get_run, run_id})

  # ---------------------------------------------------------------------------
  # Implementation
  # ---------------------------------------------------------------------------

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:start_run, run_id, workflow, input}, _from, state) do
    run = %{
      run_id: run_id,
      workflow: workflow,
      input: input,
      events: [],
      state: :running,
      result: nil,
      error: nil
    }

    {:reply, :ok, Map.put(state, run_id, run)}
  end

  def handle_call({:append, run_id, event}, _from, state) do
    state =
      Map.update(state, run_id, init_run(run_id), fn run ->
        Map.update!(run, :events, &(&1 ++ [event]))
      end)

    {:reply, :ok, state}
  end

  def handle_call({:load, run_id}, _from, state) do
    events =
      case Map.get(state, run_id) do
        %{events: events} -> events
        _ -> []
      end

    {:reply, events, state}
  end

  def handle_call({:suspend, run_id}, _from, state) do
    state =
      Map.update(state, run_id, init_run(run_id), fn run ->
        %{run | state: :suspended}
      end)

    {:reply, :ok, state}
  end

  def handle_call({:complete, run_id, result}, _from, state) do
    state =
      Map.update(state, run_id, init_run(run_id), fn run ->
        %{run | state: :completed, result: result}
      end)

    :ok = Continuum.Runtime.Engine.broadcast_run_finished(run_id, :completed, result)
    {:reply, :ok, state}
  end

  def handle_call({:fail, run_id, error}, _from, state) do
    state =
      Map.update(state, run_id, init_run(run_id), fn run ->
        %{run | state: :failed, error: error}
      end)

    broadcast_failed(run_id, error)
    {:reply, :ok, state}
  end

  def handle_call(:dump, _from, state), do: {:reply, state, state}
  def handle_call(:reset, _from, _state), do: {:reply, :ok, %{}}
  def handle_call({:get_run, run_id}, _from, state), do: {:reply, Map.get(state, run_id), state}

  defp init_run(run_id) do
    %{
      run_id: run_id,
      workflow: nil,
      input: nil,
      events: [],
      state: :running,
      result: nil,
      error: nil
    }
  end

  defp broadcast_failed(_run_id, {_kind, _reason, stacktrace}) when is_list(stacktrace), do: :ok

  defp broadcast_failed(run_id, error) do
    Continuum.Runtime.Engine.broadcast_run_finished(run_id, :failed, error)
  end
end
