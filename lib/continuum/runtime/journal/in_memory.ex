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

  alias Continuum.Runtime.Instance

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def start_run(%Instance{} = instance, run_id, workflow, input) do
    GenServer.call(__MODULE__, {:start_run, instance, run_id, workflow, input})
  end

  @impl true
  def append!(%Instance{} = instance, run_id, event, _lease_token) do
    GenServer.call(__MODULE__, {:append, instance, run_id, event})
  end

  @impl true
  def load(%Instance{} = instance, run_id) do
    GenServer.call(__MODULE__, {:load, instance.name, run_id})
  end

  @impl true
  def load_with_snapshot(%Instance{} = instance, run_id, _lease_token) do
    GenServer.call(__MODULE__, {:load_with_snapshot, instance.name, run_id})
  end

  @impl true
  def take_snapshot!(%Instance{} = instance, %Continuum.Snapshot{} = snapshot) do
    GenServer.call(__MODULE__, {:take_snapshot, instance.name, snapshot})
  end

  @impl true
  def suspend!(%Instance{} = instance, run_id, _lease_token) do
    GenServer.call(__MODULE__, {:suspend, instance, run_id})
  end

  @impl true
  def complete!(%Instance{} = instance, run_id, result, _lease_token) do
    GenServer.call(__MODULE__, {:complete, instance, run_id, result})
  end

  @impl true
  def fail!(%Instance{} = instance, run_id, error, _lease_token) do
    GenServer.call(__MODULE__, {:fail, instance, run_id, error})
  end

  @doc """
  Buffer a signal in the run's in-memory mailbox.

  Mirrors the `continuum_signals` semantics of the Postgres adapter: the
  payload is held until a live `await signal(name)` consumes it, so signals
  arriving early or out of order wait for their matching await instead of
  corrupting the journal tail. Returns `{:error, :not_found}` when the run
  does not exist.
  """
  def deliver_signal!(%Instance{} = instance, run_id, name, payload) do
    GenServer.call(__MODULE__, {:deliver_signal, instance.name, run_id, name, payload})
  end

  @doc """
  Pop the oldest buffered payload for signal `name`, or return `:none`.

  Called by `Continuum.Runtime.Effect` when an `await signal(name)` reaches
  the live tail; the consumed payload is journaled as `signal_received` with
  the await's command identity.
  """
  def consume_buffered_signal!(%Instance{} = instance, run_id, name) do
    GenServer.call(__MODULE__, {:consume_buffered_signal, instance.name, run_id, name})
  end

  @doc "Return the full state of all runs known to the in-memory journal."
  def dump, do: GenServer.call(__MODULE__, :dump)

  @doc "Wipe all journals. Test helper only."
  def reset, do: GenServer.call(__MODULE__, :reset)

  @impl true
  def get_run(%Instance{} = instance, run_id),
    do: GenServer.call(__MODULE__, {:get_run, instance.name, run_id})

  # ---------------------------------------------------------------------------
  # Implementation
  # ---------------------------------------------------------------------------

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:start_run, instance, run_id, workflow, input}, _from, state) do
    run = %{
      run_id: run_id,
      workflow: workflow,
      version_hash: version_hash(workflow),
      input: input,
      events: [],
      snapshots: [],
      signal_buffer: %{},
      state: :running,
      result: nil,
      error: nil
    }

    {:reply, :ok, put_run(state, instance.name, run_id, run)}
  end

  def handle_call({:append, instance, run_id, event}, _from, state) do
    state =
      update_run(state, instance.name, run_id, fn run ->
        Map.update!(run, :events, fn events ->
          events ++ [normalize_event_seq(event, events)]
        end)
      end)

    {:reply, :ok, state}
  end

  def handle_call({:load, instance_name, run_id}, _from, state) do
    events =
      case get_run_state(state, instance_name, run_id) do
        %{events: events} -> events
        _ -> []
      end

    {:reply, events, state}
  end

  def handle_call({:load_with_snapshot, instance_name, run_id}, _from, state) do
    {snapshot, events} =
      case get_run_state(state, instance_name, run_id) do
        %{events: events, snapshots: snapshots} ->
          snapshot = latest_snapshot(snapshots)
          events = events_after_snapshot(events, snapshot)
          {snapshot, events}

        _ ->
          {nil, []}
      end

    {:reply, {snapshot, events}, state}
  end

  def handle_call({:suspend, instance, run_id}, _from, state) do
    state =
      update_run(state, instance.name, run_id, fn run ->
        %{run | state: :suspended}
      end)

    {:reply, :ok, state}
  end

  def handle_call({:complete, instance, run_id, result}, _from, state) do
    state =
      update_run(state, instance.name, run_id, fn run ->
        %{run | state: :completed, result: result}
      end)

    :ok = Continuum.Runtime.Engine.broadcast_run_finished(instance, run_id, :completed, result)

    {:reply, :ok, state}
  end

  def handle_call({:fail, instance, run_id, error}, _from, state) do
    # Parity with the Postgres adapter's canonical terminal state: a cancel
    # is stored as :cancelled, not failed + :cancelled. (In-memory has no
    # separate cancel path, so a user failure whose error term is literally
    # :cancelled is indistinguishable here — acceptable for the test journal.)
    terminal_state = if error == :cancelled, do: :cancelled, else: :failed

    state =
      update_run(state, instance.name, run_id, fn run ->
        %{run | state: terminal_state, error: error}
      end)

    broadcast_failed(instance, run_id, error)
    {:reply, :ok, state}
  end

  def handle_call({:deliver_signal, instance_name, run_id, name, payload}, _from, state) do
    case get_run_state(state, instance_name, run_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      # Parity with the Postgres adapter: buffering a signal no run can ever
      # consume is silent signal loss, so a terminal run rejects delivery.
      %{state: run_state} when run_state not in [:running, :suspended] ->
        {:reply, {:error, :run_terminal}, state}

      _run ->
        state =
          update_run(state, instance_name, run_id, fn run ->
            Map.update(run, :signal_buffer, %{name => [payload]}, fn buffer ->
              Map.update(buffer, name, [payload], &(&1 ++ [payload]))
            end)
          end)

        {:reply, :ok, state}
    end
  end

  def handle_call({:consume_buffered_signal, instance_name, run_id, name}, _from, state) do
    case get_run_state(state, instance_name, run_id) do
      %{signal_buffer: buffer} when is_map(buffer) ->
        case Map.get(buffer, name, []) do
          [payload | rest] ->
            state =
              update_run(state, instance_name, run_id, fn run ->
                Map.put(run, :signal_buffer, put_buffer(buffer, name, rest))
              end)

            {:reply, {:ok, payload}, state}

          [] ->
            {:reply, :none, state}
        end

      _ ->
        {:reply, :none, state}
    end
  end

  def handle_call(:dump, _from, state), do: {:reply, state, state}
  def handle_call(:reset, _from, _state), do: {:reply, :ok, %{}}

  def handle_call({:take_snapshot, instance_name, snapshot}, _from, state) do
    state =
      update_run(state, instance_name, snapshot.run_id, fn run ->
        snapshots =
          run
          |> Map.get(:snapshots, [])
          |> Enum.reject(&(&1.through_seq == snapshot.through_seq))
          |> Kernel.++([snapshot])
          |> Enum.sort_by(& &1.through_seq)

        Map.put(run, :snapshots, snapshots)
      end)

    {:reply, :ok, state}
  end

  def handle_call({:get_run, instance_name, run_id}, _from, state),
    do: {:reply, get_run_state(state, instance_name, run_id), state}

  defp init_run(run_id) do
    %{
      run_id: run_id,
      workflow: nil,
      version_hash: nil,
      input: nil,
      events: [],
      snapshots: [],
      signal_buffer: %{},
      state: :running,
      result: nil,
      error: nil
    }
  end

  defp put_buffer(buffer, name, []), do: Map.delete(buffer, name)
  defp put_buffer(buffer, name, rest), do: Map.put(buffer, name, rest)

  defp get_run_state(state, instance_name, run_id) do
    state
    |> Map.get(instance_name, %{})
    |> Map.get(run_id)
  end

  defp put_run(state, instance_name, run_id, run) do
    Map.update(state, instance_name, %{run_id => run}, &Map.put(&1, run_id, run))
  end

  defp update_run(state, instance_name, run_id, fun) do
    # Map.update/4 inserts its default verbatim — fun must be applied to the
    # fresh run on both branches or the first write to an unknown run (e.g.
    # an append) is silently dropped.
    Map.update(state, instance_name, %{run_id => fun.(init_run(run_id))}, fn runs ->
      Map.update(runs, run_id, fun.(init_run(run_id)), fun)
    end)
  end

  defp broadcast_failed(_instance, _run_id, {_kind, _reason, stacktrace})
       when is_list(stacktrace),
       do: :ok

  # Cancellation broadcasts one canonical state across adapters.
  defp broadcast_failed(instance, run_id, :cancelled) do
    Continuum.Runtime.Engine.broadcast_run_finished(instance, run_id, :cancelled, :cancelled)
  end

  defp broadcast_failed(instance, run_id, error) do
    Continuum.Runtime.Engine.broadcast_run_finished(instance, run_id, :failed, error)
  end

  defp version_hash(workflow) do
    workflow.__continuum_workflow__().version_hash
  rescue
    UndefinedFunctionError -> <<0::256>>
  end

  defp latest_snapshot([]), do: nil

  defp latest_snapshot(snapshots) do
    snapshots
    |> Enum.sort_by(& &1.through_seq)
    |> List.last()
  end

  defp events_after_snapshot(events, nil), do: events

  defp events_after_snapshot(events, snapshot) do
    Enum.filter(events, &(event_seq(&1) > snapshot.through_seq))
  end

  defp event_seq(%{seq: nil}), do: -1
  defp event_seq(%{seq: seq}), do: seq

  defp normalize_event_seq(event, events) do
    case Map.get(event, :seq) do
      nil -> Map.put(event, :seq, next_seq(events))
      _seq -> event
    end
  end

  defp next_seq([]), do: 0

  defp next_seq(events) do
    events
    |> Enum.map(&event_seq/1)
    |> Enum.max(fn -> -1 end)
    |> Kernel.+(1)
  end
end
