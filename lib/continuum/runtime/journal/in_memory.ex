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

  alias Continuum.DurableTerm
  alias Continuum.Runtime.{Instance, JournalError}

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def start_run(%Instance{} = instance, run_id, workflow, input) do
    start_run(instance, run_id, workflow, input, [])
  end

  @impl true
  def start_run(%Instance{} = instance, run_id, workflow, input, opts) do
    DurableTerm.validate!(input, :input)
    idempotency_key = normalize_ingress_key(Keyword.get(opts, :idempotency_key))

    GenServer.call(
      __MODULE__,
      {:start_run, instance, run_id, workflow, input, Keyword.get(opts, :namespace, "default"),
       idempotency_key}
    )
  end

  @impl true
  def append!(%Instance{} = instance, run_id, event, _lease_token) do
    DurableTerm.validate!(event, :event)

    __MODULE__
    |> GenServer.call({:append, instance, run_id, event})
    |> unwrap_write!(:append!)
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
    DurableTerm.validate!(snapshot, :snapshot)

    __MODULE__
    |> GenServer.call({:take_snapshot, instance.name, snapshot})
    |> unwrap_write!(:take_snapshot!)
  end

  @impl true
  def suspend!(%Instance{} = instance, run_id, _lease_token) do
    __MODULE__
    |> GenServer.call({:suspend, instance, run_id})
    |> unwrap_write!(:suspend!)
  end

  @impl true
  def complete!(%Instance{} = instance, run_id, result, _lease_token) do
    DurableTerm.validate!(result, :result)

    __MODULE__
    |> GenServer.call({:complete, instance, run_id, result})
    |> unwrap_write!(:complete!)
  end

  @impl true
  def fail!(%Instance{} = instance, run_id, error, _lease_token) do
    DurableTerm.validate!(error, :error)

    __MODULE__
    |> GenServer.call({:fail, instance, run_id, error})
    |> unwrap_write!(:fail!)
  end

  @doc """
  Buffer a signal in the run's in-memory mailbox.

  Mirrors the `continuum_signals` semantics of the Postgres adapter: the
  payload is held until a live `await signal(name)` consumes it, so signals
  arriving early or out of order wait for their matching await instead of
  corrupting the journal tail. Returns `{:error, :not_found}` when the run
  does not exist.
  """
  @impl true
  def deliver_signal!(%Instance{} = instance, run_id, name, payload) do
    case deliver_signal!(instance, run_id, name, payload, []) do
      {:ok, _run_id, _status} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @impl true
  def deliver_signal!(%Instance{} = instance, run_id, name, payload, opts) do
    DurableTerm.validate!(payload, :signal)
    delivery_id = normalize_delivery_id(Keyword.get(opts, :delivery_id))

    GenServer.call(
      __MODULE__,
      {:deliver_signal, instance.name, run_id, name, payload, delivery_id}
    )
  end

  @impl true
  def start_child!(%Instance{} = instance, parent_run_id, child, _lease_token) do
    DurableTerm.validate!(child.input, :input)

    __MODULE__
    |> GenServer.call({:start_child, instance.name, parent_run_id, child})
    |> unwrap_write!(:start_child!)

    # Started from the client, not from inside `handle_call/3`: the child engine
    # journals through this same GenServer as it boots, so starting it while the
    # journal is answering its own call would deadlock. The parent link is
    # already recorded by then, which is what lets the child's terminal
    # transition wake the parent.
    case Continuum.Runtime.Engine.start_run(child.workflow, child.input,
           run_id: child.child_run_id,
           journal: __MODULE__,
           instance: instance.name
         ) do
      {:ok, _run_id} -> :ok
      {:error, reason} -> raise JournalError, op: :start_child!, reason: reason
    end
  end

  @impl true
  def await_child_terminal!(
        %Instance{} = instance,
        parent_run_id,
        child_run_id,
        command_id,
        seq,
        _lease_token
      ) do
    case GenServer.call(
           __MODULE__,
           {:await_child_terminal, instance.name, parent_run_id, child_run_id, command_id, seq}
         ) do
      {:error, reason} -> raise JournalError, op: :await_child_terminal!, reason: reason
      outcome -> outcome
    end
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
  def handle_call(
        {:start_run, instance, run_id, workflow, input, namespace, idempotency_key},
        _from,
        state
      ) do
    case find_start_conflict(state, instance.name, run_id, workflow, namespace, idempotency_key) do
      nil ->
        run = %{
          run_id: run_id,
          workflow: workflow,
          version_hash: version_hash(workflow),
          namespace: namespace,
          idempotency_key: idempotency_key,
          input: input,
          events: [],
          snapshots: [],
          signal_buffer: %{},
          signal_delivery_ids: MapSet.new(),
          state: :running,
          result: nil,
          error: nil,
          error_stacktrace: nil
        }

        {:reply, :ok, put_run(state, instance.name, run_id, run)}

      {:run_id, _existing_id} ->
        {:reply, {:error, :already_exists}, state}

      {:idempotency_key, existing_id} ->
        {:reply, {:error, {:already_started, existing_id}}, state}
    end
  end

  def handle_call({:append, instance, run_id, event}, _from, state) do
    result =
      update_active_run(state, instance.name, run_id, fn run ->
        Map.update!(run, :events, fn events ->
          events ++ [normalize_event_seq(event, events)]
        end)
      end)

    reply_update(result, state)
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
    result =
      update_active_run(state, instance.name, run_id, fn run ->
        %{run | state: :suspended}
      end)

    reply_update(result, state)
  end

  def handle_call({:complete, instance, run_id, result}, _from, state) do
    update =
      update_active_run(state, instance.name, run_id, fn run ->
        %{run | state: :completed, result: result}
      end)

    case update do
      {:ok, state} ->
        :ok =
          Continuum.Runtime.Engine.broadcast_run_finished(instance, run_id, :completed, result)

        wake_parent(state, instance, run_id)
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:fail, instance, run_id, error}, _from, state) do
    {public_error, stacktrace} = Continuum.RunFailure.split(error)

    # Parity with the Postgres adapter's canonical terminal state: a cancel
    # is stored as :cancelled, not failed + :cancelled. (In-memory has no
    # separate cancel path, so a user failure whose error term is literally
    # :cancelled is indistinguishable here — acceptable for the test journal.)
    terminal_state = if public_error == :cancelled, do: :cancelled, else: :failed

    update =
      update_active_run(state, instance.name, run_id, fn run ->
        %{run | state: terminal_state, error: public_error, error_stacktrace: stacktrace}
      end)

    case update do
      {:ok, state} ->
        broadcast_failed(instance, run_id, public_error)
        wake_parent(state, instance, run_id)
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:deliver_signal, instance_name, run_id, name, payload, delivery_id},
        _from,
        state
      ) do
    case get_run_state(state, instance_name, run_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      # Parity with the Postgres adapter: buffering a signal no run can ever
      # consume is silent signal loss, so a terminal run rejects delivery.
      %{state: run_state} when run_state not in [:running, :suspended] ->
        {:reply, {:error, :run_terminal}, state}

      run ->
        if duplicate_signal?(run, name, delivery_id) do
          {:reply, {:ok, run_id, :duplicate}, state}
        else
          state =
            update_run(state, instance_name, run_id, fn run ->
              run
              |> Map.update(:signal_buffer, %{name => [payload]}, fn buffer ->
                Map.update(buffer, name, [payload], &(&1 ++ [payload]))
              end)
              |> record_signal_delivery(name, delivery_id)
            end)

          {:reply, {:ok, run_id, :delivered}, state}
        end
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
    result =
      update_existing_run(state, instance_name, snapshot.run_id, fn run ->
        snapshots =
          run
          |> Map.get(:snapshots, [])
          |> Enum.reject(&(&1.through_seq == snapshot.through_seq))
          |> Kernel.++([snapshot])
          |> Enum.sort_by(& &1.through_seq)

        Map.put(run, :snapshots, snapshots)
      end)

    reply_update(result, state)
  end

  def handle_call({:start_child, instance_name, parent_run_id, child}, _from, state) do
    with :ok <- check_child_depth(state, instance_name, parent_run_id),
         {:ok, state} <-
           update_active_run(state, instance_name, parent_run_id, fn parent ->
             Map.update!(parent, :events, fn events ->
               events ++ [normalize_event_seq(child.started_event, events)]
             end)
           end) do
      {:reply, :ok, put_child_parent(state, instance_name, child.child_run_id, parent_run_id)}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:await_child_terminal, instance_name, parent_run_id, child_run_id, command_id, seq},
        _from,
        state
      ) do
    case child_outcome(state, instance_name, child_run_id) do
      :pending ->
        {:reply, :pending, state}

      {outcome, event_fields} ->
        event = Map.merge(event_fields, %{command_id: command_id, seq: seq})

        case update_active_run(state, instance_name, parent_run_id, fn parent ->
               Map.update!(parent, :events, &(&1 ++ [normalize_event_seq(event, &1)]))
             end) do
          {:ok, state} -> {:reply, reply_child_outcome(outcome, event), state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:get_run, instance_name, run_id}, _from, state),
    do: {:reply, get_run_state(state, instance_name, run_id), state}

  # Parent links live beside the runs rather than on them: the child run row is
  # created by the child engine's own `start_run/5`, after `start_child!/4` has
  # already had to record the relationship.
  @child_parents :__child_parents__

  defp put_child_parent(state, instance_name, child_run_id, parent_run_id) do
    Map.update(
      state,
      instance_name,
      %{@child_parents => %{child_run_id => parent_run_id}},
      fn runs ->
        Map.update(
          runs,
          @child_parents,
          %{child_run_id => parent_run_id},
          &Map.put(&1, child_run_id, parent_run_id)
        )
      end
    )
  end

  # The Postgres adapter arms the parent's next_wakeup_at and notifies; with no
  # dispatcher in-memory, the cast is the whole mechanism. It is a cast, so
  # issuing it from inside `handle_call/3` cannot deadlock, and a suspended
  # parent engine stays alive to receive it.
  defp wake_parent(state, instance, run_id) do
    case parent_of(state, instance.name, run_id) do
      nil -> :ok
      parent_run_id -> Continuum.Runtime.Engine.wake(instance, parent_run_id)
    end

    :ok
  end

  defp parent_of(state, instance_name, run_id) do
    state
    |> Map.get(instance_name, %{})
    |> Map.get(@child_parents, %{})
    |> Map.get(run_id)
  end

  defp check_child_depth(state, instance_name, parent_run_id) do
    max_depth = Application.get_env(:continuum, :max_child_depth, 10)
    depth = ancestor_depth(state, instance_name, parent_run_id, 0, max_depth)

    if depth + 1 > max_depth do
      {:error, {:max_child_depth_exceeded, depth: depth + 1, max_child_depth: max_depth}}
    else
      :ok
    end
  end

  defp ancestor_depth(_state, _instance_name, _run_id, depth, max_depth) when depth >= max_depth,
    do: depth

  defp ancestor_depth(state, instance_name, run_id, depth, max_depth) do
    case parent_of(state, instance_name, run_id) do
      nil -> depth
      parent -> ancestor_depth(state, instance_name, parent, depth + 1, max_depth)
    end
  end

  defp child_outcome(state, instance_name, child_run_id) do
    case get_run_state(state, instance_name, child_run_id) do
      %{state: :completed, result: result} ->
        {{:completed, result},
         %{type: :child_completed, child_run_id: child_run_id, result: result}}

      %{state: :failed, error: error} ->
        {{:failed, error}, %{type: :child_failed, child_run_id: child_run_id, error: error}}

      %{state: :cancelled} ->
        {:cancelled, %{type: :child_cancelled, child_run_id: child_run_id}}

      _ ->
        :pending
    end
  end

  defp reply_child_outcome({:completed, result}, event), do: {:completed, result, event}
  defp reply_child_outcome({:failed, error}, event), do: {:failed, error, event}
  defp reply_child_outcome(:cancelled, event), do: {:cancelled, event}

  defp put_buffer(buffer, name, []), do: Map.delete(buffer, name)
  defp put_buffer(buffer, name, rest), do: Map.put(buffer, name, rest)

  defp get_run_state(state, instance_name, run_id) do
    state
    |> Map.get(instance_name, %{})
    |> Map.get(run_id)
  end

  defp runs_only(state, instance_name) do
    state
    |> Map.get(instance_name, %{})
    |> Map.delete(@child_parents)
  end

  defp put_run(state, instance_name, run_id, run) do
    Map.update(state, instance_name, %{run_id => run}, &Map.put(&1, run_id, run))
  end

  defp update_run(state, instance_name, run_id, fun) do
    run = get_run_state(state, instance_name, run_id)
    put_run(state, instance_name, run_id, fun.(run))
  end

  defp update_active_run(state, instance_name, run_id, fun) do
    case get_run_state(state, instance_name, run_id) do
      nil ->
        {:error, {:run_not_found, run_id}}

      %{state: run_state} = run when run_state in [:running, :suspended] ->
        {:ok, put_run(state, instance_name, run_id, fun.(run))}

      %{state: run_state} ->
        {:error, {:run_not_active, run_state}}
    end
  end

  defp find_start_conflict(state, instance_name, run_id, workflow, namespace, idempotency_key) do
    runs = runs_only(state, instance_name)

    cond do
      Map.has_key?(runs, run_id) ->
        {:run_id, run_id}

      is_binary(idempotency_key) ->
        Enum.find_value(runs, fn {existing_id, run} ->
          if run.workflow == workflow and Map.get(run, :namespace, "default") == namespace and
               Map.get(run, :idempotency_key) == idempotency_key do
            {:idempotency_key, existing_id}
          end
        end)

      true ->
        nil
    end
  end

  defp duplicate_signal?(_run, _name, nil), do: false

  defp duplicate_signal?(run, name, delivery_id) do
    run
    |> Map.get(:signal_delivery_ids, MapSet.new())
    |> MapSet.member?({name, delivery_id})
  end

  defp record_signal_delivery(run, _name, nil), do: run

  defp record_signal_delivery(run, name, delivery_id) do
    Map.update(run, :signal_delivery_ids, MapSet.new([{name, delivery_id}]), fn ids ->
      MapSet.put(ids, {name, delivery_id})
    end)
  end

  defp normalize_ingress_key(nil), do: nil

  defp normalize_ingress_key(key)
       when is_binary(key) and byte_size(key) > 0 and byte_size(key) <= 255,
       do: key

  defp normalize_ingress_key(other) do
    raise ArgumentError,
          "expected :idempotency_key to be a non-empty binary of at most 255 bytes, got: #{inspect(other)}"
  end

  defp normalize_delivery_id(nil), do: nil

  defp normalize_delivery_id(id)
       when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= 255,
       do: id

  defp normalize_delivery_id(other) do
    raise ArgumentError,
          "expected :delivery_id to be a non-empty binary of at most 255 bytes, got: #{inspect(other)}"
  end

  defp update_existing_run(state, instance_name, run_id, fun) do
    case get_run_state(state, instance_name, run_id) do
      nil -> {:error, {:run_not_found, run_id}}
      run -> {:ok, put_run(state, instance_name, run_id, fun.(run))}
    end
  end

  defp reply_update({:ok, updated_state}, _state), do: {:reply, :ok, updated_state}
  defp reply_update({:error, reason}, state), do: {:reply, {:error, reason}, state}

  defp unwrap_write!(:ok, _op), do: :ok

  defp unwrap_write!({:error, reason}, op) do
    raise JournalError, op: op, reason: reason
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
