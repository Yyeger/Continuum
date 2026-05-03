defmodule Continuum.Runtime.Journal.Postgres do
  @moduledoc """
  Durable journal adapter backed by Postgres via Ecto.

  Implements the `Continuum.Runtime.Journal` behaviour. Every append
  operation is transactional and CAS-guarded by the lease state on the
  run row. Appends lock the run row, validate the lease token, assign a
  sequence number, and insert the event in one transaction.

  The replay loop and engine code are identical whether this adapter or
  `InMemory` is in use — the only difference is durability and the
  fencing-token enforcement on writes.
  """

  @behaviour Continuum.Runtime.Journal

  import Ecto.Query

  alias Continuum.Schema.{ActivityTask, Event, Run, Signal, Timer}
  alias Continuum.Telemetry

  @impl true
  def start_run(run_id, workflow, input) do
    version_hash =
      try do
        workflow.__continuum_workflow__().version_hash
      rescue
        UndefinedFunctionError -> <<0::256>>
      end

    changeset =
      %Run{}
      |> Ecto.Changeset.change(%{
        id: run_id,
        workflow: inspect(workflow),
        version_hash: version_hash,
        state: "running",
        input: encode_term(input)
      })

    case repo().insert(changeset) do
      {:ok, _} -> :ok
      {:error, changeset} -> {:error, changeset}
    end
  end

  @impl true
  def append!(run_id, event, lease_token) do
    {event_type, payload} = encode_event(event)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        seq = event[:seq] || next_seq(run_id)

        changeset =
          %Event{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            seq: seq,
            event_type: event_type,
            payload: payload,
            inserted_at: DateTime.utc_now()
          })

        case repo().insert(changeset) do
          {:ok, _} -> :ok
          {:error, changeset} -> repo().rollback({:insert_failed, changeset})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres append! failed: #{inspect(reason)}"
    end
  end

  @impl true
  def load(run_id) do
    events =
      repo().all(
        from(e in Event,
          where: e.run_id == ^run_id,
          order_by: [asc: e.seq]
        )
      )

    Enum.map(events, &decode_event/1)
  end

  def schedule_activity!(run_id, event, task, lease_token) do
    {event_type, payload} = encode_event(event)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)
        now = DateTime.utc_now()

        event_changeset =
          %Event{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            seq: event.seq,
            event_type: event_type,
            payload: payload,
            inserted_at: now
          })

        task_changeset =
          %ActivityTask{}
          |> Ecto.Changeset.change(%{
            id: task.id,
            run_id: run_id,
            seq: task.seq,
            mfa: encode_term(task),
            attempt: 1,
            state: "available"
          })

        with {:ok, _event} <- repo().insert(event_changeset),
             {:ok, _task} <- repo().insert(task_changeset) do
          :ok
        else
          {:error, changeset} -> repo().rollback({:activity_schedule_failed, changeset})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres schedule_activity! failed: #{inspect(reason)}"
    end
  end

  def schedule_timer!(run_id, event, timer, lease_token) do
    {event_type, payload} = encode_event(event)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        event_changeset =
          %Event{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            seq: event.seq,
            event_type: event_type,
            payload: payload,
            inserted_at: DateTime.utc_now()
          })

        timer_changeset =
          %Timer{}
          |> Ecto.Changeset.change(%{
            id: timer.id,
            run_id: run_id,
            fires_at: timer.fires_at,
            fired: false
          })

        with {:ok, _event} <- repo().insert(event_changeset),
             {:ok, _timer} <- repo().insert(timer_changeset),
             {1, _} <-
               repo().update_all(
                 leased_run_query(run_id, lease_token),
                 set: [next_wakeup_at: timer.fires_at]
               ) do
          :ok
        else
          {0, _} -> repo().rollback({:timer_schedule_failed, :lease_mismatch})
          {:error, changeset} -> repo().rollback({:timer_schedule_failed, changeset})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres schedule_timer! failed: #{inspect(reason)}"
    end
  end

  def schedule_signal_await!(run_id, event, lease_token) do
    {event_type, payload} = encode_event(event)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        changeset =
          %Event{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            seq: event.seq,
            event_type: event_type,
            payload: payload,
            inserted_at: DateTime.utc_now()
          })

        case repo().insert(changeset) do
          {:ok, _event} -> :ok
          {:error, changeset} -> repo().rollback({:signal_await_failed, changeset})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres schedule_signal_await! failed: #{inspect(reason)}"
    end
  end

  def deliver_signal!(run_id, name, payload) do
    signal_name = Atom.to_string(name)
    now = DateTime.utc_now()

    result =
      repo().transaction(fn ->
        changeset =
          %Signal{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            name: signal_name,
            payload: encode_term(payload),
            delivered: false,
            inserted_at: now
          })

        with {:ok, _signal} <- repo().insert(changeset),
             {_count, _} <-
               repo().update_all(
                 from(r in Run, where: r.id == ^run_id),
                 set: [next_wakeup_at: now]
               ),
             {:ok, _} <- repo().query("SELECT pg_notify('continuum_signal', $1)", [run_id]) do
          :ok
        else
          {:error, reason} -> repo().rollback({:signal_delivery_failed, reason})
        end
      end)

    case result do
      {:ok, :ok} ->
        Telemetry.execute([:continuum, :signal, :delivered], %{}, %{
          run_id: run_id,
          signal_name: name,
          durable?: true
        })

        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres deliver_signal! failed: #{inspect(reason)}"
    end
  end

  def consume_signal(run_id, name, lease_token) do
    signal_name = Atom.to_string(name)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        signal =
          repo().one(
            from(s in Signal,
              where: s.run_id == ^run_id and s.name == ^signal_name and s.delivered == false,
              order_by: [asc: s.inserted_at, asc: s.id],
              limit: 1,
              lock: "FOR UPDATE SKIP LOCKED"
            )
          )

        case signal do
          nil ->
            :none

          %Signal{} = signal ->
            payload = decode_term(signal.payload)

            event = %{
              type: :signal_received,
              name: name,
              payload: payload,
              seq: next_seq(run_id)
            }

            {event_type, event_payload} = encode_event(event)

            with {:ok, _event} <-
                   %Event{}
                   |> Ecto.Changeset.change(%{
                     run_id: run_id,
                     seq: event.seq,
                     event_type: event_type,
                     payload: event_payload,
                     inserted_at: DateTime.utc_now()
                   })
                   |> repo().insert(),
                 {1, _} <-
                   repo().update_all(
                     from(s in Signal, where: s.id == ^signal.id),
                     set: [delivered: true]
                   ) do
              {:ok, payload}
            else
              {0, _} -> repo().rollback({:signal_consume_failed, :already_delivered})
              {:error, changeset} -> repo().rollback({:signal_consume_failed, changeset})
            end
        end
      end)

    case result do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres consume_signal failed: #{inspect(reason)}"
    end
  end

  def clear_next_wakeup!(run_id, lease_token) do
    cas_update_run(run_id, lease_token, %{next_wakeup_at: nil})
  end

  @impl true
  def suspend!(run_id, lease_token) do
    cas_update_run(run_id, lease_token, %{state: "suspended"})
  end

  @impl true
  def complete!(run_id, result, lease_token) do
    cas_update_run(run_id, lease_token, %{
      state: "completed",
      result: encode_term(result),
      completed_at: DateTime.utc_now()
    })
  end

  @impl true
  def fail!(run_id, error, lease_token) do
    cas_update_run(run_id, lease_token, %{
      state: "failed",
      error: encode_term(error),
      completed_at: DateTime.utc_now()
    })
  end

  @impl true
  def get_run(run_id) do
    case repo().one(from(r in Run, where: r.id == ^run_id)) do
      nil -> nil
      run -> decode_run(run)
    end
  end

  defp cas_update_run(run_id, lease_token, updates) do
    query = leased_run_query(run_id, lease_token)

    case repo().update_all(query, set: Map.to_list(updates)) do
      {1, _} ->
        :ok

      {0, _} ->
        raise "Continuum.Runtime.Journal.Postgres CAS update failed for run #{inspect(run_id)} — lease token mismatch or run not found"
    end
  end

  defp validate_lease!(%Run{lease_token: nil, lease_owner: nil}, nil), do: :ok

  defp validate_lease!(%Run{lease_token: token}, token) when not is_nil(token), do: :ok

  defp validate_lease!(%Run{} = run, lease_token) do
    repo().rollback(
      {:lease_mismatch,
       expected: lease_token,
       actual: %{lease_owner: run.lease_owner, lease_token: run.lease_token}}
    )
  end

  defp lock_and_validate_run!(run_id, lease_token) do
    run =
      repo().one(
        from(r in Run,
          where: r.id == ^run_id,
          lock: "FOR UPDATE"
        )
      )

    case run do
      nil -> repo().rollback({:run_not_found, run_id})
      %Run{} = run -> validate_lease!(run, lease_token)
    end
  end

  defp leased_run_query(run_id, nil) do
    from(r in Run,
      where: r.id == ^run_id and is_nil(r.lease_owner) and is_nil(r.lease_token)
    )
  end

  defp leased_run_query(run_id, lease_token) do
    from(r in Run,
      where: r.id == ^run_id and r.lease_token == ^lease_token
    )
  end

  defp next_seq(run_id) do
    case repo().one(
           from(e in Event,
             where: e.run_id == ^run_id,
             select: max(e.seq)
           )
         ) do
      nil -> 0
      seq -> seq + 1
    end
  end

  defp encode_event(%{type: type} = event) do
    payload =
      event
      |> Map.delete(:type)
      |> Map.delete(:seq)
      |> encode_term()

    {Atom.to_string(type), payload}
  end

  defp decode_event(%Event{event_type: event_type, payload: payload, seq: seq}) do
    decoded = decode_term(payload)
    type = String.to_atom(event_type)

    decoded
    |> Map.put(:type, type)
    |> Map.put(:seq, seq)
    |> atomize_keys_by_type(type)
  end

  defp atomize_keys_by_type(map, :side_effect) do
    map
    |> maybe_atomize(:kind)
  end

  defp atomize_keys_by_type(map, :activity_completed) do
    map
    |> maybe_decode_mfa()
  end

  defp atomize_keys_by_type(map, :signal_received) do
    map
    |> maybe_atomize(:name)
  end

  defp atomize_keys_by_type(map, _type), do: map

  defp maybe_atomize(map, key) do
    case Map.get(map, key) || Map.get(map, to_string(key)) do
      nil -> map
      val when is_binary(val) -> Map.put(map, key, String.to_atom(val))
      _ -> map
    end
  end

  defp maybe_decode_mfa(map) do
    key = :mfa
    str_key = "mfa"

    case Map.get(map, key) || Map.get(map, str_key) do
      [mod, fun, args] when is_binary(mod) and is_binary(fun) and is_list(args) ->
        Map.put(map, key, {String.to_atom("Elixir." <> mod), String.to_atom(fun), args})

      [mod, fun, args] when is_atom(mod) and is_atom(fun) and is_list(args) ->
        Map.put(map, key, {mod, fun, args})

      _ ->
        map
    end
  end

  defp decode_run(%Run{} = run) do
    %{
      run_id: run.id,
      workflow: run.workflow,
      state: String.to_atom(run.state),
      result: decode_term(run.result),
      error: decode_term(run.error),
      input: decode_term(run.input)
    }
  end

  defp encode_term(nil), do: nil
  defp encode_term(term), do: %{__term__: Base.encode64(:erlang.term_to_binary(term))}

  defp decode_term(nil), do: nil

  defp decode_term(%{"__term__" => encoded}) when is_binary(encoded) do
    :erlang.binary_to_term(Base.decode64!(encoded))
  end

  defp decode_term(other), do: other

  defp repo do
    Application.fetch_env!(:continuum, :repo)
  end
end
