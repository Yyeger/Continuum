defmodule Continuum.Runtime.TimerWheel do
  @moduledoc """
  Caches near-term durable timers and wakes runs when timers fire.

  Postgres remains the source of truth. The wheel hydrates an ETS cache with
  timers due inside a short window, listens for `continuum_timer_armed`
  notifications, and schedules its next tick from the earliest cached timer.
  A periodic refresh rebuilds the cache, so a dropped Postgres notification can
  delay a timer by at most the refresh interval.
  """

  use GenServer
  require Logger

  import Ecto.Query

  alias Continuum.{Runtime.Engine, Runtime.Instance, Runtime.Journal, Telemetry}
  alias Continuum.Schema.Timer

  @default_refresh_ms 30_000
  @default_window_ms 60_000
  @default_batch_size 50
  @due_retry_ms 100

  @doc false
  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    GenServer.start_link(__MODULE__, opts, name: instance.timer_wheel)
  end

  @doc """
  Fire due timers once.
  """
  @spec fire_due_once(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def fire_due_once(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with {:ok, timers} <- claim_due(instance, batch_size) do
      Enum.each(timers, &fire_timer(instance, &1))
      {:ok, length(timers)}
    end
  end

  @doc false
  def reset_cache(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case GenServer.whereis(instance.timer_wheel) do
      nil -> :ok
      pid -> GenServer.call(pid, :reset_cache)
    end
  end

  @impl true
  def init(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    config = timer_config()
    table = :ets.new(:continuum_timer_cache, [:set, :protected])

    state = %{
      instance: instance,
      enabled?:
        Keyword.get(opts, :enabled?, Keyword.get(config, :enabled?, timer_enabled?(instance))),
      listen?: Keyword.get(opts, :listen?, Keyword.get(config, :listen?, true)),
      refresh_ms:
        Keyword.get(opts, :refresh_ms, Keyword.get(config, :refresh_ms, @default_refresh_ms)),
      window_ms:
        Keyword.get(opts, :window_ms, Keyword.get(config, :window_ms, @default_window_ms)),
      batch_size:
        Keyword.get(opts, :batch_size, Keyword.get(config, :batch_size, @default_batch_size)),
      table: table,
      tick_ref: nil,
      refresh_ref: nil,
      notifier: nil,
      notify_ref: nil
    }

    state =
      if state.enabled? do
        state
        |> start_listener()
        |> hydrate_window()
        |> schedule_refresh()
        |> schedule_next_tick()
      else
        state
      end

    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state =
      state
      |> fire_cached_due()
      |> schedule_next_tick()

    {:noreply, state}
  end

  def handle_info(:refresh, state) do
    state =
      state
      |> hydrate_window()
      |> schedule_refresh()
      |> schedule_next_tick()

    {:noreply, state}
  end

  def handle_info({:notification, _pid, _ref, "continuum_timer_armed", payload}, state) do
    state =
      payload
      |> notified_run_id()
      |> case do
        nil -> state
        run_id -> hydrate_run_timers(state, run_id)
      end
      |> schedule_next_tick()

    {:noreply, state}
  end

  def handle_info({:notification, _pid, _ref, _channel, _payload}, state), do: {:noreply, state}

  @impl true
  def handle_call(:reset_cache, _from, state) do
    cancel_timer(state.tick_ref)
    :ets.delete_all_objects(state.table)
    {:reply, :ok, %{state | tick_ref: nil}}
  end

  defp claim_due(instance, batch_size) do
    sql = """
    SELECT t.id::text, t.run_id::text, r.lease_token
    FROM continuum_timers AS t
    JOIN continuum_runs AS r ON r.id = t.run_id
    WHERE t.fired = false
      AND t.fires_at <= now()
      AND r.state = 'suspended'
      AND r.lease_token IS NOT NULL
      AND r.lease_expires_at > now()
    ORDER BY t.fires_at
    FOR UPDATE SKIP LOCKED
    LIMIT $1
    """

    case instance.repo.query(sql, [batch_size]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [id, run_id, lease_token] ->
           %{id: id, run_id: run_id, lease_token: lease_token}
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp claim_cached(_instance, [], _batch_size), do: {:ok, []}

  defp claim_cached(instance, timer_ids, batch_size) do
    sql = """
    SELECT t.id::text, t.run_id::text, r.lease_token
    FROM continuum_timers AS t
    JOIN continuum_runs AS r ON r.id = t.run_id
    WHERE t.id::text = ANY($1::text[])
      AND t.fired = false
      AND t.fires_at <= now()
      AND r.state = 'suspended'
      AND r.lease_token IS NOT NULL
      AND r.lease_expires_at > now()
    ORDER BY t.fires_at
    FOR UPDATE SKIP LOCKED
    LIMIT $2
    """

    case instance.repo.query(sql, [timer_ids, batch_size]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [id, run_id, lease_token] ->
           %{id: id, run_id: run_id, lease_token: lease_token}
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fire_cached_due(state) do
    due = due_entries(state.table)
    due_ids = Enum.map(due, fn {timer_id, _run_id, _fires_at_ms} -> timer_id end)

    case claim_cached(state.instance, due_ids, state.batch_size) do
      {:ok, timers} ->
        Enum.each(timers, fn timer ->
          :ets.delete(state.table, timer.id)
          fire_timer(state.instance, timer)
        end)

        purge_resolved_cached_timers(state, due_ids)

      {:error, reason} ->
        Logger.error("TimerWheel tick failed: #{inspect(reason)}")
    end

    state
  end

  defp fire_timer(instance, timer) do
    :ok = Journal.Postgres.fire_timer!(instance, timer.run_id, timer.id, timer.lease_token)
    Engine.wake(instance, timer.run_id)

    Telemetry.execute([:continuum, :timer, :fired], %{}, %{
      instance: instance.name,
      run_id: timer.run_id,
      timer_id: timer.id
    })
  rescue
    error in Continuum.Runtime.JournalError ->
      # Expected races, not wheel bugs: the run was cancelled/completed or its
      # lease rotated between our claim and the fire. The journal write rolled
      # back; whoever owns the run now (or nobody) handles the timer.
      if terminal_or_fenced?(error) do
        Logger.debug(
          "TimerWheel dropped fire for timer #{timer.id} (run #{timer.run_id}): " <>
            Exception.message(error)
        )
      else
        reraise(error, __STACKTRACE__)
      end
  end

  defp terminal_or_fenced?(%Continuum.Runtime.JournalError{reason: reason} = error) do
    case reason do
      {:run_not_active, _state} -> true
      {:run_not_found, _run_id} -> true
      _other -> Continuum.Runtime.JournalError.lease_lost?(error)
    end
  end

  defp hydrate_window(%{instance: %{repo: nil}} = state), do: state

  defp hydrate_window(state) do
    :ets.delete_all_objects(state.table)
    cache_timers(state, timers_due_within(state.instance, state.window_ms))
  end

  defp hydrate_run_timers(%{instance: %{repo: nil}} = state, _run_id), do: state

  defp hydrate_run_timers(state, run_id) do
    cache_timers(state, run_timers_due_within(state.instance, run_id, state.window_ms))
  end

  defp cache_timers(state, timers) do
    Enum.each(timers, fn %{id: id, run_id: run_id, fires_at: fires_at} ->
      :ets.insert(state.table, {id, run_id, date_time_to_ms(fires_at)})
    end)

    state
  end

  defp timers_due_within(instance, window_ms) do
    window_end =
      DateTime.utc_now()
      |> DateTime.add(window_ms, :millisecond)
      |> DateTime.truncate(:microsecond)

    instance.repo.all(
      from(t in Timer,
        where: t.fired == false and t.fires_at <= ^window_end,
        select: %{id: t.id, run_id: t.run_id, fires_at: t.fires_at}
      )
    )
  end

  defp run_timers_due_within(instance, run_id, window_ms) do
    window_end =
      DateTime.utc_now()
      |> DateTime.add(window_ms, :millisecond)
      |> DateTime.truncate(:microsecond)

    instance.repo.all(
      from(t in Timer,
        where: t.run_id == ^run_id and t.fired == false and t.fires_at <= ^window_end,
        select: %{id: t.id, run_id: t.run_id, fires_at: t.fires_at}
      )
    )
  end

  defp due_entries(table) do
    now_ms = System.system_time(:millisecond)

    table
    |> :ets.tab2list()
    |> Enum.filter(fn {_timer_id, _run_id, fires_at_ms} -> fires_at_ms <= now_ms end)
    |> Enum.sort_by(fn {_timer_id, _run_id, fires_at_ms} -> fires_at_ms end)
  end

  defp purge_resolved_cached_timers(_state, []), do: :ok

  defp purge_resolved_cached_timers(state, timer_ids) do
    pending =
      state.instance.repo.all(
        from(t in Timer,
          where: t.id in ^timer_ids and t.fired == false,
          select: t.id
        )
      )
      |> MapSet.new()

    timer_ids
    |> Enum.reject(&MapSet.member?(pending, &1))
    |> Enum.each(&:ets.delete(state.table, &1))
  end

  defp schedule_next_tick(state) do
    cancel_timer(state.tick_ref)

    tick_ref =
      case earliest_cached_timer_ms(state.table) do
        nil ->
          nil

        fires_at_ms ->
          Process.send_after(self(), :tick, tick_delay_ms(fires_at_ms))
      end

    %{state | tick_ref: tick_ref}
  end

  defp schedule_refresh(%{enabled?: false} = state), do: state

  defp schedule_refresh(state) do
    cancel_timer(state.refresh_ref)
    %{state | refresh_ref: Process.send_after(self(), :refresh, state.refresh_ms)}
  end

  defp earliest_cached_timer_ms(table) do
    table
    |> :ets.tab2list()
    |> Enum.map(fn {_timer_id, _run_id, fires_at_ms} -> fires_at_ms end)
    |> Enum.min(fn -> nil end)
  end

  defp tick_delay_ms(fires_at_ms) do
    delay_ms = fires_at_ms - System.system_time(:millisecond)
    if delay_ms <= 0, do: @due_retry_ms, else: delay_ms
  end

  defp start_listener(%{listen?: false} = state), do: state
  defp start_listener(%{instance: %{repo: nil}} = state), do: state

  defp start_listener(state) do
    case Postgrex.Notifications.start_link(state.instance.repo.config()) do
      {:ok, notifier} ->
        {:ok, ref} = Postgrex.Notifications.listen(notifier, "continuum_timer_armed")
        %{state | notifier: notifier, notify_ref: ref}

      {:error, reason} ->
        Logger.warning("TimerWheel notification listener failed: #{inspect(reason)}")
        state
    end
  end

  defp notified_run_id(payload) when is_binary(payload) do
    case String.split(payload, "|", parts: 2) do
      [run_id, _fires_at] -> run_id
      _ -> nil
    end
  end

  defp notified_run_id(_payload), do: nil

  defp timer_config do
    case Application.get_env(:continuum, :timer_wheel, []) do
      false -> [enabled?: false]
      true -> [enabled?: true]
      opts when is_list(opts) -> opts
    end
  end

  defp timer_enabled?(instance) do
    instance.repo != nil
  end

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(ref) do
    Process.cancel_timer(ref)
    :ok
  end

  defp date_time_to_ms(%DateTime{} = date_time), do: DateTime.to_unix(date_time, :millisecond)

  defp date_time_to_ms(%NaiveDateTime{} = naive) do
    naive
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_unix(:millisecond)
  end
end
