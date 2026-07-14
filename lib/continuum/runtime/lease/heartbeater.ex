defmodule Continuum.Runtime.Lease.Heartbeater do
  @moduledoc """
  Renews leases owned by local workflow engines.

  Engines register their acquired lease here. On each heartbeat, the process
  renews every registered lease with the same owner/token pair. If renewal
  fails because the row no longer matches, the engine is told to stop itself.
  """

  use GenServer
  require Logger

  alias Continuum.{Runtime.Dispatcher, Runtime.Engine, Runtime.Lease, Telemetry}

  @default_interval_ms 10_000
  @default_ttl_seconds 30
  @default_drain_timeout_ms 5_000
  @kill_timeout_ms 1_000

  @doc false
  def child_spec(opts) do
    drain_timeout_ms = Keyword.get(opts, :drain_timeout_ms, @default_drain_timeout_ms)

    %{
      id: {__MODULE__, Keyword.get(opts, :instance, Continuum)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: drain_timeout_ms + @kill_timeout_ms + 1_000,
      type: :worker
    }
  end

  @doc false
  def start_link(opts \\ []) do
    instance = Continuum.Runtime.Instance.lookup(Keyword.get(opts, :instance, Continuum))
    name = Keyword.get(opts, :name, instance.heartbeater)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Track a lease for periodic renewal.
  """
  def track(instance, %Lease{} = lease, pid, reservation \\ nil) do
    GenServer.call(instance.heartbeater, {:track, lease, pid, reservation})
  end

  @doc false
  def track_engine(instance, run_id, pid, reservation \\ nil) do
    GenServer.call(instance.heartbeater, {:track_engine, run_id, pid, reservation})
  end

  @doc false
  def reserve_claim(instance) do
    GenServer.call(instance.heartbeater, :reserve_claim)
  end

  @doc false
  def cancel_claim(instance, reservation) when is_reference(reservation) do
    GenServer.call(instance.heartbeater, {:cancel_claim, reservation})
  end

  @doc """
  Stop tracking a run's lease.
  """
  def untrack(instance, run_id) do
    GenServer.call(instance.heartbeater, {:untrack, run_id})
  end

  @doc """
  Renew all tracked leases immediately.

  This is mainly useful for deterministic tests and shutdown paths.
  """
  def renew_once(instance) do
    GenServer.call(instance.heartbeater, :renew_once)
  end

  @doc """
  Stop new run claims and hand off all locally tracked run leases.

  Engines get up to `timeout_ms` to finish their current workflow step and
  release voluntarily. Engines still busy at the deadline are stopped before
  their leases are fenced and released.
  """
  def drain(instance, timeout_ms \\ nil)

  def drain(_instance, timeout_ms)
      when not is_nil(timeout_ms) and (not is_integer(timeout_ms) or timeout_ms < 0) do
    {:error, {:invalid_timeout, timeout_ms}}
  end

  def drain(instance, timeout_ms) do
    timeout_ms = timeout_ms || instance.drain_timeout_ms || @default_drain_timeout_ms

    case Process.whereis(instance.heartbeater) do
      nil ->
        {:error, :runtime_not_started}

      _pid ->
        GenServer.call(
          instance.heartbeater,
          {:drain, timeout_ms},
          timeout_ms + @kill_timeout_ms + 250
        )
    end
  catch
    :exit, {:noproc, _} -> {:error, :runtime_not_started}
    :exit, {:timeout, _} -> {:error, :timeout}
  end

  @doc false
  def readiness(instance) do
    case Process.whereis(instance.heartbeater) do
      nil -> not_started_status(instance)
      _pid -> GenServer.call(instance.heartbeater, :readiness)
    end
  catch
    :exit, _ -> not_started_status(instance)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    instance = Continuum.Runtime.Instance.lookup(Keyword.get(opts, :instance, Continuum))

    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      ttl_seconds: Keyword.get(opts, :ttl_seconds, @default_ttl_seconds),
      drain_timeout_ms: Keyword.get(opts, :drain_timeout_ms, instance.drain_timeout_ms),
      instance: instance.name,
      repo: instance.repo,
      leases: %{},
      refs: %{},
      lifecycle: :ready,
      reservations: MapSet.new(),
      drain: nil,
      last_drain: nil
    }

    schedule_tick(state.interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(:reserve_claim, _from, %{lifecycle: :ready} = state) do
    reservation = make_ref()

    {:reply, {:ok, reservation},
     %{state | reservations: MapSet.put(state.reservations, reservation)}}
  end

  def handle_call(:reserve_claim, _from, state), do: {:reply, {:error, :not_ready}, state}

  def handle_call({:cancel_claim, reservation}, _from, state) do
    state = %{state | reservations: MapSet.delete(state.reservations, reservation)}
    {:reply, :ok, maybe_finish_drain(state)}
  end

  def handle_call({:track, %Lease{} = lease, pid, reservation}, _from, state) do
    entry = %{run_id: lease.run_id, owner: lease.owner, token: lease.token, durable?: true}
    do_handle_track(entry, pid, reservation, state)
  end

  def handle_call({:track_engine, run_id, pid, reservation}, _from, state) do
    entry = %{run_id: run_id, owner: nil, token: nil, durable?: false}
    do_handle_track(entry, pid, reservation, state)
  end

  def handle_call({:untrack, run_id}, _from, state) do
    {:reply, :ok, state |> untrack_run(run_id) |> maybe_finish_drain()}
  end

  def handle_call(:renew_once, _from, state) do
    {:reply, :ok, renew_all(state)}
  end

  def handle_call(:readiness, _from, state) do
    {:reply, readiness_status(state), state}
  end

  def handle_call({:drain, nil}, from, state) do
    handle_call({:drain, state.drain_timeout_ms}, from, state)
  end

  def handle_call({:drain, timeout_ms}, _from, state)
      when not is_integer(timeout_ms) or timeout_ms < 0 do
    {:reply, {:error, {:invalid_timeout, timeout_ms}}, state}
  end

  def handle_call({:drain, timeout_ms}, from, %{lifecycle: :ready} = state)
      when is_integer(timeout_ms) and timeout_ms >= 0 do
    state = begin_drain(state, from, timeout_ms)
    {:noreply, maybe_finish_drain(state)}
  end

  def handle_call({:drain, _timeout_ms}, from, %{lifecycle: :draining} = state) do
    drain = Map.update!(state.drain, :waiters, &[from | &1])
    {:noreply, %{state | drain: drain}}
  end

  def handle_call({:drain, _timeout_ms}, _from, state) do
    {:reply, {:ok, state.last_drain}, state}
  end

  @impl true
  def handle_info(:heartbeat, %{lifecycle: :ready} = state) do
    state = renew_all(state)
    schedule_tick(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(:heartbeat, state) do
    schedule_tick(state.interval_ms)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.fetch(state.refs, ref) do
      {:ok, run_id} ->
        {:noreply, state |> untrack_run(run_id) |> maybe_finish_drain()}

      :error ->
        {:noreply, release_worker_down(state, ref)}
    end
  end

  def handle_info(
        {:drain_release_result, drain_ref, released_count, release_failed_count},
        %{drain: %{ref: drain_ref} = drain} = state
      ) do
    demonitor_release_worker(drain.release_worker)

    drain = %{
      drain
      | phase: :released,
        release_worker: nil,
        released_count: released_count,
        release_failed_count: release_failed_count
    }

    {:noreply, finish_drain(%{state | drain: drain})}
  end

  def handle_info({:pause_dispatcher, drain_ref}, %{drain: %{ref: drain_ref}} = state) do
    pause_dispatcher_async(state, drain_ref)
    {:noreply, state}
  end

  def handle_info({:dispatcher_paused, drain_ref}, %{drain: %{ref: drain_ref} = drain} = state) do
    state = %{state | drain: %{drain | dispatcher_paused?: true}}
    {:noreply, maybe_finish_drain(state)}
  end

  def handle_info({:drain_deadline, drain_ref}, %{drain: %{ref: drain_ref} = drain} = state) do
    forced_run_ids = Map.keys(state.leases)
    Enum.each(state.leases, fn {_run_id, entry} -> Process.exit(entry.pid, :kill) end)

    drain = %{
      drain
      | phase: :killing,
        forced_run_ids: MapSet.union(drain.forced_run_ids, MapSet.new(forced_run_ids)),
        abandoned_claim_count: MapSet.size(state.reservations)
    }

    state = %{state | drain: drain, reservations: MapSet.new()}
    {:noreply, maybe_finish_drain(state)}
  end

  def handle_info(
        {:drain_hard_deadline, drain_ref},
        %{drain: %{ref: drain_ref}} = state
      ) do
    Enum.each(state.leases, fn {_run_id, entry} -> Process.exit(entry.pid, :kill) end)
    kill_release_worker(state.drain.release_worker)

    drain = %{
      state.drain
      | abandoned_claim_count: state.drain.abandoned_claim_count + MapSet.size(state.reservations)
    }

    {:noreply, finish_drain(%{state | drain: drain, reservations: MapSet.new()})}
  end

  def handle_info({:dispatcher_paused, _stale_ref}, state), do: {:noreply, state}
  def handle_info({:pause_dispatcher, _stale_ref}, state), do: {:noreply, state}

  def handle_info({:drain_release_result, _ref, _released, _failed}, state),
    do: {:noreply, state}

  def handle_info({:drain_deadline, _stale_ref}, state), do: {:noreply, state}
  def handle_info({:drain_hard_deadline, _stale_ref}, state), do: {:noreply, state}

  @impl true
  def terminate(reason, state) when reason in [:shutdown, :normal] do
    drain_tracked_leases(state, state.drain_timeout_ms)
    :ok
  end

  def terminate({:shutdown, _reason}, state) do
    drain_tracked_leases(state, state.drain_timeout_ms)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp drain_tracked_leases(state, timeout_ms) do
    :ok = Dispatcher.pause(Continuum.Runtime.Instance.lookup(state.instance))
    state = renew_all(state)
    leases = state.leases

    Telemetry.execute([:continuum, :lease, :drain_started], %{count: map_size(leases)}, %{
      instance: state.instance,
      timeout_ms: timeout_ms
    })

    Enum.each(leases, fn {_run_id, entry} -> Engine.drain(entry.pid) end)

    deadline = System.monotonic_time(:millisecond) + timeout_ms
    remaining = await_down(leases, deadline)
    forced_count = map_size(remaining)

    Enum.each(remaining, fn {_run_id, entry} -> Process.exit(entry.pid, :kill) end)

    remaining_after_kill =
      await_down(remaining, System.monotonic_time(:millisecond) + @kill_timeout_ms)

    releasable = leases |> Map.drop(Map.keys(remaining_after_kill)) |> durable_entries()

    {released_count, release_failed_count} =
      Enum.reduce(releasable, {0, 0}, fn {run_id, entry}, {released, failed} ->
        case Lease.release(run_id, entry.owner, entry.token, repo: state.repo) do
          :ok ->
            {released + 1, failed}

          {:error, :lost} ->
            {released, failed}

          {:error, reason} ->
            Logger.error("Lease release during drain failed for #{run_id}: #{inspect(reason)}")
            {released, failed + 1}
        end
      end)

    clear_state = clear_tracked_leases(state)

    summary = %{
      tracked_count: map_size(leases),
      graceful_count: map_size(leases) - forced_count,
      forced_count: forced_count,
      unreleased_count: map_size(remaining_after_kill) + release_failed_count,
      released_count: released_count
    }

    Telemetry.execute([:continuum, :lease, :drain_completed], summary, %{
      instance: state.instance
    })

    {summary, clear_state}
  end

  defp begin_drain(state, from, timeout_ms) do
    drain_ref = make_ref()
    deadline_timer = Process.send_after(self(), {:drain_deadline, drain_ref}, timeout_ms)

    hard_deadline_timer =
      Process.send_after(self(), {:drain_hard_deadline, drain_ref}, timeout_ms + @kill_timeout_ms)

    Telemetry.execute([:continuum, :lease, :drain_started], %{count: map_size(state.leases)}, %{
      instance: state.instance,
      timeout_ms: timeout_ms
    })

    Enum.each(state.leases, fn {_run_id, entry} -> Engine.drain(entry.pid) end)
    send(self(), {:pause_dispatcher, drain_ref})

    drain = %{
      ref: drain_ref,
      phase: :graceful,
      waiters: [from],
      entries: state.leases,
      forced_run_ids: MapSet.new(),
      abandoned_claim_count: 0,
      dispatcher_paused?: false,
      deadline_timer: deadline_timer,
      hard_deadline_timer: hard_deadline_timer,
      release_worker: nil,
      released_count: 0,
      release_failed_count: 0
    }

    %{state | lifecycle: :draining, drain: drain, last_drain: nil}
  end

  defp pause_dispatcher_async(state, drain_ref) do
    heartbeater = self()
    instance = Continuum.Runtime.Instance.lookup(state.instance)

    spawn(fn ->
      :ok = Dispatcher.pause(instance)
      send(heartbeater, {:dispatcher_paused, drain_ref})
    end)

    :ok
  end

  defp maybe_finish_drain(%{lifecycle: :draining, drain: drain} = state) do
    if drain.dispatcher_paused? and map_size(state.leases) == 0 and
         MapSet.size(state.reservations) == 0 do
      start_release_worker(state)
    else
      state
    end
  end

  defp maybe_finish_drain(state), do: state

  defp finish_drain(state) do
    drain = state.drain
    cancel_timer(drain.deadline_timer)
    cancel_timer(drain.hard_deadline_timer)
    kill_release_worker(drain.release_worker)

    active_run_count = map_size(state.leases)
    pending_release_count = pending_release_count(drain)
    forced_count = MapSet.size(drain.forced_run_ids)

    summary = %{
      tracked_count: map_size(drain.entries),
      graceful_count: max(map_size(drain.entries) - forced_count, 0),
      forced_count: forced_count,
      abandoned_claim_count: drain.abandoned_claim_count,
      unreleased_count: active_run_count + pending_release_count + drain.release_failed_count,
      released_count: drain.released_count,
      dispatcher_paused?: drain.dispatcher_paused?
    }

    lifecycle =
      if summary.unreleased_count == 0 and summary.dispatcher_paused? and
           summary.abandoned_claim_count == 0,
         do: :drained,
         else: :degraded

    Telemetry.execute([:continuum, :lease, :drain_completed], summary, %{
      instance: state.instance,
      lifecycle: lifecycle
    })

    Enum.each(drain.waiters, &GenServer.reply(&1, {:ok, summary}))

    state
    |> clear_tracked_leases()
    |> Map.merge(%{
      lifecycle: lifecycle,
      reservations: MapSet.new(),
      drain: nil,
      last_drain: summary
    })
  end

  defp start_release_worker(%{drain: %{phase: :releasing}} = state), do: state
  defp start_release_worker(%{drain: %{phase: :released}} = state), do: state
  defp start_release_worker(%{drain: %{release_worker: %{} = _worker}} = state), do: state

  defp start_release_worker(state) do
    entries = durable_entries(state.drain.entries)

    if map_size(entries) == 0 do
      finish_drain(%{state | drain: %{state.drain | phase: :released}})
    else
      heartbeater = self()
      drain_ref = state.drain.ref
      repo = state.repo

      {pid, monitor_ref} =
        spawn_monitor(fn ->
          {released_count, release_failed_count} = release_entries(repo, entries)

          send(
            heartbeater,
            {:drain_release_result, drain_ref, released_count, release_failed_count}
          )
        end)

      drain = %{
        state.drain
        | phase: :releasing,
          release_worker: %{pid: pid, ref: monitor_ref, count: map_size(entries)}
      }

      %{state | drain: drain}
    end
  end

  defp release_entries(repo, entries) do
    Enum.reduce(entries, {0, 0}, fn {run_id, entry}, {released, failed} ->
      case Lease.release(run_id, entry.owner, entry.token, repo: repo) do
        :ok ->
          {released + 1, failed}

        {:error, :lost} ->
          {released, failed}

        {:error, reason} ->
          Logger.error("Lease release during drain failed for #{run_id}: #{inspect(reason)}")
          {released, failed + 1}
      end
    end)
  end

  defp do_handle_track(entry, pid, reservation, state) do
    cond do
      state.lifecycle == :ready and valid_reservation?(state, reservation) ->
        state = state |> consume_reservation(reservation) |> track_run(entry, pid)
        {:reply, :ok, state}

      state.lifecycle == :draining and reservation_member?(state, reservation) ->
        state =
          state
          |> consume_reservation(reservation)
          |> track_run(entry, pid)
          |> add_drain_entry(entry.run_id)

        Engine.drain(pid)
        {:reply, :ok, state}

      state.lifecycle == :ready ->
        {:reply, {:error, :invalid_claim_reservation}, state}

      true ->
        {:reply, {:error, :not_ready}, state}
    end
  end

  defp track_run(state, entry, pid) do
    state = untrack_run(state, entry.run_id)
    ref = Process.monitor(pid)
    entry = Map.merge(entry, %{pid: pid, ref: ref})

    %{
      state
      | leases: Map.put(state.leases, entry.run_id, entry),
        refs: Map.put(state.refs, ref, entry.run_id)
    }
  end

  defp add_drain_entry(%{drain: drain} = state, run_id) do
    %{state | drain: %{drain | entries: Map.put(drain.entries, run_id, state.leases[run_id])}}
  end

  defp release_worker_down(%{drain: %{release_worker: %{ref: ref} = worker} = drain} = state, ref) do
    drain = %{
      drain
      | phase: :released,
        release_worker: nil,
        release_failed_count: drain.release_failed_count + worker.count
    }

    finish_drain(%{state | drain: drain})
  end

  defp release_worker_down(state, _ref), do: state

  defp durable_entries(entries) do
    Map.filter(entries, fn {_run_id, entry} -> entry.durable? end)
  end

  defp pending_release_count(%{phase: :released}), do: 0
  defp pending_release_count(%{release_worker: %{count: count}}), do: count
  defp pending_release_count(drain), do: map_size(durable_entries(drain.entries))

  defp demonitor_release_worker(nil), do: :ok

  defp demonitor_release_worker(%{ref: ref}) do
    Process.demonitor(ref, [:flush])
    :ok
  end

  defp kill_release_worker(nil), do: :ok

  defp kill_release_worker(%{pid: pid, ref: ref}) do
    Process.demonitor(ref, [:flush])
    Process.exit(pid, :kill)
    :ok
  end

  defp valid_reservation?(_state, nil), do: true
  defp valid_reservation?(state, reservation), do: reservation_member?(state, reservation)

  defp reservation_member?(state, reservation) when is_reference(reservation) do
    MapSet.member?(state.reservations, reservation)
  end

  defp reservation_member?(_state, _reservation), do: false

  defp consume_reservation(state, nil), do: state

  defp consume_reservation(state, reservation) do
    %{state | reservations: MapSet.delete(state.reservations, reservation)}
  end

  defp readiness_status(state) do
    %{
      instance: state.instance,
      state: state.lifecycle,
      ready?: state.lifecycle == :ready,
      drained?: state.lifecycle == :drained,
      active_run_count: map_size(state.leases),
      pending_claim_count: MapSet.size(state.reservations),
      last_drain: state.last_drain
    }
  end

  defp not_started_status(instance) do
    %{
      instance: instance.name,
      state: :not_started,
      ready?: false,
      drained?: false,
      active_run_count: 0,
      pending_claim_count: 0,
      last_drain: nil
    }
  end

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(ref) do
    Process.cancel_timer(ref)
    :ok
  end

  defp await_down(leases, _deadline) when map_size(leases) == 0, do: leases

  defp await_down(leases, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:DOWN, ref, :process, _pid, _reason} ->
        await_down(drop_ref(leases, ref), deadline)
    after
      timeout -> leases
    end
  end

  defp drop_ref(leases, ref) do
    leases
    |> Enum.reject(fn {_run_id, entry} -> entry.ref == ref end)
    |> Map.new()
  end

  defp clear_tracked_leases(state) do
    Enum.each(state.refs, fn {ref, _run_id} -> Process.demonitor(ref, [:flush]) end)
    %{state | leases: %{}, refs: %{}}
  end

  defp renew_all(state) do
    Enum.reduce(state.leases, state, fn {run_id, entry}, acc ->
      renew_entry(acc, run_id, entry)
    end)
  end

  defp renew_entry(state, _run_id, %{durable?: false}), do: state

  defp renew_entry(state, run_id, entry) do
    case Lease.renew(run_id, entry.owner, entry.token,
           ttl_seconds: state.ttl_seconds,
           repo: state.repo
         ) do
      :ok ->
        state

      {:ok, :cancel_requested} ->
        # A durable cancel request was recorded while no caller could reach
        # this engine; the owner honors it on its heartbeat.
        send(entry.pid, {:continuum_cancel_requested, run_id})
        state

      {:error, :lost} ->
        Telemetry.execute([:continuum, :lease, :lost], %{}, %{
          instance: state.instance,
          run_id: run_id,
          owner: entry.owner,
          lease_token: entry.token
        })

        send(entry.pid, {:continuum_lease_lost, run_id, entry.token})
        untrack_run(state, run_id)

      {:error, reason} ->
        Logger.error("Lease renewal failed for #{run_id}: #{inspect(reason)}")
        state
    end
  end

  defp untrack_run(state, run_id) do
    case Map.pop(state.leases, run_id) do
      {nil, _leases} ->
        state

      {%{ref: ref}, leases} ->
        Process.demonitor(ref, [:flush])
        %{state | leases: leases, refs: Map.delete(state.refs, ref)}
    end
  end

  defp schedule_tick(interval_ms) do
    Process.send_after(self(), :heartbeat, interval_ms)
  end
end
