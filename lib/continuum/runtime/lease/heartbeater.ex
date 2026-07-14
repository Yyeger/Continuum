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
  def track(instance, %Lease{} = lease, pid) do
    GenServer.call(instance.heartbeater, {:track, lease, pid})
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
  def drain(instance, timeout_ms \\ @default_drain_timeout_ms) do
    GenServer.call(
      instance.heartbeater,
      {:drain, timeout_ms},
      timeout_ms + @kill_timeout_ms + 1_000
    )
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    instance = Continuum.Runtime.Instance.lookup(Keyword.get(opts, :instance, Continuum))

    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      ttl_seconds: Keyword.get(opts, :ttl_seconds, @default_ttl_seconds),
      drain_timeout_ms: Keyword.get(opts, :drain_timeout_ms, @default_drain_timeout_ms),
      instance: instance.name,
      repo: instance.repo,
      leases: %{},
      refs: %{}
    }

    schedule_tick(state.interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:track, %Lease{} = lease, pid}, _from, state) do
    state = untrack_run(state, lease.run_id)
    ref = Process.monitor(pid)

    entry = %{
      owner: lease.owner,
      token: lease.token,
      pid: pid,
      ref: ref
    }

    state = %{
      state
      | leases: Map.put(state.leases, lease.run_id, entry),
        refs: Map.put(state.refs, ref, lease.run_id)
    }

    {:reply, :ok, state}
  end

  def handle_call({:untrack, run_id}, _from, state) do
    {:reply, :ok, untrack_run(state, run_id)}
  end

  def handle_call(:renew_once, _from, state) do
    {:reply, :ok, renew_all(state)}
  end

  def handle_call({:drain, timeout_ms}, _from, state) do
    {summary, state} = drain_tracked_leases(state, timeout_ms)
    {:reply, {:ok, summary}, state}
  end

  @impl true
  def handle_info(:heartbeat, state) do
    state = renew_all(state)
    schedule_tick(state.interval_ms)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.fetch(state.refs, ref) do
      {:ok, run_id} -> {:noreply, untrack_run(state, run_id)}
      :error -> {:noreply, state}
    end
  end

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

    releasable = Map.drop(leases, Map.keys(remaining_after_kill))

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
      case Lease.renew(run_id, entry.owner, entry.token,
             ttl_seconds: acc.ttl_seconds,
             repo: acc.repo
           ) do
        :ok ->
          acc

        {:ok, :cancel_requested} ->
          # A durable cancel request was recorded while no caller could reach
          # this engine; the owner honors it on its heartbeat.
          send(entry.pid, {:continuum_cancel_requested, run_id})
          acc

        {:error, :lost} ->
          Telemetry.execute([:continuum, :lease, :lost], %{}, %{
            instance: acc.instance,
            run_id: run_id,
            owner: entry.owner,
            lease_token: entry.token
          })

          send(entry.pid, {:continuum_lease_lost, run_id, entry.token})
          untrack_run(acc, run_id)

        {:error, reason} ->
          Logger.error("Lease renewal failed for #{run_id}: #{inspect(reason)}")
          acc
      end
    end)
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
