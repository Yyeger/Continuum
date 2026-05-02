defmodule Continuum.Runtime.Lease.Heartbeater do
  @moduledoc """
  Renews leases owned by local workflow engines.

  Engines register their acquired lease here. On each heartbeat, the process
  renews every registered lease with the same owner/token pair. If renewal
  fails because the row no longer matches, the engine is told to stop itself.
  """

  use GenServer
  require Logger

  alias Continuum.Runtime.Lease

  @default_interval_ms 10_000
  @default_ttl_seconds 30

  @doc false
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Track a lease for periodic renewal.
  """
  @spec track(Lease.t(), pid()) :: :ok
  def track(%Lease{} = lease, pid \\ self()) do
    GenServer.call(__MODULE__, {:track, lease, pid})
  end

  @doc """
  Stop tracking a run's lease.
  """
  @spec untrack(binary()) :: :ok
  def untrack(run_id) do
    GenServer.call(__MODULE__, {:untrack, run_id})
  end

  @doc """
  Renew all tracked leases immediately.

  This is mainly useful for deterministic tests and shutdown paths.
  """
  @spec renew_once() :: :ok
  def renew_once do
    GenServer.call(__MODULE__, :renew_once)
  end

  @impl true
  def init(opts) do
    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      ttl_seconds: Keyword.get(opts, :ttl_seconds, @default_ttl_seconds),
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

  defp renew_all(state) do
    Enum.reduce(state.leases, state, fn {run_id, entry}, acc ->
      case Lease.renew(run_id, entry.owner, entry.token, ttl_seconds: acc.ttl_seconds) do
        :ok ->
          acc

        {:error, :lost} ->
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
