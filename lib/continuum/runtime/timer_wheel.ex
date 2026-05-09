defmodule Continuum.Runtime.TimerWheel do
  @moduledoc """
  Polls durable timers and wakes runs when timers fire.

  v0.1 keeps the durable source of truth in Postgres and uses a lightweight
  GenServer poller. The ETS-backed near-term cache described in the roadmap can
  be layered on this module without changing the timer event contract.
  """

  use GenServer
  require Logger

  alias Continuum.{Runtime.Engine, Runtime.Instance, Runtime.Journal, Telemetry}

  @default_interval_ms 1_000
  @default_batch_size 50

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

  @impl true
  def init(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    config = timer_config()

    state = %{
      instance: instance,
      enabled?:
        Keyword.get(opts, :enabled?, Keyword.get(config, :enabled?, timer_enabled?(instance))),
      interval_ms:
        Keyword.get(opts, :interval_ms, Keyword.get(config, :interval_ms, @default_interval_ms)),
      batch_size:
        Keyword.get(opts, :batch_size, Keyword.get(config, :batch_size, @default_batch_size))
    }

    if state.enabled?, do: schedule_poll(0)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    case fire_due_once(instance: state.instance, batch_size: state.batch_size) do
      {:ok, _count} -> :ok
      {:error, reason} -> Logger.error("TimerWheel poll failed: #{inspect(reason)}")
    end

    schedule_poll(state.interval_ms)
    {:noreply, state}
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

  defp fire_timer(instance, timer) do
    :ok = Journal.Postgres.fire_timer!(instance, timer.run_id, timer.id, timer.lease_token)
    Engine.wake(instance, timer.run_id)

    Telemetry.execute([:continuum, :timer, :fired], %{}, %{
      instance: instance.name,
      run_id: timer.run_id,
      timer_id: timer.id
    })
  end

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

  defp schedule_poll(interval_ms) do
    Process.send_after(self(), :poll, interval_ms)
  end
end
