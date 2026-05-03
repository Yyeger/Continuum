defmodule Continuum.Runtime.TimerWheel do
  @moduledoc """
  Polls durable timers and wakes runs when timers fire.

  v0.1 keeps the durable source of truth in Postgres and uses a lightweight
  GenServer poller. The ETS-backed near-term cache described in the roadmap can
  be layered on this module without changing the timer event contract.
  """

  use GenServer
  require Logger

  alias Continuum.Runtime.{Engine, Journal}
  alias Continuum.Schema.Run

  import Ecto.Query

  @default_interval_ms 1_000
  @default_batch_size 50

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Fire due timers once.
  """
  @spec fire_due_once(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def fire_due_once(opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with {:ok, timers} <- claim_due(batch_size) do
      Enum.each(timers, &fire_timer/1)
      {:ok, length(timers)}
    end
  end

  @impl true
  def init(opts) do
    config = timer_config()

    state = %{
      enabled?: Keyword.get(opts, :enabled?, Keyword.get(config, :enabled?, timer_enabled?())),
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
    case fire_due_once(batch_size: state.batch_size) do
      {:ok, _count} -> :ok
      {:error, reason} -> Logger.error("TimerWheel poll failed: #{inspect(reason)}")
    end

    schedule_poll(state.interval_ms)
    {:noreply, state}
  end

  defp claim_due(batch_size) do
    sql = """
    WITH candidates AS (
      SELECT t.id
      FROM continuum_timers AS t
      JOIN continuum_runs AS r ON r.id = t.run_id
      WHERE t.fired = false
        AND t.fires_at <= now()
        AND r.state = 'suspended'
        AND r.lease_token IS NOT NULL
      ORDER BY t.fires_at
      FOR UPDATE SKIP LOCKED
      LIMIT $1
    )
    UPDATE continuum_timers AS t
    SET fired = true
    FROM candidates
    WHERE t.id = candidates.id
    RETURNING t.id::text, t.run_id::text
    """

    case repo().query(sql, [batch_size]) do
      {:ok, %{rows: rows}} ->
        {:ok, Enum.map(rows, fn [id, run_id] -> %{id: id, run_id: run_id} end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fire_timer(timer) do
    lease_token = run_lease_token(timer.run_id)

    event = %{
      type: :timer_fired,
      timer_id: timer.id,
      seq: nil
    }

    :ok = Journal.Postgres.append!(timer.run_id, event, lease_token)
    :ok = Journal.Postgres.clear_next_wakeup!(timer.run_id, lease_token)
    Engine.wake(timer.run_id)
  end

  defp run_lease_token(run_id) do
    repo().one(from(r in Run, where: r.id == ^run_id, select: r.lease_token))
  end

  defp timer_config do
    case Application.get_env(:continuum, :timer_wheel, []) do
      false -> [enabled?: false]
      true -> [enabled?: true]
      opts when is_list(opts) -> opts
    end
  end

  defp timer_enabled? do
    Application.get_env(:continuum, :repo) != nil
  end

  defp repo do
    Application.fetch_env!(:continuum, :repo)
  end

  defp schedule_poll(interval_ms) do
    Process.send_after(self(), :poll, interval_ms)
  end
end
