defmodule Continuum.Runtime.ActivityWorker.Dispatcher do
  @moduledoc """
  Polls `continuum_activity_tasks`, leases available tasks, and starts workers.
  """

  use GenServer
  require Logger

  alias Continuum.Runtime.ActivityWorker.Worker

  @default_interval_ms 1_000
  @default_batch_size 10
  @default_ttl_seconds 30

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Run one activity dispatch pass synchronously.
  """
  @spec dispatch_once(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def dispatch_once(opts \\ []) do
    owner = Keyword.get_lazy(opts, :owner, &owner/0)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)

    with {:ok, tasks} <- claim(owner, batch_size, ttl_seconds) do
      Enum.each(tasks, &start_worker/1)
      {:ok, length(tasks)}
    end
  end

  @impl true
  def init(opts) do
    config = worker_config()

    state = %{
      enabled?: Keyword.get(opts, :enabled?, Keyword.get(config, :enabled?, worker_enabled?())),
      interval_ms:
        Keyword.get(opts, :interval_ms, Keyword.get(config, :interval_ms, @default_interval_ms)),
      batch_size:
        Keyword.get(opts, :batch_size, Keyword.get(config, :batch_size, @default_batch_size)),
      ttl_seconds:
        Keyword.get(opts, :ttl_seconds, Keyword.get(config, :ttl_seconds, @default_ttl_seconds))
    }

    if state.enabled?, do: schedule_poll(0)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    case dispatch_once(batch_size: state.batch_size, ttl_seconds: state.ttl_seconds) do
      {:ok, _count} -> :ok
      {:error, reason} -> Logger.error("Activity dispatcher poll failed: #{inspect(reason)}")
    end

    schedule_poll(state.interval_ms)
    {:noreply, state}
  end

  defp claim(owner, batch_size, ttl_seconds) do
    sql = """
    WITH candidates AS (
      SELECT id
      FROM continuum_activity_tasks
      WHERE state = 'available'
        AND available_at <= now()
        AND (lease_owner IS NULL OR lease_expires_at < now())
      ORDER BY available_at, scheduled_at
      FOR UPDATE SKIP LOCKED
      LIMIT $2
    )
    UPDATE continuum_activity_tasks AS t
    SET state = 'leased',
        lease_owner = $1,
        lease_expires_at = now() + make_interval(secs => $3)
    FROM candidates
    WHERE t.id = candidates.id
    RETURNING t.id::text, t.run_id::text, t.seq, t.mfa, t.attempt, t.lease_owner
    """

    case repo().query(sql, [owner, batch_size, ttl_seconds]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &decode_claim/1)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_claim([id, run_id, seq, encoded_task, attempt, lease_owner]) do
    task =
      encoded_task
      |> decode_term()
      |> Map.merge(%{
        id: id,
        run_id: run_id,
        seq: seq,
        attempt: attempt,
        lease_owner: lease_owner
      })

    task
  end

  defp start_worker(task) do
    case DynamicSupervisor.start_child(
           Continuum.Runtime.ActivityWorker.Supervisor,
           {Worker, task}
         ) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        Logger.error("Activity worker failed to start #{task.id}: #{inspect(reason)}")
        :error
    end
  end

  defp worker_config do
    case Application.get_env(:continuum, :activity_worker, []) do
      false -> [enabled?: false]
      true -> [enabled?: true]
      opts when is_list(opts) -> opts
    end
  end

  defp worker_enabled? do
    Application.get_env(:continuum, :repo) != nil
  end

  defp owner do
    "#{node()}:#{inspect(self())}:activity"
  end

  defp decode_term(%{"__term__" => encoded}) when is_binary(encoded) do
    :erlang.binary_to_term(Base.decode64!(encoded))
  end

  defp decode_term(other), do: other

  defp repo do
    Application.fetch_env!(:continuum, :repo)
  end

  defp schedule_poll(interval_ms) do
    Process.send_after(self(), :poll, interval_ms)
  end
end
