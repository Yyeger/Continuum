defmodule Continuum.Runtime.ActivityWorker.Dispatcher do
  @moduledoc """
  Polls `continuum_activity_tasks`, leases available tasks, and starts workers.
  """

  use GenServer
  require Logger

  alias Continuum.{Runtime.ActivityWorker.Worker, Runtime.Instance, Telemetry}

  @default_interval_ms 1_000
  @default_batch_size 10
  @default_ttl_seconds 30

  @doc false
  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    GenServer.start_link(__MODULE__, opts, name: instance.activity_dispatcher)
  end

  @doc """
  Run one activity dispatch pass synchronously.
  """
  @spec dispatch_once(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def dispatch_once(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    owner = Keyword.get_lazy(opts, :owner, fn -> owner(instance) end)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)

    with {:ok, tasks} <- claim(instance, owner, batch_size, ttl_seconds) do
      Enum.each(tasks, &start_worker/1)

      Telemetry.execute([:continuum, :activity_dispatcher, :polled], %{count: length(tasks)}, %{
        instance: instance.name,
        owner: owner,
        batch_size: batch_size
      })

      {:ok, length(tasks)}
    end
  end

  @impl true
  def init(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    config = worker_config()

    state = %{
      instance: instance,
      enabled?:
        Keyword.get(opts, :enabled?, Keyword.get(config, :enabled?, worker_enabled?(instance))),
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
    case dispatch_once(
           instance: state.instance,
           batch_size: state.batch_size,
           ttl_seconds: state.ttl_seconds
         ) do
      {:ok, _count} -> :ok
      {:error, reason} -> Logger.error("Activity dispatcher poll failed: #{inspect(reason)}")
    end

    schedule_poll(state.interval_ms)
    {:noreply, state}
  end

  defp claim(instance, owner, batch_size, ttl_seconds) do
    sql = """
    WITH candidates AS (
      SELECT t.id, r.lease_token
      FROM continuum_activity_tasks AS t
      JOIN continuum_runs AS r ON r.id = t.run_id
      WHERE t.state = 'available'
        AND t.available_at <= now()
        AND (t.lease_owner IS NULL OR t.lease_expires_at < now())
        AND r.state IN ('running', 'suspended')
        AND r.lease_token IS NOT NULL
        AND r.lease_expires_at > now()
      ORDER BY t.available_at, t.scheduled_at
      FOR UPDATE SKIP LOCKED
      LIMIT $2
    )
    UPDATE continuum_activity_tasks AS t
    SET state = 'leased',
        lease_owner = $1,
        lease_expires_at = now() + make_interval(secs => $3)
    FROM candidates
    WHERE t.id = candidates.id
    RETURNING t.id::text, t.run_id::text, t.seq, t.mfa, t.attempt, t.lease_owner,
              candidates.lease_token
    """

    case instance.repo.query(sql, [owner, batch_size, ttl_seconds]) do
      {:ok, %{rows: rows}} ->
        tasks = Enum.map(rows, &decode_claim(instance, &1))

        Telemetry.execute(
          [:continuum, :activity_dispatcher, :claimed],
          %{count: length(tasks)},
          %{
            instance: instance.name,
            owner: owner,
            batch_size: batch_size
          }
        )

        {:ok, tasks}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_claim(instance, [
         id,
         run_id,
         seq,
         encoded_task,
         attempt,
         lease_owner,
         run_lease_token
       ]) do
    task =
      encoded_task
      |> decode_term()
      |> Map.merge(%{
        id: id,
        run_id: run_id,
        instance: instance,
        seq: seq,
        attempt: attempt,
        lease_owner: lease_owner,
        run_lease_token: run_lease_token
      })

    task
  end

  defp start_worker(task) do
    case DynamicSupervisor.start_child(
           task.instance.activity_supervisor,
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

  defp worker_enabled?(instance) do
    instance.repo != nil
  end

  defp owner(instance) do
    "#{node()}/#{instance.name}/#{inspect(self())}:activity"
  end

  defp decode_term(binary) when is_binary(binary), do: :erlang.binary_to_term(binary)
  defp decode_term(other), do: other

  defp schedule_poll(interval_ms) do
    Process.send_after(self(), :poll, interval_ms)
  end
end
