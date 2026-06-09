defmodule Continuum.Runtime.ActivityWorker.Dispatcher do
  @moduledoc """
  Polls `continuum_activity_tasks`, leases available tasks, and starts workers.
  """

  use GenServer
  require Logger

  alias Continuum.{
    Oban,
    Runtime.ActivityWorker.Worker,
    Runtime.Instance,
    Runtime.Recovery,
    Telemetry
  }

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

    requeue_expired(instance)

    case instance.activity_executor do
      :builtin ->
        dispatch_builtin(instance, owner, batch_size, ttl_seconds)

      {:oban, _opts} ->
        dispatch_oban(instance, owner, batch_size)
    end
  end

  # Boot-time recovery only runs once per node; without this sweep a task
  # whose worker died stays 'leased' forever on a long-lived node (the claim
  # queries only consider 'available' tasks).
  defp requeue_expired(instance) do
    case Recovery.recover_activity_tasks(instance) do
      {:ok, 0} ->
        :ok

      {:ok, count} ->
        Telemetry.execute(
          [:continuum, :activity_dispatcher, :requeued],
          %{count: count},
          %{instance: instance.name}
        )

        :ok

      {:error, reason} ->
        Logger.warning("Activity task requeue sweep failed: #{inspect(reason)}")
        :ok
    end
  end

  @doc false
  def claim_one(instance, task_id, expected_attempt, owner, ttl_seconds \\ @default_ttl_seconds) do
    instance = Instance.lookup(instance)

    sql = """
    WITH candidate AS (
      SELECT t.id, r.lease_token
      FROM continuum_activity_tasks AS t
      JOIN continuum_runs AS r ON r.id = t.run_id
      WHERE t.id = $1::text::uuid
        AND t.state = 'available'
        AND t.attempt = $2
        AND t.available_at <= now()
        AND (t.lease_owner IS NULL OR t.lease_expires_at < now())
        AND r.state IN ('running', 'suspended')
        AND r.lease_token IS NOT NULL
        AND r.lease_expires_at > now()
      FOR UPDATE SKIP LOCKED
    )
    UPDATE continuum_activity_tasks AS t
    SET state = 'leased',
        lease_owner = $3,
        lease_expires_at = now() + make_interval(secs => $4)
    FROM candidate
    WHERE t.id = candidate.id
    RETURNING t.id::text, t.run_id::text, t.seq, t.mfa, t.attempt, t.lease_owner,
              candidate.lease_token
    """

    case instance.repo.query(sql, [task_id, expected_attempt, owner, ttl_seconds]) do
      {:ok, %{rows: [row]}} ->
        {:ok, instance |> decode_claim(row) |> Map.put(:executor, :oban)}

      {:ok, %{rows: []}} ->
        classify_claim_miss(instance, task_id, expected_attempt)

      {:error, reason} ->
        {:error, reason}
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

  defp dispatch_builtin(instance, owner, batch_size, ttl_seconds) do
    with {:ok, tasks} <- claim(instance, owner, batch_size, ttl_seconds) do
      Enum.each(tasks, &start_worker/1)

      emit_polled(instance, owner, batch_size, length(tasks), :builtin)

      {:ok, length(tasks)}
    end
  end

  defp dispatch_oban(instance, owner, batch_size) do
    with {:ok, tasks} <- available_tasks(instance, batch_size),
         :ok <- enqueue_oban_tasks(instance, tasks) do
      emit_polled(instance, owner, batch_size, length(tasks), :oban)

      {:ok, length(tasks)}
    end
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
        tasks =
          rows
          |> Enum.map(&decode_claim(instance, &1))
          |> Enum.map(&Map.put(&1, :executor, :builtin))

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

  defp available_tasks(instance, batch_size) do
    sql = """
    SELECT t.id::text, t.attempt
    FROM continuum_activity_tasks AS t
    JOIN continuum_runs AS r ON r.id = t.run_id
    WHERE t.state = 'available'
      AND t.available_at <= now()
      AND (t.lease_owner IS NULL OR t.lease_expires_at < now())
      AND r.state IN ('running', 'suspended')
      AND r.lease_token IS NOT NULL
      AND r.lease_expires_at > now()
    ORDER BY t.available_at, t.scheduled_at
    LIMIT $1
    """

    case instance.repo.query(sql, [batch_size]) do
      {:ok, %{rows: rows}} ->
        {:ok, Enum.map(rows, fn [id, attempt] -> %{id: id, attempt: attempt} end)}

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

  defp enqueue_oban_tasks(instance, tasks) do
    Enum.reduce_while(tasks, :ok, fn task, :ok ->
      case Oban.enqueue(instance, task) do
        {:ok, _job} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp emit_polled(instance, owner, batch_size, count, executor) do
    Telemetry.execute([:continuum, :activity_dispatcher, :polled], %{count: count}, %{
      instance: instance.name,
      owner: owner,
      batch_size: batch_size,
      executor: executor
    })
  end

  defp classify_claim_miss(instance, task_id, expected_attempt) do
    sql = """
    SELECT state, attempt
    FROM continuum_activity_tasks
    WHERE id = $1::text::uuid
    """

    case instance.repo.query(sql, [task_id]) do
      {:ok, %{rows: []}} ->
        :not_available

      {:ok, %{rows: [[_state, attempt]]}} when attempt != expected_attempt ->
        :stale

      {:ok, %{rows: [[_state, _attempt]]}} ->
        :not_available

      {:error, reason} ->
        {:error, reason}
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
