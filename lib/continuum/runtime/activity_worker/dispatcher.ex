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
  @default_backpressure_jitter_ms 250

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
      SELECT t.id, r.lease_token,
             GREATEST(
               0,
               FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - t.scheduled_at)) * 1000)
             )::bigint AS queue_age_ms
      FROM continuum_activity_tasks AS t
      JOIN continuum_runs AS r ON r.id = t.run_id
      WHERE t.id = $1::text::uuid
        AND t.state = 'available'
        AND t.attempt = $2
        AND t.available_at <= clock_timestamp()
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
              candidate.lease_token, candidate.queue_age_ms, t.queue, t.priority
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
    opts = Continuum.Config.validate_component!(:activity_dispatcher, opts)
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    config = Continuum.Config.validate_component!(:activity_dispatcher, worker_config())

    state = %{
      instance: instance,
      enabled?:
        Keyword.get(opts, :enabled?, Keyword.get(config, :enabled?, worker_enabled?(instance))),
      interval_ms:
        Keyword.get(opts, :interval_ms, Keyword.get(config, :interval_ms, @default_interval_ms)),
      batch_size:
        Keyword.get(opts, :batch_size, Keyword.get(config, :batch_size, @default_batch_size)),
      ttl_seconds:
        Keyword.get(opts, :ttl_seconds, Keyword.get(config, :ttl_seconds, @default_ttl_seconds)),
      backpressure_jitter_ms:
        Keyword.get(
          opts,
          :backpressure_jitter_ms,
          Keyword.get(config, :backpressure_jitter_ms, @default_backpressure_jitter_ms)
        )
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

    saturated? =
      state.instance.activity_executor == :builtin and available_capacity(state.instance) == 0

    delay = next_poll_delay(state.interval_ms, state.backpressure_jitter_ms, saturated?)
    schedule_poll(delay)
    {:noreply, state}
  end

  defp dispatch_builtin(instance, owner, batch_size, ttl_seconds) do
    active_tasks = active_worker_tasks(instance)
    active = length(active_tasks)
    capacity = max(instance.activity_max_concurrency - active, 0)
    claim_limit = min(batch_size, capacity)

    if claim_limit == 0 do
      emit_saturated(instance, owner, batch_size, active)
      emit_polled(instance, owner, batch_size, 0, :builtin, active, 0)
      {:ok, 0}
    else
      with {:ok, tasks} <-
             claim_queues(instance, owner, claim_limit, ttl_seconds, active_tasks) do
        {started, rejected} = start_workers(tasks)

        Enum.each(rejected, fn {task, reason} ->
          release_unstarted_task(task)
          emit_claim_rejected(instance, owner, task, reason)
        end)

        queue_age_ms = tasks |> Enum.map(&Map.get(&1, :queue_age_ms, 0)) |> Enum.max(fn -> 0 end)

        emit_polled(
          instance,
          owner,
          batch_size,
          started,
          :builtin,
          active + started,
          queue_age_ms
        )

        {:ok, started}
      end
    end
  end

  defp dispatch_oban(instance, owner, batch_size) do
    with {:ok, tasks} <- available_tasks(instance, batch_size),
         :ok <- enqueue_oban_tasks(instance, tasks) do
      emit_polled(instance, owner, batch_size, length(tasks), :oban, 0, 0)

      {:ok, length(tasks)}
    end
  end

  defp claim_queues(instance, owner, claim_limit, ttl_seconds, active_tasks) do
    active_by_queue = Enum.frequencies_by(active_tasks, &Map.get(&1, :queue, "default"))

    pending_queues(instance)
    |> Enum.reduce_while({:ok, [], claim_limit}, fn queue, {:ok, claimed, remaining} ->
      configured_limit =
        Map.get(instance.activity_queues || %{}, queue, instance.activity_max_concurrency)

      queue_capacity = max(configured_limit - Map.get(active_by_queue, queue, 0), 0)
      limit = min(queue_capacity, remaining)

      cond do
        remaining == 0 ->
          {:halt, {:ok, claimed, 0}}

        limit == 0 ->
          {:cont, {:ok, claimed, remaining}}

        true ->
          case claim_queue(instance, owner, queue, limit, ttl_seconds) do
            {:ok, tasks} -> {:cont, {:ok, claimed ++ tasks, remaining - length(tasks)}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
      end
    end)
    |> case do
      {:ok, tasks, _remaining} ->
        Telemetry.execute(
          [:continuum, :activity_dispatcher, :claimed],
          %{count: length(tasks)},
          %{instance: instance.name, owner: owner, batch_size: claim_limit}
        )

        {:ok, tasks}

      {:error, _reason} = error ->
        error
    end
  end

  defp pending_queues(instance) do
    sql = """
    SELECT t.queue
    FROM continuum_activity_tasks AS t
    JOIN continuum_runs AS r ON r.id = t.run_id
    WHERE t.state = 'available'
      AND t.available_at <= clock_timestamp()
      AND (t.lease_owner IS NULL OR t.lease_expires_at < now())
      AND r.state IN ('running', 'suspended')
      AND r.lease_token IS NOT NULL
      AND r.lease_expires_at > now()
    GROUP BY t.queue
    ORDER BY max(t.priority) DESC, min(t.available_at), t.queue
    """

    case instance.repo.query(sql) do
      {:ok, %{rows: rows}} -> Enum.map(rows, fn [queue] -> queue end)
      {:error, reason} -> raise "Activity queue discovery failed: #{inspect(reason)}"
    end
  end

  defp claim_queue(instance, owner, queue, batch_size, ttl_seconds) do
    sql = """
    WITH candidates AS (
      SELECT t.id, r.lease_token,
             GREATEST(
               0,
               FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - t.scheduled_at)) * 1000)
             )::bigint AS queue_age_ms
      FROM continuum_activity_tasks AS t
      JOIN continuum_runs AS r ON r.id = t.run_id
      WHERE t.state = 'available'
        AND t.queue = $4
        AND t.available_at <= clock_timestamp()
        AND (t.lease_owner IS NULL OR t.lease_expires_at < now())
        AND r.state IN ('running', 'suspended')
        AND r.lease_token IS NOT NULL
        AND r.lease_expires_at > now()
      ORDER BY t.priority DESC, t.available_at, t.scheduled_at, t.id
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
              candidates.lease_token, candidates.queue_age_ms, t.queue, t.priority
    """

    case instance.repo.query(sql, [owner, batch_size, ttl_seconds, queue]) do
      {:ok, %{rows: rows}} ->
        tasks =
          rows
          |> Enum.map(&decode_claim(instance, &1))
          |> Enum.map(&Map.put(&1, :executor, :builtin))

        {:ok, tasks}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp available_tasks(instance, batch_size) do
    sql = """
    SELECT t.id::text, t.attempt, t.queue, t.priority
    FROM continuum_activity_tasks AS t
    JOIN continuum_runs AS r ON r.id = t.run_id
    WHERE t.state = 'available'
      AND t.available_at <= clock_timestamp()
      AND (t.lease_owner IS NULL OR t.lease_expires_at < now())
      AND r.state IN ('running', 'suspended')
      AND r.lease_token IS NOT NULL
      AND r.lease_expires_at > now()
    ORDER BY t.priority DESC, t.available_at, t.scheduled_at, t.id
    LIMIT $1
    """

    case instance.repo.query(sql, [batch_size]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [id, attempt, queue, priority] ->
           %{id: id, attempt: attempt, queue: queue, priority: priority}
         end)}

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
         run_lease_token,
         queue_age_ms,
         queue,
         priority
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
        run_lease_token: run_lease_token,
        queue_age_ms: queue_age_ms,
        queue: queue,
        priority: priority
      })

    task
  end

  defp start_workers(tasks) do
    Enum.reduce(tasks, {0, []}, fn task, {started, rejected} ->
      case start_worker(task) do
        :ok -> {started + 1, rejected}
        {:error, reason} -> {started, [{task, reason} | rejected]}
      end
    end)
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
        {:error, reason}
    end
  end

  defp release_unstarted_task(task) do
    sql = """
    UPDATE continuum_activity_tasks
    SET state = 'available',
        available_at = clock_timestamp(),
        lease_owner = NULL,
        lease_expires_at = NULL
    WHERE id = $1::text::uuid
      AND state = 'leased'
      AND lease_owner = $2
      AND attempt = $3
    """

    case task.instance.repo.query(sql, [task.id, task.lease_owner, task.attempt]) do
      {:ok, _result} -> :ok
      {:error, reason} -> Logger.error("Activity claim release failed: #{inspect(reason)}")
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

  defp emit_polled(instance, owner, batch_size, count, executor, active, queue_age_ms) do
    Telemetry.execute(
      [:continuum, :activity_dispatcher, :polled],
      %{count: count, active: active, oldest_queue_age_ms: queue_age_ms},
      %{
        instance: instance.name,
        owner: owner,
        batch_size: batch_size,
        max_concurrency: instance.activity_max_concurrency,
        executor: executor
      }
    )
  end

  defp emit_saturated(instance, owner, batch_size, active) do
    {pending, queue_age_ms} = pending_queue_stats(instance)

    if pending > 0 do
      metadata = %{
        instance: instance.name,
        owner: owner,
        batch_size: batch_size,
        max_concurrency: instance.activity_max_concurrency,
        reason: :capacity
      }

      Telemetry.execute(
        [:continuum, :activity_dispatcher, :saturated],
        %{active: active, pending: pending, oldest_queue_age_ms: queue_age_ms},
        metadata
      )

      Telemetry.execute(
        [:continuum, :activity_dispatcher, :claim_rejected],
        %{count: pending},
        metadata
      )
    end
  end

  defp emit_claim_rejected(instance, owner, task, reason) do
    Telemetry.execute(
      [:continuum, :activity_dispatcher, :claim_rejected],
      %{count: 1},
      %{
        instance: instance.name,
        owner: owner,
        task_id: task.id,
        max_concurrency: instance.activity_max_concurrency,
        reason: reason
      }
    )
  end

  defp pending_queue_stats(instance) do
    sql = """
    SELECT COUNT(*)::bigint,
           COALESCE(
             MAX(
               GREATEST(
                 0,
                 FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - t.scheduled_at)) * 1000)
               )::bigint
             ),
             0
           )::bigint
    FROM continuum_activity_tasks AS t
    JOIN continuum_runs AS r ON r.id = t.run_id
    WHERE t.state = 'available'
      AND t.available_at <= clock_timestamp()
      AND r.state IN ('running', 'suspended')
      AND r.lease_token IS NOT NULL
      AND r.lease_expires_at > now()
    """

    case instance.repo.query(sql) do
      {:ok, %{rows: [[pending, queue_age_ms]]}} ->
        {pending, queue_age_ms}

      {:error, reason} ->
        Logger.warning("Activity queue stats failed: #{inspect(reason)}")
        {0, 0}
    end
  end

  defp active_workers(instance), do: length(active_worker_tasks(instance))

  defp active_worker_tasks(instance) do
    instance.activity_supervisor
    |> DynamicSupervisor.which_children()
    |> Enum.flat_map(fn
      {_id, pid, :worker, _modules} when is_pid(pid) ->
        instance.registry
        |> Registry.keys(pid)
        |> Enum.flat_map(fn
          {:activity_worker, task_id} = key ->
            case Registry.lookup(instance.registry, key) do
              [{^pid, metadata}] -> [Map.put(metadata, :id, task_id)]
              _other -> []
            end

          _other ->
            []
        end)

      _child ->
        []
    end)
  catch
    :exit, _reason -> []
  end

  defp available_capacity(instance) do
    max(instance.activity_max_concurrency - active_workers(instance), 0)
  end

  @doc false
  def next_poll_delay(interval_ms, jitter_ms, true)
      when is_integer(interval_ms) and interval_ms >= 0 and is_integer(jitter_ms) and
             jitter_ms > 0 do
    interval_ms + :rand.uniform(jitter_ms + 1) - 1
  end

  def next_poll_delay(interval_ms, _jitter_ms, _saturated?), do: interval_ms

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
