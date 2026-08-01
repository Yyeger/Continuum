defmodule Continuum.Runtime.ActivityWorker do
  @moduledoc false

  require Logger

  alias Continuum.{DurableTerm, Runtime.Engine, Runtime.Journal, Telemetry}
  alias Continuum.Activity.Context, as: ActivityContext

  @max_error_bytes 65_536
  @max_error_text_bytes 4_096
  @max_stacktrace_entries 20
  @max_heartbeat_bytes 16_384

  @doc false
  def record_heartbeat(%ActivityContext{} = context, details) do
    DurableTerm.validate!(details, :activity_heartbeat)
    encoded = :erlang.term_to_binary(details)

    if byte_size(encoded) > @max_heartbeat_bytes do
      raise ArgumentError,
            "activity heartbeat details exceed #{@max_heartbeat_bytes} encoded bytes"
    end

    sql = """
    UPDATE continuum_activity_tasks AS task
    SET last_heartbeat_at = clock_timestamp(),
        heartbeat_details = $5
    FROM continuum_runs AS run
    WHERE task.id = $1::text::uuid
      AND task.run_id = $2::text::uuid
      AND task.state = 'leased'
      AND task.lease_owner = $3
      AND task.attempt = $4
      AND run.id = task.run_id
      AND run.state IN ('running', 'suspended')
      AND run.cancel_requested_at IS NULL
    """

    case context.instance.repo.query(sql, [
           context.task_id,
           context.run_id,
           context.lease_owner,
           context.attempt,
           encoded
         ]) do
      {:ok, %{num_rows: 1}} ->
        Telemetry.execute(
          [:continuum, :activity, :heartbeat],
          %{encoded_bytes: byte_size(encoded)},
          %{
            run_id: context.run_id,
            task_id: context.task_id,
            attempt: context.attempt
          }
        )

        :ok

      {:ok, %{num_rows: 0}} ->
        case activity_context_state(context) do
          :cancelled -> {:error, :cancelled}
          _other -> {:error, :lease_lost}
        end

      {:error, reason} ->
        raise "Continuum activity heartbeat persistence failed: #{inspect(reason)}"
    end
  end

  @doc false
  def activity_cancelled?(%ActivityContext{} = context) do
    activity_context_state(context) != :active
  rescue
    error ->
      Logger.warning(
        "Continuum activity cancellation check failed for #{context.task_id}: " <>
          Exception.message(error)
      )

      true
  end

  defp activity_context_state(context) do
    sql = """
    SELECT task.state, task.lease_owner, task.attempt,
           run.state, run.cancel_requested_at
    FROM continuum_activity_tasks AS task
    LEFT JOIN continuum_runs AS run ON run.id = task.run_id
    WHERE task.id = $1::text::uuid
      AND task.run_id = $2::text::uuid
    """

    case context.instance.repo.query(sql, [context.task_id, context.run_id]) do
      {:ok,
       %{
         rows: [
           ["leased", owner, attempt, run_state, cancel_requested_at]
         ]
       }}
      when owner == context.lease_owner and attempt == context.attempt ->
        if run_state in ["running", "suspended"] and is_nil(cancel_requested_at),
          do: :active,
          else: :cancelled

      {:ok, %{rows: [_row]}} ->
        :lease_lost

      {:ok, %{rows: []}} ->
        :lease_lost

      {:error, reason} ->
        raise "Continuum activity cancellation check failed: #{inspect(reason)}"
    end
  end

  def execute(task) do
    started_at = System.monotonic_time(:millisecond)

    case renew_task_lease(task) do
      :ok ->
        heartbeat = start_task_lease_heartbeat(task)

        try do
          fenced(task, fn ->
            if task.attempt > max_attempts(task.retry) do
              # Only crash requeues push attempt past the policy: the previous
              # execution died mid-flight and consumed the final attempt. Fail
              # without re-executing rather than re-running a poison task (or a
              # non-retryable side effect) forever.
              fail(task, :attempts_exhausted, started_at)
            else
              emit_started(task)

              case idempotency_hit(task) do
                {:hit, result} ->
                  complete(task, result, started_at, idempotency_hit?: true)

                :miss ->
                  case run_activity(task) do
                    {:ok, result} -> complete_durable(task, result, started_at)
                    {:error, error} -> fail_or_retry(task, error, started_at)
                  end
              end
            end
          end)
        after
          stop_task_lease_heartbeat(heartbeat)
        end

        :ok

      :lost ->
        Logger.warning(
          "Continuum activity task #{task.id} lease no longer held by #{task.lease_owner}; skipping execution"
        )

        :ok
    end
  end

  defp complete_durable(task, result, started_at) do
    case DurableTerm.validate(result, :activity_result) do
      :ok -> complete(task, result, started_at)
      {:error, error} -> fail(task, error, started_at)
    end
  end

  # The dispatcher claims tasks with a short TTL that only needs to cover the
  # claim-to-execution window. The activity may legally run for its full
  # configured timeout, and every completion/retry write validates task-lease
  # expiry — so the worker heartbeats the task lease on a short horizon while
  # executing. A crashed worker's task therefore expires within one TTL and
  # the steady-state sweep rescues it promptly, instead of waiting out a
  # one-shot timeout + margin extension (minutes for long activities).
  defp renew_task_lease(task) do
    sql = """
    UPDATE continuum_activity_tasks
    SET lease_expires_at = clock_timestamp() + make_interval(secs => $5)
    WHERE id = $1::text::uuid
      AND run_id = $2::text::uuid
      AND state = 'leased'
      AND lease_owner = $3
      AND attempt = $4
    """

    params = [
      task.id,
      task.run_id,
      task.lease_owner,
      task.attempt,
      task_lease_ttl_seconds()
    ]

    case task.instance.repo.query(sql, params) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        :lost

      {:error, reason} ->
        raise "Continuum activity task lease renewal failed: #{inspect(reason)}"
    end
  end

  # Public for tests only: the transient-vs-lost renewal classification is
  # exercised with a scripted repo, without a full claim/execute cycle.
  @doc false
  def start_task_lease_heartbeat(task) do
    worker = self()

    # Unlinked on purpose: a renewal hiccup must not kill the executing
    # worker. The monitor stops the heartbeat if the worker dies, so the
    # lease then expires within one TTL and the sweep rescues the task.
    spawn(fn ->
      ref = Process.monitor(worker)
      task_lease_heartbeat_loop(task, ref, task_lease_renew_ms())
    end)
  end

  defp stop_task_lease_heartbeat(pid) do
    send(pid, :stop)
    :ok
  end

  defp task_lease_heartbeat_loop(task, ref, delay_ms) do
    receive do
      :stop -> :ok
      {:DOWN, ^ref, :process, _pid, _reason} -> :ok
    after
      delay_ms ->
        case safe_renew_task_lease(task) do
          :ok -> task_lease_heartbeat_loop(task, ref, task_lease_renew_ms())
          :retry -> task_lease_heartbeat_loop(task, ref, task_lease_retry_ms())
          :lost -> :ok
        end
    end
  end

  defp safe_renew_task_lease(task) do
    renew_task_lease(task)
  rescue
    error ->
      # Only a renewal CAS miss (`:lost`) is authoritative — the lease is gone
      # and renewing again can never succeed. A raised DB error is transient:
      # stopping the heartbeat on one blip would expire a healthy long
      # activity's lease, and the sweep would then consume an attempt for an
      # execution that is still running. Retry on a shorter horizon instead;
      # the TTL leaves several renewal periods of slack to ride it out.
      Logger.warning(
        "Continuum task lease heartbeat for #{task.id} renewal failed (will retry): " <>
          Exception.message(error)
      )

      :retry
  end

  defp task_lease_ttl_seconds do
    Application.get_env(:continuum, :task_lease_ttl_seconds, 30)
  end

  defp task_lease_renew_ms do
    Application.get_env(:continuum, :task_lease_renew_ms, 10_000)
  end

  defp task_lease_retry_ms do
    max(div(task_lease_renew_ms(), 4), 250)
  end

  # Fencing rejections (run lease rotated, task lease expired or taken over,
  # run already terminal) are expected races, not worker bugs. Raising here
  # would strand the task in 'leased' — instead release it so the next claim
  # re-executes under the current authority, or discard it when the run can
  # no longer use the result. The rejection itself stays authoritative: the
  # journal write was rolled back, nothing of this attempt is visible.
  defp fenced(task, fun) do
    fun.()
  rescue
    error in Continuum.Runtime.JournalError ->
      case classify_fenced(error.reason) do
        :requeue -> release_fenced_task(task, error)
        :discard -> discard_fenced_task(task, error)
        :drop -> log_fenced(task, :dropped, error)
        :reraise -> reraise(error, __STACKTRACE__)
      end
  end

  defp classify_fenced(reason) do
    case reason do
      # Task lease still ours, only expired/incomplete: safe to re-execute.
      {:activity_task_lease_expired, _} -> :requeue
      {:activity_task_lease_missing_expiry, _} -> :requeue
      # Run lease rotated under us: the new engine still needs this activity.
      {:lease_mismatch, _} -> :requeue
      # Task no longer ours: another claimer owns it, leave it alone.
      {:activity_task_lease_mismatch, _} -> :drop
      {:activity_task_attempt_mismatch, _} -> :drop
      {:activity_task_not_leased, _} -> :drop
      {:activity_task_not_found, _} -> :drop
      {:activity_task_run_mismatch, _} -> :drop
      {_op, :task_lease_mismatch} -> :drop
      # Run is terminal or gone: the result has nowhere to land.
      {:run_not_found, _} -> :discard
      {:run_not_active, _} -> :discard
      _other -> :reraise
    end
  end

  defp release_fenced_task(task, error) do
    update_own_task(task, "available", "available_at = now(),")
    log_fenced(task, :requeued, error)
  end

  defp discard_fenced_task(task, error) do
    update_own_task(task, "discarded", "")
    log_fenced(task, :discarded, error)
  end

  # CAS on our own claim: if the task was reclaimed, completed, or cancelled
  # in the meantime, this matches zero rows and the row is left untouched.
  defp update_own_task(task, state, extra_set_sql) do
    sql = """
    UPDATE continuum_activity_tasks
    SET state = $5,
        #{extra_set_sql}
        lease_owner = NULL,
        lease_expires_at = NULL
    WHERE id = $1::text::uuid
      AND run_id = $2::text::uuid
      AND state = 'leased'
      AND lease_owner = $3
      AND attempt = $4
    """

    case task.instance.repo.query(sql, [
           task.id,
           task.run_id,
           task.lease_owner,
           task.attempt,
           state
         ]) do
      {:ok, _} -> :ok
      {:error, reason} -> raise "Continuum fenced task release failed: #{inspect(reason)}"
    end
  end

  defp log_fenced(task, action, error) do
    Logger.warning(
      "Continuum activity task #{task.id} (run #{task.run_id}) write was fenced out; " <>
        "#{action}: #{Exception.message(error)}"
    )

    Telemetry.execute([:continuum, :activity, :fenced], %{}, %{
      run_id: task.run_id,
      task_id: task.id,
      attempt: task.attempt,
      executor: executor(task),
      action: action
    })

    :ok
  end

  defp idempotency_hit(task) do
    case idempotency(task) do
      nil ->
        :miss

      [module: module, key: key] ->
        case Journal.Postgres.get_activity_result(task.instance, module, key) do
          {:ok, result} -> {:hit, result}
          :miss -> :miss
        end
    end
  end

  defp run_activity(%{mfa: {mod, fun, args}, timeout_ms: timeout_ms} = task) do
    parent = self()
    ref = make_ref()

    args =
      if Map.get(task, :context?, false), do: [ActivityContext.from_task(task) | args], else: args

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        result =
          try do
            {:ok, apply(mod, fun, args)}
          rescue
            exception -> {:error, {:activity_exception, exception, __STACKTRACE__}}
          catch
            kind, reason -> {:error, {:activity_catch, kind, reason, __STACKTRACE__}}
          end

        send(parent, {ref, result})
      end)

    receive do
      {^ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        {:error, {:activity_exit, reason}}
    after
      timeout_ms ->
        Process.demonitor(monitor_ref, [:flush])
        Process.exit(pid, :kill)
        {:error, :timeout}
    end
  end

  defp complete(task, result, started_at, opts \\ []) do
    idempotency = idempotency(task)
    duration = System.monotonic_time(:millisecond) - started_at
    metadata = base_metadata(task)

    if compensation?(task) do
      :ok =
        Journal.Postgres.complete_compensation_task!(
          task.instance,
          task,
          result,
          task.run_lease_token,
          idempotency_opts(idempotency)
        )

      Engine.wake(task.instance, task.run_id)

      Telemetry.execute(
        [:continuum, :compensation, :completed],
        %{duration_ms: duration},
        metadata
      )
    else
      :ok =
        Journal.Postgres.complete_activity_task!(
          task.instance,
          task,
          result,
          task.run_lease_token,
          idempotency_opts(idempotency)
        )

      Engine.wake(task.instance, task.run_id)

      if Keyword.get(opts, :idempotency_hit?, false) do
        Telemetry.execute([:continuum, :activity, :idempotency_hit], %{}, %{
          run_id: metadata.run_id,
          task_id: metadata.task_id,
          mfa: task.mfa,
          attempt: metadata.attempt,
          executor: metadata.executor,
          idempotency_key: Keyword.fetch!(idempotency, :key)
        })
      end

      Telemetry.execute(
        [:continuum, :activity, :completed],
        %{duration_ms: duration},
        Map.put(metadata, :mfa, task.mfa)
      )
    end
  end

  defp fail_or_retry(task, error, started_at) do
    error = normalize_error(error)

    if task.attempt < max_attempts(task.retry) do
      retry(task, error, started_at)
    else
      fail(task, error, started_at)
    end
  end

  @doc false
  def normalize_error({:activity_exception, exception, stacktrace}) do
    durable_or_fallback(exception, :error, exception.__struct__, stacktrace)
  end

  def normalize_error({:activity_catch, kind, reason, stacktrace})
      when kind in [:throw, :exit] do
    durable_or_fallback({kind, reason}, kind, nil, stacktrace)
  end

  def normalize_error({:activity_exit, reason}) do
    durable_or_fallback({:exit, reason}, :exit, nil, [])
  end

  def normalize_error(error) do
    durable_or_fallback(error, :error, error_class(error), [])
  end

  defp durable_or_fallback(error, kind, class, stacktrace) do
    with :ok <- DurableTerm.validate(error, :activity_error),
         true <- byte_size(:erlang.term_to_binary(error)) <= @max_error_bytes do
      error
    else
      _ -> fallback_error(error, kind, class, stacktrace)
    end
  rescue
    _ -> fallback_error(error, kind, class, stacktrace)
  end

  defp fallback_error(error, kind, class, stacktrace) do
    %Continuum.ActivityError{
      kind: kind,
      class: if(class, do: inspect(class), else: nil),
      message: error_message(error, kind),
      reason: bounded_inspect(error),
      stacktrace:
        stacktrace
        |> Enum.take(@max_stacktrace_entries)
        |> Enum.map(&bounded_stacktrace_entry/1)
    }
  end

  defp error_message(error, :error) when is_exception(error) do
    error
    |> Exception.message()
    |> truncate(@max_error_text_bytes)
  rescue
    _ -> "activity raised an unserializable exception"
  end

  defp error_message(_error, kind), do: "activity #{kind} contained non-durable data"

  defp bounded_inspect(error) do
    error
    |> inspect(limit: 20, printable_limit: @max_error_text_bytes, width: 80)
    |> truncate(@max_error_text_bytes)
  rescue
    _ -> "#<uninspectable activity error>"
  end

  defp bounded_stacktrace_entry(entry) do
    entry
    |> Exception.format_stacktrace_entry()
    |> truncate(@max_error_text_bytes)
  rescue
    _ -> "#<uninspectable stacktrace entry>"
  end

  defp truncate(value, max_bytes) when byte_size(value) <= max_bytes, do: value
  defp truncate(value, max_bytes), do: binary_part(value, 0, max_bytes) <> "..."

  defp error_class(error) when is_exception(error), do: error.__struct__
  defp error_class(_error), do: nil

  defp retry(task, error, started_at) do
    backoff_ms = backoff_ms(task.retry, task.attempt)

    # The authoritative available_at is computed on the database clock inside
    # retry_activity_task!; this app-clock timestamp is observability metadata.
    retry_at =
      DateTime.utc_now()
      |> DateTime.add(backoff_ms, :millisecond)
      |> DateTime.truncate(:microsecond)

    :ok =
      Journal.Postgres.retry_activity_task!(
        task.instance,
        task,
        error,
        backoff_ms,
        task.run_lease_token
      )

    Telemetry.execute(
      [:continuum, :activity, :retried],
      %{duration_ms: System.monotonic_time(:millisecond) - started_at},
      %{
        run_id: task.run_id,
        task_id: task.id,
        mfa: task.mfa,
        attempt: task.attempt,
        executor: executor(task),
        next_attempt: task.attempt + 1,
        retry_at: retry_at,
        error: error
      }
    )
  end

  defp fail(task, error, started_at) do
    duration = System.monotonic_time(:millisecond) - started_at
    metadata = base_metadata(task)

    if compensation?(task) do
      :ok =
        Journal.Postgres.fail_compensation_task!(task.instance, task, error, task.run_lease_token)

      Engine.wake(task.instance, task.run_id)

      Telemetry.execute(
        [:continuum, :compensation, :failed],
        %{duration_ms: duration},
        Map.put(metadata, :error, error)
      )
    else
      :ok = Journal.Postgres.fail_activity_task!(task.instance, task, error, task.run_lease_token)
      Engine.wake(task.instance, task.run_id)

      Telemetry.execute([:continuum, :activity, :failed], %{duration_ms: duration}, %{
        run_id: metadata.run_id,
        task_id: metadata.task_id,
        mfa: task.mfa,
        attempt: metadata.attempt,
        executor: metadata.executor,
        error: error
      })
    end
  end

  defp compensation?(task), do: Map.get(task, :kind) == :compensation

  defp emit_started(task) do
    metadata = base_metadata(task)

    if compensation?(task) do
      Telemetry.execute([:continuum, :compensation, :started], %{}, metadata)
    else
      Telemetry.execute([:continuum, :activity, :started], %{}, %{
        run_id: metadata.run_id,
        task_id: metadata.task_id,
        mfa: task.mfa,
        attempt: metadata.attempt,
        executor: metadata.executor
      })
    end
  end

  defp base_metadata(task) do
    metadata =
      %{
        run_id: task.run_id,
        task_id: task.id,
        attempt: task.attempt,
        executor: executor(task)
      }
      |> maybe_put(:oban_job_id, Map.get(task, :oban_job_id))

    if compensation?(task) do
      Map.put(metadata, :target_activity_id, task.target_activity_id)
    else
      metadata
    end
  end

  defp executor(task), do: Map.get(task, :executor, :builtin)

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp max_attempts(retry) do
    Keyword.get(retry || [], :max_attempts, 1)
  end

  defp idempotency(%{idempotency_key: nil}), do: nil

  defp idempotency(%{idempotency_key: key, mfa: {module, _fun, _args}}) do
    [module: module, key: key]
  end

  defp idempotency(_task), do: nil

  defp idempotency_opts(nil), do: []
  defp idempotency_opts(idempotency), do: [idempotency: idempotency]

  defp backoff_ms(retry, attempt) do
    Continuum.Activity.Policy.backoff_ms(retry, attempt)
  end
end
