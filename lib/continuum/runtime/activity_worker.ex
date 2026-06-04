defmodule Continuum.Runtime.ActivityWorker do
  @moduledoc false

  alias Continuum.{Runtime.Engine, Runtime.Journal, Telemetry}

  def execute(task) do
    started_at = System.monotonic_time(:millisecond)

    emit_started(task)

    case idempotency_hit(task) do
      {:hit, result} ->
        complete(task, result, started_at, idempotency_hit?: true)

      :miss ->
        case run_activity(task) do
          {:ok, result} -> complete(task, result, started_at)
          {:error, error} -> fail_or_retry(task, error, started_at)
        end
    end

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

  defp run_activity(%{mfa: {mod, fun, args}, timeout_ms: timeout_ms}) do
    parent = self()
    ref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        result =
          try do
            {:ok, apply(mod, fun, args)}
          rescue
            exception -> {:error, exception}
          catch
            kind, reason -> {:error, {kind, reason}}
          end

        send(parent, {ref, result})
      end)

    receive do
      {^ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        {:error, {:exit, reason}}
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

      Telemetry.execute([:continuum, :compensation, :completed], %{duration_ms: duration}, %{
        run_id: task.run_id,
        task_id: task.id,
        target_activity_id: task.target_activity_id,
        attempt: task.attempt
      })
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
          run_id: task.run_id,
          task_id: task.id,
          mfa: task.mfa,
          attempt: task.attempt,
          idempotency_key: Keyword.fetch!(idempotency, :key)
        })
      end

      Telemetry.execute([:continuum, :activity, :completed], %{duration_ms: duration}, %{
        run_id: task.run_id,
        task_id: task.id,
        mfa: task.mfa,
        attempt: task.attempt
      })
    end
  end

  defp fail_or_retry(task, error, started_at) do
    if task.attempt < max_attempts(task.retry) do
      retry(task, error, started_at)
    else
      fail(task, error, started_at)
    end
  end

  defp retry(task, error, started_at) do
    retry_at = retry_at(task)

    :ok =
      Journal.Postgres.retry_activity_task!(
        task.instance,
        task,
        error,
        retry_at,
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
        next_attempt: task.attempt + 1,
        retry_at: retry_at,
        error: error
      }
    )
  end

  defp fail(task, error, started_at) do
    duration = System.monotonic_time(:millisecond) - started_at

    if compensation?(task) do
      :ok =
        Journal.Postgres.fail_compensation_task!(task.instance, task, error, task.run_lease_token)

      Engine.wake(task.instance, task.run_id)

      Telemetry.execute([:continuum, :compensation, :failed], %{duration_ms: duration}, %{
        run_id: task.run_id,
        task_id: task.id,
        target_activity_id: task.target_activity_id,
        attempt: task.attempt,
        error: error
      })
    else
      :ok = Journal.Postgres.fail_activity_task!(task.instance, task, error, task.run_lease_token)
      Engine.wake(task.instance, task.run_id)

      Telemetry.execute([:continuum, :activity, :failed], %{duration_ms: duration}, %{
        run_id: task.run_id,
        task_id: task.id,
        mfa: task.mfa,
        attempt: task.attempt,
        error: error
      })
    end
  end

  defp compensation?(task), do: Map.get(task, :kind) == :compensation

  defp emit_started(task) do
    if compensation?(task) do
      Telemetry.execute([:continuum, :compensation, :started], %{}, %{
        run_id: task.run_id,
        task_id: task.id,
        target_activity_id: task.target_activity_id,
        attempt: task.attempt
      })
    else
      Telemetry.execute([:continuum, :activity, :started], %{}, %{
        run_id: task.run_id,
        task_id: task.id,
        mfa: task.mfa,
        attempt: task.attempt
      })
    end
  end

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

  defp retry_at(task) do
    DateTime.utc_now()
    |> DateTime.add(backoff_ms(task.retry, task.attempt), :millisecond)
    |> DateTime.truncate(:microsecond)
  end

  defp backoff_ms(retry, attempt) do
    retry = retry || []
    base_ms = Keyword.get(retry, :base_ms, 1_000)

    case Keyword.get(retry, :backoff, :constant) do
      :exponential -> trunc(base_ms * :math.pow(2, max(attempt - 1, 0)))
      _ -> base_ms
    end
  end
end
