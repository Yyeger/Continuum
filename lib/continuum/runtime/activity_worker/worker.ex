defmodule Continuum.Runtime.ActivityWorker.Worker do
  @moduledoc """
  Executes one leased activity task.
  """

  use GenServer

  import Ecto.Query

  alias Continuum.{Runtime.Engine, Runtime.Journal, Telemetry}
  alias Continuum.Schema.{ActivityTask, Run}

  @doc false
  def start_link(task) do
    GenServer.start_link(__MODULE__, task)
  end

  @doc false
  def child_spec(task) do
    %{
      id: {__MODULE__, task.id},
      start: {__MODULE__, :start_link, [task]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(task) do
    {:ok, task, {:continue, :run}}
  end

  @impl true
  def handle_continue(:run, task) do
    started_at = System.monotonic_time(:millisecond)

    Telemetry.execute([:continuum, :activity, :started], %{}, %{
      run_id: task.run_id,
      task_id: task.id,
      mfa: task.mfa,
      attempt: task.attempt
    })

    case run_activity(task) do
      {:ok, result} -> complete(task, result, started_at)
      {:error, error} -> fail_or_retry(task, error, started_at)
    end

    {:stop, :normal, task}
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
            exception -> {:error, {exception.__struct__, Exception.message(exception)}}
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

  defp complete(task, result, started_at) do
    event = %{
      type: :activity_completed,
      mfa: task.mfa,
      payload: result,
      seq: nil
    }

    :ok = Journal.Postgres.append!(task.run_id, event, run_lease_token(task.run_id))

    repo().update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [state: "completed", result: encode_term(result)]
    )

    Engine.wake(task.run_id)

    Telemetry.execute(
      [:continuum, :activity, :completed],
      %{duration_ms: System.monotonic_time(:millisecond) - started_at},
      %{run_id: task.run_id, task_id: task.id, mfa: task.mfa, attempt: task.attempt}
    )
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

    repo().update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [
        state: "available",
        attempt: task.attempt + 1,
        available_at: retry_at,
        lease_owner: nil,
        lease_expires_at: nil,
        error: encode_term(error)
      ]
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
    event = %{
      type: :activity_failed,
      mfa: task.mfa,
      error: error,
      attempt: task.attempt,
      seq: nil
    }

    :ok = Journal.Postgres.append!(task.run_id, event, run_lease_token(task.run_id))

    repo().update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [state: "discarded", error: encode_term(error)]
    )

    Engine.wake(task.run_id)

    Telemetry.execute(
      [:continuum, :activity, :failed],
      %{duration_ms: System.monotonic_time(:millisecond) - started_at},
      %{run_id: task.run_id, task_id: task.id, mfa: task.mfa, attempt: task.attempt, error: error}
    )
  end

  defp max_attempts(retry) do
    Keyword.get(retry || [], :max_attempts, 1)
  end

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

  defp run_lease_token(run_id) do
    repo().one(from(r in Run, where: r.id == ^run_id, select: r.lease_token))
  end

  defp encode_term(nil), do: nil
  defp encode_term(term), do: %{__term__: Base.encode64(:erlang.term_to_binary(term))}

  defp repo do
    Application.fetch_env!(:continuum, :repo)
  end
end
