defmodule Continuum.Runtime.ScheduleRunner do
  @moduledoc false

  use GenServer
  require Logger

  import Ecto.Query

  alias Continuum.Runtime.{Engine, Instance}
  alias Continuum.Schema.{Run, Schedule}

  @default_interval_ms 1_000
  @default_batch_size 25
  @stale_claim_seconds 30

  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    GenServer.start_link(__MODULE__, opts, name: instance.schedule_runner)
  end

  @doc false
  @spec dispatch_once(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def dispatch_once(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with {:ok, schedules} <- claim(instance, batch_size) do
      Enum.each(schedules, &start_schedule(instance, &1))
      {:ok, length(schedules)}
    end
  end

  @impl true
  def init(opts) do
    state = %{
      instance: Instance.lookup(Keyword.get(opts, :instance, Continuum)),
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size)
    }

    schedule_poll(0)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    case dispatch_once(instance: state.instance, batch_size: state.batch_size) do
      {:ok, _count} -> :ok
      {:error, reason} -> Logger.warning("Continuum schedule dispatch failed: #{inspect(reason)}")
    end

    schedule_poll(state.interval_ms)
    {:noreply, state}
  end

  defp claim(%Instance{repo: nil}, _batch_size), do: {:error, :repo_not_configured}

  defp claim(instance, batch_size) when is_integer(batch_size) and batch_size > 0 do
    sql = """
    WITH candidates AS (
      SELECT id
      FROM continuum_schedules
      WHERE scheduled_at <= clock_timestamp()
        AND (
          state = 'scheduled'
          OR (state = 'starting' AND claimed_at < now() - make_interval(secs => $1))
        )
      ORDER BY scheduled_at, id
      FOR UPDATE SKIP LOCKED
      LIMIT $2
    )
    UPDATE continuum_schedules AS schedule
    SET state = 'starting',
        attempt = schedule.attempt + 1,
        claimed_at = clock_timestamp()
    FROM candidates
    WHERE schedule.id = candidates.id
    RETURNING schedule.id::text, schedule.run_id::text, schedule.workflow,
              schedule.version_hash, schedule.input, schedule.namespace,
              schedule.attributes, schedule.trace_context
    """

    case instance.repo.query(sql, [@stale_claim_seconds, min(batch_size, 1_000)]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &decode_claim/1)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp claim(_instance, batch_size), do: {:error, {:invalid_batch_size, batch_size}}

  defp decode_claim([
         id,
         run_id,
         workflow,
         version_hash,
         input,
         namespace,
         attributes,
         trace_context
       ]) do
    %{
      id: id,
      run_id: run_id,
      workflow: workflow,
      version_hash: version_hash,
      input: :erlang.binary_to_term(input),
      namespace: namespace,
      attributes: attributes || %{},
      trace_context: trace_context
    }
  end

  defp start_schedule(instance, schedule) do
    cond do
      instance.repo.exists?(from(r in Run, where: r.id == ^schedule.run_id)) ->
        mark_started(instance, schedule)

      true ->
        case Continuum.VersionRegistry.resolve(
               schedule.workflow,
               schedule.version_hash,
               instance
             ) do
          {:ok, %{entrypoint: entrypoint}} ->
            opts = [
              instance: instance,
              journal: Continuum.Runtime.Journal.Postgres,
              run_id: schedule.run_id,
              namespace: schedule.namespace,
              attributes: schedule.attributes,
              trace_context: schedule.trace_context,
              idempotency_key: "schedule:#{schedule.id}"
            ]

            case Engine.start_run(entrypoint, schedule.input, opts) do
              {:ok, _run_id} -> mark_started(instance, schedule)
              {:error, {:already_started, _run_id}} -> mark_started(instance, schedule)
              {:error, reason} -> retry_later(instance, schedule, reason)
            end

          {:error, reason} ->
            retry_later(instance, schedule, reason)
        end
    end
  rescue
    error -> retry_later(instance, schedule, error)
  end

  defp mark_started(instance, schedule) do
    instance.repo.update_all(
      from(s in Schedule, where: s.id == ^schedule.id and s.state == "starting"),
      set: [state: "started", started_at: DateTime.utc_now(), last_error: nil]
    )

    :telemetry.execute([:continuum, :schedule, :started], %{}, %{
      instance: instance.name,
      schedule_id: schedule.id,
      run_id: schedule.run_id
    })
  end

  defp retry_later(instance, schedule, reason) do
    instance.repo.update_all(
      from(s in Schedule, where: s.id == ^schedule.id and s.state == "starting"),
      set: [
        state: "scheduled",
        scheduled_at: DateTime.utc_now() |> DateTime.add(5, :second),
        last_error: inspect(reason, limit: 20, printable_limit: 2_000)
      ]
    )
  end

  defp schedule_poll(delay), do: Process.send_after(self(), :poll, delay)
end
