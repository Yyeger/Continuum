defmodule Continuum.Runtime.Recovery do
  @moduledoc """
  Boot-time recovery for orphaned durable work.

  Recovery is intentionally narrow in v0.1: it does not execute work itself.
  It makes abandoned rows eligible for the existing dispatcher, timer wheel,
  and activity worker pollers after a node restart.
  """

  use GenServer
  require Logger

  alias Continuum.{Runtime.Instance, Telemetry}

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      type: :worker
    }
  end

  @doc false
  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    config = recovery_config()

    enabled? =
      Keyword.get(opts, :enabled?, Keyword.get(config, :enabled?, recovery_enabled?(instance)))

    if enabled? do
      GenServer.start_link(__MODULE__, Keyword.put(opts, :instance, instance),
        name: Keyword.get(opts, :name, instance.recovery)
      )
    else
      :ignore
    end
  end

  @doc """
  Run one recovery scan.
  """
  @spec recover_once(keyword()) ::
          {:ok,
           %{
             runs: non_neg_integer(),
             activity_tasks: non_neg_integer(),
             timers: non_neg_integer()
           }}
          | {:error, term()}
  def recover_once(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    with {:ok, runs} <- recover_runs(instance),
         {:ok, activity_tasks} <- recover_activity_tasks(instance),
         {:ok, timers} <- recover_due_timers(instance) do
      Telemetry.execute([:continuum, :recovery, :completed], %{}, %{
        instance: instance.name,
        runs: runs,
        activity_tasks: activity_tasks,
        timers: timers
      })

      {:ok, %{runs: runs, activity_tasks: activity_tasks, timers: timers}}
    end
  end

  @impl true
  def init(opts) do
    {:ok, %{instance: Instance.lookup(Keyword.get(opts, :instance, Continuum))},
     {:continue, :recover}}
  end

  @impl true
  def handle_continue(:recover, state) do
    case recover_once(instance: state.instance) do
      {:ok, counts} ->
        Logger.info("Continuum recovery completed: #{inspect(counts)}")
        {:stop, :normal, state}

      {:error, reason} ->
        Logger.error("Continuum recovery failed: #{inspect(reason)}")
        {:stop, reason, state}
    end
  end

  defp recover_runs(instance) do
    local_run_ids = local_run_ids(instance)

    {skip_local_sql, params} =
      case local_run_ids do
        [] -> {"", []}
        ids -> {"AND id::text <> ALL($1::text[])", [ids]}
      end

    sql = """
    UPDATE continuum_runs
    SET state = CASE WHEN state = 'running' THEN 'suspended' ELSE state END,
        lease_owner = NULL,
        lease_token = NULL,
        lease_expires_at = NULL,
        next_wakeup_at = CASE
          WHEN state = 'running' AND next_wakeup_at IS NULL THEN now()
          ELSE next_wakeup_at
        END
    WHERE state IN ('running', 'suspended')
      AND (lease_owner IS NOT NULL OR lease_token IS NOT NULL OR lease_expires_at IS NOT NULL)
      AND lease_expires_at < now()
      #{skip_local_sql}
    RETURNING id
    """

    query_count(instance, sql, params)
  end

  defp recover_activity_tasks(instance) do
    sql = """
    UPDATE continuum_activity_tasks
    SET state = 'available',
        lease_owner = NULL,
        lease_expires_at = NULL,
        available_at = now()
    WHERE state = 'leased'
      AND lease_expires_at < now()
    RETURNING id
    """

    query_count(instance, sql, [])
  end

  defp recover_due_timers(instance) do
    sql = """
    UPDATE continuum_runs AS r
    SET next_wakeup_at = now()
    FROM continuum_timers AS t
    WHERE t.run_id = r.id
      AND t.fired = false
      AND t.fires_at <= now()
      AND r.state = 'suspended'
      AND (r.next_wakeup_at IS NULL OR r.next_wakeup_at > now())
    RETURNING r.id
    """

    query_count(instance, sql, [])
  end

  defp query_count(instance, sql, params) do
    case instance.repo.query(sql, params) do
      {:ok, %{num_rows: count}} -> {:ok, count}
      {:error, reason} -> {:error, reason}
    end
  end

  defp recovery_config do
    case Application.get_env(:continuum, :recovery, []) do
      false -> [enabled?: false]
      true -> [enabled?: true]
      opts when is_list(opts) -> opts
    end
  end

  defp recovery_enabled?(instance) do
    instance.repo != nil
  end

  defp local_run_ids(instance) do
    Registry.select(instance.registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end
end
