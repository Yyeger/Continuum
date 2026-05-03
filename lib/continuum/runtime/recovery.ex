defmodule Continuum.Runtime.Recovery do
  @moduledoc """
  Boot-time recovery for orphaned durable work.

  Recovery is intentionally narrow in v0.1: it does not execute work itself.
  It makes abandoned rows eligible for the existing dispatcher, timer wheel,
  and activity worker pollers after a node restart.
  """

  use GenServer
  require Logger

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
    config = recovery_config()
    enabled? = Keyword.get(opts, :enabled?, Keyword.get(config, :enabled?, recovery_enabled?()))

    if enabled? do
      GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
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
  def recover_once(_opts \\ []) do
    with {:ok, runs} <- recover_runs(),
         {:ok, activity_tasks} <- recover_activity_tasks(),
         {:ok, timers} <- recover_due_timers() do
      {:ok, %{runs: runs, activity_tasks: activity_tasks, timers: timers}}
    end
  end

  @impl true
  def init(_opts) do
    {:ok, %{}, {:continue, :recover}}
  end

  @impl true
  def handle_continue(:recover, state) do
    case recover_once() do
      {:ok, counts} ->
        Logger.info("Continuum recovery completed: #{inspect(counts)}")
        {:stop, :normal, state}

      {:error, reason} ->
        Logger.error("Continuum recovery failed: #{inspect(reason)}")
        {:stop, reason, state}
    end
  end

  defp recover_runs do
    local_run_ids = local_run_ids()

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
      #{skip_local_sql}
    RETURNING id
    """

    query_count(sql, params)
  end

  defp recover_activity_tasks do
    sql = """
    UPDATE continuum_activity_tasks
    SET state = 'available',
        lease_owner = NULL,
        lease_expires_at = NULL,
        available_at = now()
    WHERE state = 'leased'
    RETURNING id
    """

    query_count(sql, [])
  end

  defp recover_due_timers do
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

    query_count(sql, [])
  end

  defp query_count(sql, params) do
    case repo().query(sql, params) do
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

  defp recovery_enabled? do
    Application.get_env(:continuum, :repo) != nil
  end

  defp local_run_ids do
    Registry.select(Continuum.Runtime.Registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp repo do
    Application.fetch_env!(:continuum, :repo)
  end
end
