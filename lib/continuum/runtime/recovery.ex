defmodule Continuum.Runtime.Recovery do
  @moduledoc """
  Boot-time recovery for orphaned durable work.

  Recovery is intentionally narrow in v0.1: it does not execute work itself.
  It makes abandoned rows eligible for the existing dispatcher, timer wheel,
  and activity worker pollers after a node restart.
  """

  use GenServer
  require Logger

  alias Continuum.{Runtime.Instance, Runtime.SQL, Telemetry}

  @max_local_exclusions 256

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
    opts = Continuum.Config.validate_component!(:recovery, opts)
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    config = Continuum.Config.validate_component!(:recovery, recovery_config())

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
    case SQL.dialect(instance.repo) do
      :postgres -> recover_runs_postgres(instance)
      :sqlite -> recover_runs_sqlite(instance)
    end
  end

  defp recover_runs_sqlite(instance) do
    {skip_local_sql, params} =
      case Instance.local_run_ids(instance, @max_local_exclusions) do
        [] -> {"", []}
        :too_many -> {"", []}
        ids -> {"AND id NOT IN (SELECT value FROM json_each(?2))", [Jason.encode!(ids)]}
      end

    sql = """
    UPDATE continuum_runs
    SET state = CASE WHEN state = 'running' THEN 'suspended' ELSE state END,
        lease_owner = NULL,
        lease_token = NULL,
        lease_expires_at = NULL,
        next_wakeup_at = CASE
          WHEN state = 'running' AND next_wakeup_at IS NULL THEN ?1
          ELSE next_wakeup_at
        END
    WHERE state IN ('running', 'suspended')
      AND (lease_owner IS NOT NULL OR lease_token IS NOT NULL OR lease_expires_at IS NOT NULL)
      AND lease_expires_at < ?1
      #{skip_local_sql}
    """

    query_count(instance, sql, [now_param(instance) | params])
  end

  defp recover_runs_postgres(instance) do
    # Same bound and same reasoning as the dispatcher's claim: excluding
    # locally-registered runs is an optimisation over the lease-expiry
    # predicate below, so a node with more live runs than the cap ships no
    # array rather than a 10k-element one.
    {skip_local_sql, params} =
      case Instance.local_run_ids(instance, @max_local_exclusions) do
        [] -> {"", []}
        :too_many -> {"", []}
        ids -> {"AND NOT (id = ANY($1::uuid[]))", [Enum.map(ids, &Ecto.UUID.dump!/1)]}
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

  # Shared with the activity dispatcher's poll loop: a worker that dies
  # between claim and completion leaves its task 'leased' forever otherwise,
  # because the claim queries only consider 'available' tasks.
  #
  # The interrupted execution consumes an attempt, exactly like an execution
  # that returned an error — otherwise a task that kills its worker (or the
  # node) re-executes forever, never exhausting max_attempts. The worker
  # fails tasks whose attempt exceeds the policy before re-executing them.
  @doc false
  def recover_activity_tasks(instance) do
    {now_sql, params} = now_sql(instance)

    sql = """
    UPDATE continuum_activity_tasks
    SET state = 'available',
        attempt = attempt + 1,
        lease_owner = NULL,
        lease_expires_at = NULL,
        available_at = #{now_sql}
    WHERE state = 'leased'
      AND lease_expires_at < #{now_sql}
    RETURNING id
    """

    query_count(instance, sql, params)
  end

  defp recover_due_timers(instance) do
    {now_sql, params} = now_sql(instance)

    sql = """
    UPDATE continuum_runs AS r
    SET next_wakeup_at = #{now_sql}
    FROM continuum_timers AS t
    WHERE t.run_id = r.id
      AND t.fired = false
      AND t.fires_at <= #{now_sql}
      AND r.state = 'suspended'
      AND (r.next_wakeup_at IS NULL OR r.next_wakeup_at > #{now_sql})
    RETURNING #{if SQL.sqlite?(instance.repo), do: "id", else: "r.id"}
    """

    query_count(instance, sql, params)
  end

  # PostgreSQL compares against its own transaction clock; SQLite against the
  # host clock it shares with the application, bound once as `?1`. SQLite's
  # RETURNING names columns of the updated table unqualified.
  defp now_sql(instance) do
    if SQL.sqlite?(instance.repo), do: {"?1", [now_param(instance)]}, else: {"now()", []}
  end

  defp now_param(instance), do: SQL.timestamp(instance.repo, SQL.utc_now())

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
end
