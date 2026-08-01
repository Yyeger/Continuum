defmodule Continuum.Health do
  @moduledoc """
  Read-only operational health reporting and explicitly fenced repairs.

  `report/1` is the shared source for `mix continuum.health` and the Observer
  health panel. It reports partition coverage, workflow registration drift,
  active-run wait reasons, durable wake and timer lag, leases, activities, and
  signal backlog.

  `repair/3` is a dry run unless `execute: true` is passed. Mutating actions
  compare the lease epoch/owner or activity attempt/owner observed by the
  operator, so a stale repair cannot overwrite newer runtime authority.
  """

  alias Continuum.Runtime.{Engine, Instance}
  alias Continuum.Schema.{HealthReview, WorkflowVersion}

  @default_partition_months 3
  @default_lost_wake_after_ms 60_000
  @default_limit 100
  @active_states ~w(running suspended stuck_unknown_version)

  @type repair_action ::
          :wake | :retry | :release_expired_lease | :requeue_activity | :mark_reviewed

  @doc since: "0.6.2"
  @doc """
  Builds an operational health report for a PostgreSQL-backed instance.

  Options include `:instance` or `:repo`, `:partition_months` (default 3),
  `:lost_wake_after_ms` (default 60 seconds), and `:limit` for candidate lists.
  """
  @spec report(keyword()) :: {:ok, map()} | {:error, term()}
  def report(opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      now = database_now(instance.repo)
      reviews = review_keys(instance.repo)
      partitions = partition_health(instance.repo, now, opts)
      workflow_versions = workflow_version_health(instance, instance.repo)
      runs = run_health(instance.repo, now)
      timers = timer_health(instance.repo, now, opts, reviews)
      leases = lease_health(instance.repo, now, opts, reviews)
      activities = activity_health(instance.repo, now, opts, reviews)
      signals = signal_health(instance.repo, now)
      lost_wakes = lost_wake_health(instance.repo, now, opts, reviews)
      runtime = Continuum.readiness(instance: instance)

      runs =
        runs
        |> Map.put(:lost_wake_candidates, lost_wakes)
        |> Map.put(
          :lost_wake_count,
          lost_wake_count(instance.repo, opts)
        )

      report = %{
        generated_at: now,
        instance: inspect(instance.name),
        status:
          overall_status(
            runtime,
            partitions,
            workflow_versions,
            runs,
            timers,
            leases,
            activities
          ),
        runtime: runtime,
        partitions: partitions,
        workflow_versions: workflow_versions,
        runs: runs,
        timers: timers,
        leases: leases,
        activities: activities,
        signals: signals
      }

      {:ok, report}
    end
  rescue
    error -> {:error, error}
  end

  @doc since: "0.6.2"
  @doc """
  Plans or executes one operational repair.

  Supported actions:

    * `:wake` — arm and route a run wake; requires `:lease_token`.
    * `:retry` — retry durable registration for a loaded workflow version.
    * `:release_expired_lease` — release only an expired run lease; requires
      `:lease_owner` and `:lease_token`.
    * `:requeue_activity` — requeue only an expired activity claim; requires
      `:lease_owner` and `:attempt`.
    * `:mark_reviewed` — acknowledge a finding fingerprint; requires
      `:finding_type` and `:fingerprint`.

  Every action defaults to a dry run. Pass `execute: true` only after reviewing
  the returned plan.
  """
  @spec repair(repair_action() | binary(), binary(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def repair(action, subject_id, opts \\ []) when is_binary(subject_id) do
    with {:ok, instance} <- repo_instance(opts),
         {:ok, action} <- normalize_action(action) do
      execute? = Keyword.get(opts, :execute, false)
      do_repair(action, subject_id, instance, execute?, opts)
    end
  rescue
    error -> {:error, error}
  end

  @doc since: "0.6.2"
  @doc "Returns true when a health report contains an actionable degradation."
  @spec degraded?(map()) :: boolean()
  def degraded?(%{status: status}), do: status == :degraded

  defp partition_health(repo, now, opts) do
    months = positive_integer(Keyword.get(opts, :partition_months, @default_partition_months))
    first = Date.new!(now.year, now.month, 1)
    required = Enum.map(0..(months - 1), &partition_name(shift_month(first, &1)))

    attached =
      query_rows(repo, """
      SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
      FROM pg_inherits i
      JOIN pg_class c ON c.oid = i.inhrelid
      WHERE i.inhparent = to_regclass('continuum_events')
      ORDER BY c.relname
      """)

    default = Enum.find(attached, fn [_name, bound] -> bound == "DEFAULT" end)

    present =
      attached
      |> Enum.reject(fn [_name, bound] -> bound == "DEFAULT" end)
      |> Enum.map(&hd/1)

    missing = required -- present
    default_partition = default_partition_health(repo, default)

    %{
      status:
        if(missing == [] and default_partition.present? and default_partition.row_count == 0,
          do: :ok,
          else: :degraded
        ),
      required_months: required,
      present: present,
      missing: missing,
      horizon_months: months,
      horizon_end: required |> List.last() |> partition_month_label(),
      default_partition: default_partition
    }
  end

  defp default_partition_health(_repo, nil) do
    %{
      name: Continuum.Partitions.default_partition_name(),
      present?: false,
      row_count: 0,
      oldest_inserted_at: nil
    }
  end

  defp default_partition_health(repo, [name, "DEFAULT"]) do
    [[count, oldest_inserted_at]] =
      query_rows(
        repo,
        "SELECT count(*), min(inserted_at) FROM ONLY #{quote_ident(name)}"
      )

    %{
      name: name,
      present?: true,
      row_count: count,
      oldest_inserted_at: oldest_inserted_at
    }
  end

  defp workflow_version_health(instance, repo) do
    loaded =
      Continuum.VersionRegistry.entries(instance)
      |> Enum.map(fn entry ->
        %{
          workflow: entry.workflow_string,
          version_hash: encode_hash(entry.version_hash),
          raw_hash: entry.version_hash,
          entrypoint: inspect(entry.entrypoint)
        }
      end)

    durable =
      repo.all(WorkflowVersion)
      |> Enum.map(fn row ->
        %{
          workflow: row.workflow,
          version_hash: encode_hash(row.version_hash),
          entrypoint: row.entrypoint
        }
      end)
      |> filter_durable_versions(instance, loaded)

    durable_keys = MapSet.new(durable, &{&1.workflow, &1.version_hash})
    loaded_keys = MapSet.new(loaded, &{&1.workflow, &1.version_hash})

    missing =
      loaded
      |> Enum.reject(&MapSet.member?(durable_keys, {&1.workflow, &1.version_hash}))
      |> Enum.map(&Map.drop(&1, [:raw_hash]))

    unavailable =
      Enum.reject(durable, &MapSet.member?(loaded_keys, {&1.workflow, &1.version_hash}))

    registrar =
      if instance.name == :health do
        %{state: :not_applicable, registered_count: 0, pending_count: 0, last_error: nil}
      else
        Continuum.VersionRegistry.status(instance)
      end

    %{
      status:
        if(missing == [] and registrar.state in [:ready, :not_applicable],
          do: :ok,
          else: :degraded
        ),
      loaded_count: length(loaded),
      durable_count: length(durable),
      missing_in_database: missing,
      registered_without_loaded_code: unavailable,
      registrar: registrar
    }
  end

  defp run_health(repo, now) do
    groups =
      query_rows(
        repo,
        """
        WITH active AS (
          SELECT r.*,
                 CASE
                   WHEN r.state = 'stuck_unknown_version' THEN 'unknown_version'
                   WHEN latest.event_type = 'activity_scheduled' THEN 'activity'
                   WHEN latest.event_type = 'compensation_scheduled' THEN 'compensation'
                   WHEN latest.event_type = 'timer_started' THEN 'timer'
                   WHEN latest.event_type = 'signal_awaited' THEN 'signal'
                   WHEN latest.event_type = 'child_started' THEN 'child'
                   WHEN r.state = 'running' THEN 'executing'
                   ELSE COALESCE(latest.event_type, 'unknown')
                 END AS reason
          FROM continuum_runs r
          LEFT JOIN LATERAL (
            SELECT e.event_type
            FROM continuum_events e
            WHERE e.run_id = r.id
            ORDER BY e.seq DESC, e.inserted_at DESC
            LIMIT 1
          ) latest ON true
          WHERE r.state = ANY($1)
        )
        SELECT state, reason, count(*)::bigint, min(started_at)
        FROM active
        GROUP BY state, reason
        ORDER BY state, reason
        """,
        [@active_states]
      )
      |> Enum.map(fn [state, reason, count, oldest] ->
        %{
          state: state,
          reason: reason,
          count: count,
          oldest_started_at: oldest,
          oldest_age_ms: age_ms(now, oldest)
        }
      end)

    %{active_count: Enum.sum(Enum.map(groups, & &1.count)), groups: groups}
  end

  defp lost_wake_health(repo, now, opts, reviews) do
    threshold =
      non_negative_integer(Keyword.get(opts, :lost_wake_after_ms, @default_lost_wake_after_ms))

    limit = candidate_query_limit(opts, reviews)

    query_rows(
      repo,
      """
      SELECT id::text, state, lease_owner, lease_token, next_wakeup_at
      FROM continuum_runs
      WHERE state IN ('running', 'suspended')
        AND lease_owner IS NOT NULL
        AND lease_token IS NOT NULL
        AND lease_expires_at > clock_timestamp()
        AND next_wakeup_at <= clock_timestamp() - ($1 * interval '1 millisecond')
      ORDER BY next_wakeup_at
      LIMIT $2
      """,
      [threshold, limit]
    )
    |> Enum.map(fn [run_id, state, owner, token, wake_at] ->
      finding(:lost_wake, run_id, [token, wake_at], reviews, %{
        run_id: run_id,
        state: state,
        lease_owner: owner,
        lease_token: token,
        next_wakeup_at: wake_at,
        lag_ms: age_ms(now, wake_at)
      })
    end)
    |> limit_findings(result_limit(opts))
  end

  defp lost_wake_count(repo, opts) do
    threshold =
      non_negative_integer(Keyword.get(opts, :lost_wake_after_ms, @default_lost_wake_after_ms))

    scalar(
      repo,
      """
      SELECT count(*)::bigint
      FROM continuum_runs
      WHERE state IN ('running', 'suspended')
        AND lease_owner IS NOT NULL
        AND lease_token IS NOT NULL
        AND lease_expires_at > clock_timestamp()
        AND next_wakeup_at <= clock_timestamp() - ($1 * interval '1 millisecond')
      """,
      [threshold]
    )
  end

  defp timer_health(repo, now, opts, reviews) do
    limit = candidate_query_limit(opts, reviews)

    overdue =
      query_rows(
        repo,
        """
        SELECT t.id::text, t.run_id::text, t.fires_at, r.lease_token
        FROM continuum_timers t
        JOIN continuum_runs r ON r.id = t.run_id
        WHERE t.fired = false
          AND t.fires_at <= clock_timestamp()
          AND r.state IN ('running', 'suspended')
        ORDER BY t.fires_at
        LIMIT $1
        """,
        [limit]
      )
      |> Enum.map(fn [timer_id, run_id, fires_at, token] ->
        finding(:overdue_timer, timer_id, [run_id, fires_at], reviews, %{
          timer_id: timer_id,
          run_id: run_id,
          lease_token: token,
          fires_at: fires_at,
          overdue_ms: age_ms(now, fires_at)
        })
      end)
      |> limit_findings(result_limit(opts))

    count =
      scalar(repo, """
      SELECT count(*)::bigint
      FROM continuum_timers t
      JOIN continuum_runs r ON r.id = t.run_id
      WHERE t.fired = false AND t.fires_at <= clock_timestamp()
        AND r.state IN ('running', 'suspended')
      """)

    %{overdue_count: count, overdue: overdue}
  end

  defp lease_health(repo, now, opts, reviews) do
    limit = candidate_query_limit(opts, reviews)

    entries =
      query_rows(
        repo,
        """
        SELECT id::text, lease_owner, lease_token, lease_acquired_at,
               lease_heartbeat_at, lease_expires_at,
               lease_expires_at <= clock_timestamp() AS expired
        FROM continuum_runs
        WHERE state IN ('running', 'suspended') AND lease_owner IS NOT NULL
        ORDER BY lease_expires_at
        LIMIT $1
        """,
        [limit]
      )
      |> Enum.map(fn [run_id, owner, token, acquired_at, heartbeat_at, expires_at, expired?] ->
        base = %{
          run_id: run_id,
          owner: owner,
          epoch: token,
          acquired_at: acquired_at,
          age_ms: age_ms(now, acquired_at),
          heartbeat_at: heartbeat_at,
          heartbeat_lag_ms: age_ms(now, heartbeat_at),
          expires_at: expires_at,
          expired: expired?
        }

        if expired? do
          finding(:expired_lease, run_id, [owner, token, expires_at], reviews, base)
        else
          Map.merge(base, %{fingerprint: nil, reviewed: false})
        end
      end)
      |> limit_lease_entries(result_limit(opts))

    %{
      active_count:
        scalar(repo, """
        SELECT count(*)::bigint FROM continuum_runs
        WHERE state IN ('running', 'suspended') AND lease_owner IS NOT NULL
        """),
      expired_count:
        scalar(repo, """
        SELECT count(*)::bigint FROM continuum_runs
        WHERE state IN ('running', 'suspended') AND lease_owner IS NOT NULL
          AND lease_expires_at <= clock_timestamp()
        """),
      entries: entries
    }
  end

  defp activity_health(repo, now, opts, reviews) do
    counts =
      query_rows(repo, """
      SELECT state, count(*)::bigint
      FROM continuum_activity_tasks
      GROUP BY state
      ORDER BY state
      """)
      |> Map.new(fn [state, count] -> {state, count} end)

    retry_distribution =
      query_rows(repo, """
      SELECT attempt, count(*)::bigint
      FROM continuum_activity_tasks
      WHERE state IN ('available', 'leased')
      GROUP BY attempt
      ORDER BY attempt
      """)
      |> Enum.map(fn [attempt, count] -> %{attempt: attempt, count: count} end)

    oldest =
      query_rows(repo, """
      SELECT min(scheduled_at)
      FROM continuum_activity_tasks
      WHERE state IN ('available', 'leased')
      """)
      |> List.first()
      |> case do
        [value] -> value
        _ -> nil
      end

    expired_leases =
      query_rows(
        repo,
        """
        SELECT id::text, run_id::text, attempt, lease_owner, lease_expires_at
        FROM continuum_activity_tasks
        WHERE state = 'leased' AND lease_expires_at <= clock_timestamp()
        ORDER BY lease_expires_at
        LIMIT $1
        """,
        [candidate_query_limit(opts, reviews)]
      )
      |> Enum.map(fn [task_id, run_id, attempt, owner, expires_at] ->
        finding(:expired_activity_lease, task_id, [attempt, owner, expires_at], reviews, %{
          task_id: task_id,
          run_id: run_id,
          attempt: attempt,
          lease_owner: owner,
          lease_expires_at: expires_at,
          overdue_ms: age_ms(now, expires_at)
        })
      end)
      |> limit_findings(result_limit(opts))

    dead_letters =
      query_rows(
        repo,
        """
        SELECT id::text, run_id::text, attempt, scheduled_at, error
        FROM continuum_activity_tasks
        WHERE state IN ('discarded', 'dead_lettered')
          AND (error IS NULL OR error <> $1)
        ORDER BY scheduled_at
        LIMIT $2
        """,
        [:erlang.term_to_binary(:cancelled), candidate_query_limit(opts, reviews)]
      )
      |> Enum.map(fn [task_id, run_id, attempt, scheduled_at, error] ->
        decoded_error = decode_term(error)

        finding(:dead_letter_activity, task_id, [attempt, error], reviews, %{
          task_id: task_id,
          run_id: run_id,
          attempt: attempt,
          scheduled_at: scheduled_at,
          age_ms: age_ms(now, scheduled_at),
          error: inspect(decoded_error),
          cancelled: decoded_error == :cancelled
        })
      end)
      |> Enum.map(&Map.delete(&1, :cancelled))
      |> limit_findings(result_limit(opts))

    heartbeats =
      query_rows(
        repo,
        """
        SELECT id::text, run_id::text, state, attempt, last_heartbeat_at, heartbeat_details
        FROM continuum_activity_tasks
        WHERE last_heartbeat_at IS NOT NULL
        ORDER BY last_heartbeat_at DESC, id
        LIMIT $1
        """,
        [result_limit(opts)]
      )
      |> Enum.map(fn [task_id, run_id, state, attempt, heartbeat_at, details] ->
        %{
          task_id: task_id,
          run_id: run_id,
          state: String.to_atom(state),
          attempt: attempt,
          heartbeat_at: heartbeat_at,
          heartbeat_age_ms: age_ms(now, heartbeat_at),
          details: decode_term(details)
        }
      end)

    %{
      pending_count: Map.get(counts, "available", 0),
      leased_count: Map.get(counts, "leased", 0),
      oldest_task_at: oldest,
      oldest_task_age_ms: age_ms(now, oldest),
      retry_distribution: retry_distribution,
      expired_lease_count:
        scalar(repo, """
        SELECT count(*)::bigint FROM continuum_activity_tasks
        WHERE state = 'leased' AND lease_expires_at <= clock_timestamp()
        """),
      expired_leases: expired_leases,
      dead_letter_count:
        scalar(
          repo,
          """
          SELECT count(*)::bigint FROM continuum_activity_tasks
          WHERE state IN ('discarded', 'dead_lettered') AND (error IS NULL OR error <> $1)
          """,
          [:erlang.term_to_binary(:cancelled)]
        ),
      explicit_dead_letter_count:
        scalar(
          repo,
          "SELECT count(*)::bigint FROM continuum_activity_tasks WHERE state = 'dead_lettered'"
        ),
      dead_letter_candidates: dead_letters,
      heartbeat_count:
        scalar(
          repo,
          "SELECT count(*)::bigint FROM continuum_activity_tasks WHERE last_heartbeat_at IS NOT NULL"
        ),
      heartbeats: heartbeats,
      counts_by_state: counts
    }
  end

  defp signal_health(repo, now) do
    [count, oldest] =
      query_rows(repo, """
      SELECT count(*)::bigint, min(inserted_at)
      FROM continuum_signals
      WHERE delivered = false
      """)
      |> List.first()

    %{
      pending_count: count,
      oldest_signal_at: oldest,
      catch_up_lag_ms: age_ms(now, oldest)
    }
  end

  defp overall_status(runtime, partitions, versions, runs, timers, leases, activities) do
    degraded? =
      runtime.state == :degraded or partitions.status == :degraded or
        versions.status == :degraded or
        unreviewed?(runs.lost_wake_count, runs.lost_wake_candidates) or
        unreviewed?(timers.overdue_count, timers.overdue) or
        unreviewed?(leases.expired_count, Enum.filter(leases.entries, & &1.expired)) or
        unreviewed?(activities.expired_lease_count, activities.expired_leases) or
        unreviewed?(activities.dead_letter_count, activities.dead_letter_candidates)

    if degraded?, do: :degraded, else: :ok
  end

  defp unreviewed?(count, candidates) do
    count > Enum.count(candidates, & &1.reviewed)
  end

  defp do_repair(:wake, run_id, instance, execute?, opts) do
    token = fetch_integer!(opts, :lease_token)
    plan = %{action: :wake, subject_id: run_id, lease_token: token}

    if execute? do
      sql = """
      UPDATE continuum_runs
      SET next_wakeup_at = clock_timestamp()
      WHERE id = $1::text::uuid
        AND state IN ('running', 'suspended')
        AND lease_token = $2
      """

      case instance.repo.query(sql, [run_id, token]) do
        {:ok, %{num_rows: 1}} ->
          if Process.whereis(instance.registry), do: Engine.wake(instance, run_id)
          repaired(instance, plan)

        {:ok, %{num_rows: 0}} ->
          {:error, :stale_or_inactive}

        {:error, reason} ->
          {:error, reason}
      end
    else
      ensure_run_lease(instance.repo, run_id, token, nil, false, plan)
    end
  end

  defp do_repair(:retry, workflow, instance, execute?, _opts) do
    entries =
      instance
      |> Continuum.VersionRegistry.entries()
      |> Enum.filter(&(&1.workflow_string == workflow))

    plan = %{action: :retry, subject_id: workflow, versions: length(entries)}

    cond do
      entries == [] ->
        {:error, :workflow_not_loaded}

      not execute? ->
        planned(plan)

      true ->
        case Enum.reduce_while(entries, :ok, fn entry, :ok ->
               case Continuum.VersionRegistry.ensure_registered(entry.entrypoint, instance) do
                 {:ok, _entry} -> {:cont, :ok}
                 {:error, reason} -> {:halt, {:error, reason}}
               end
             end) do
          :ok -> repaired(instance, plan)
          {:error, _reason} = error -> error
        end
    end
  end

  defp do_repair(:release_expired_lease, run_id, instance, execute?, opts) do
    token = fetch_integer!(opts, :lease_token)
    owner = fetch_binary!(opts, :lease_owner)

    plan = %{
      action: :release_expired_lease,
      subject_id: run_id,
      lease_owner: owner,
      lease_token: token
    }

    if execute? do
      sql = """
      UPDATE continuum_runs
      SET state = CASE WHEN state = 'running' THEN 'suspended' ELSE state END,
          lease_owner = NULL,
          lease_token = NULL,
          lease_expires_at = NULL,
          next_wakeup_at = COALESCE(next_wakeup_at, clock_timestamp())
      WHERE id = $1::text::uuid
        AND state IN ('running', 'suspended')
        AND lease_owner = $2
        AND lease_token = $3
        AND lease_expires_at <= clock_timestamp()
      """

      case instance.repo.query(sql, [run_id, owner, token]) do
        {:ok, %{num_rows: 1}} -> repaired(instance, plan)
        {:ok, %{num_rows: 0}} -> already_or_stale_run(instance.repo, run_id, plan)
        {:error, reason} -> {:error, reason}
      end
    else
      ensure_run_lease(instance.repo, run_id, token, owner, true, plan)
    end
  end

  defp do_repair(:requeue_activity, task_id, instance, execute?, opts) do
    attempt = fetch_integer!(opts, :attempt)
    owner = fetch_binary!(opts, :lease_owner)

    plan = %{
      action: :requeue_activity,
      subject_id: task_id,
      lease_owner: owner,
      attempt: attempt
    }

    if execute? do
      sql = """
      UPDATE continuum_activity_tasks
      SET state = 'available',
          attempt = attempt + 1,
          lease_owner = NULL,
          lease_expires_at = NULL,
          available_at = clock_timestamp()
      WHERE id = $1::text::uuid
        AND state = 'leased'
        AND lease_owner = $2
        AND attempt = $3
        AND lease_expires_at <= clock_timestamp()
      """

      case instance.repo.query(sql, [task_id, owner, attempt]) do
        {:ok, %{num_rows: 1}} -> repaired(instance, plan)
        {:ok, %{num_rows: 0}} -> already_or_stale_task(instance.repo, task_id, attempt, plan)
        {:error, reason} -> {:error, reason}
      end
    else
      ensure_expired_task(instance.repo, task_id, owner, attempt, plan)
    end
  end

  defp do_repair(:mark_reviewed, subject_id, instance, execute?, opts) do
    finding_type = fetch_binary!(opts, :finding_type)
    fingerprint = fetch_binary!(opts, :fingerprint)

    plan = %{
      action: :mark_reviewed,
      subject_id: subject_id,
      finding_type: finding_type,
      fingerprint: fingerprint,
      reviewed_by: Keyword.get(opts, :reviewed_by),
      reason: Keyword.get(opts, :reason)
    }

    if execute? do
      now = database_now(instance.repo)

      instance.repo.insert_all(
        HealthReview,
        [
          %{
            finding_type: finding_type,
            subject_id: subject_id,
            fingerprint: fingerprint,
            reviewed_by: Keyword.get(opts, :reviewed_by),
            reason: Keyword.get(opts, :reason),
            reviewed_at: now
          }
        ],
        on_conflict: {:replace, [:reviewed_by, :reason, :reviewed_at]},
        conflict_target: [:finding_type, :subject_id, :fingerprint]
      )

      repaired(instance, plan)
    else
      planned(plan)
    end
  end

  defp ensure_run_lease(repo, run_id, token, owner, expired?, plan) do
    params = if owner, do: [run_id, token, owner], else: [run_id, token]
    owner_sql = if owner, do: "AND lease_owner = $3", else: ""
    expiry_sql = if expired?, do: "AND lease_expires_at <= clock_timestamp()", else: ""

    count =
      scalar(
        repo,
        """
        SELECT count(*)::bigint
        FROM continuum_runs
        WHERE id = $1::text::uuid AND state IN ('running', 'suspended')
          AND lease_token = $2 #{owner_sql} #{expiry_sql}
        """,
        params
      )

    if count == 1, do: planned(plan), else: {:error, :stale_or_inactive}
  end

  defp ensure_expired_task(repo, task_id, owner, attempt, plan) do
    count =
      scalar(
        repo,
        """
        SELECT count(*)::bigint
        FROM continuum_activity_tasks
        WHERE id = $1::text::uuid AND state = 'leased'
          AND lease_owner = $2 AND attempt = $3
          AND lease_expires_at <= clock_timestamp()
        """,
        [task_id, owner, attempt]
      )

    if count == 1, do: planned(plan), else: {:error, :stale_or_not_expired}
  end

  defp already_or_stale_run(repo, run_id, plan) do
    case query_rows(
           repo,
           "SELECT lease_owner, lease_token FROM continuum_runs WHERE id = $1::text::uuid",
           [run_id]
         ) do
      [[nil, nil]] -> already_applied(plan)
      _ -> {:error, :stale_or_not_expired}
    end
  end

  defp already_or_stale_task(repo, task_id, attempt, plan) do
    case query_rows(
           repo,
           "SELECT state, attempt, lease_owner FROM continuum_activity_tasks WHERE id = $1::text::uuid",
           [task_id]
         ) do
      [["available", next_attempt, nil]] when next_attempt == attempt + 1 -> already_applied(plan)
      _ -> {:error, :stale_or_not_expired}
    end
  end

  defp planned(plan), do: {:ok, Map.put(plan, :status, :planned)}
  defp already_applied(plan), do: {:ok, Map.put(plan, :status, :already_applied)}

  defp repaired(instance, plan) do
    :telemetry.execute([:continuum, :health, :repaired], %{count: 1}, %{
      instance: instance.name,
      action: plan.action,
      subject_id: plan.subject_id
    })

    {:ok, Map.put(plan, :status, :executed)}
  end

  defp finding(type, subject_id, parts, reviews, values) do
    fingerprint = fingerprint(parts)

    Map.merge(values, %{
      finding_type: Atom.to_string(type),
      subject_id: subject_id,
      fingerprint: fingerprint,
      reviewed: MapSet.member?(reviews, {Atom.to_string(type), subject_id, fingerprint})
    })
  end

  defp review_keys(repo) do
    repo.all(HealthReview)
    |> MapSet.new(&{&1.finding_type, &1.subject_id, &1.fingerprint})
  end

  defp filter_durable_versions(durable, %Instance{workflow_modules: nil}, _loaded), do: durable

  defp filter_durable_versions(durable, %Instance{}, loaded) do
    configured_workflows = MapSet.new(loaded, & &1.workflow)
    Enum.filter(durable, &MapSet.member?(configured_workflows, &1.workflow))
  end

  defp fingerprint(parts) do
    :crypto.hash(:sha256, :erlang.term_to_binary(parts))
    |> Base.encode16(case: :lower)
  end

  defp normalize_action(action) when is_binary(action) do
    case action do
      "wake" -> {:ok, :wake}
      "retry" -> {:ok, :retry}
      "release" -> {:ok, :release_expired_lease}
      "release_expired_lease" -> {:ok, :release_expired_lease}
      "requeue" -> {:ok, :requeue_activity}
      "requeue_activity" -> {:ok, :requeue_activity}
      "review" -> {:ok, :mark_reviewed}
      "mark_reviewed" -> {:ok, :mark_reviewed}
      _ -> {:error, {:unknown_action, action}}
    end
  end

  defp normalize_action(action)
       when action in [:wake, :retry, :release_expired_lease, :requeue_activity, :mark_reviewed],
       do: {:ok, action}

  defp normalize_action(action), do: {:error, {:unknown_action, action}}

  defp repo_instance(opts) do
    case Keyword.get(opts, :repo) do
      nil ->
        instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
        if instance.repo, do: {:ok, instance}, else: {:error, :repo_not_configured}

      repo ->
        repo_instance_for_repo(repo, Keyword.get(opts, :instance))
    end
  end

  defp repo_instance_for_repo(repo, nil) do
    case Instance.running_for_repo(repo) do
      {:ok, instance} -> {:ok, instance}
      :none -> {:ok, Instance.new(name: :health, repo: repo)}
      {:error, _reason} = error -> error
    end
  end

  defp repo_instance_for_repo(repo, %Instance{} = instance) do
    if instance.repo == repo, do: {:ok, instance}, else: {:error, :instance_repo_mismatch}
  end

  defp repo_instance_for_repo(repo, instance_name) do
    instance = Instance.lookup(instance_name)
    repo_instance_for_repo(repo, instance)
  end

  defp database_now(repo), do: scalar(repo, "SELECT clock_timestamp()")

  defp query_rows(repo, sql, params \\ []) do
    %{rows: rows} = repo.query!(sql, params)
    rows
  end

  defp scalar(repo, sql, params \\ []) do
    [[value]] = query_rows(repo, sql, params)
    value
  end

  defp age_ms(_now, nil), do: nil

  defp age_ms(%DateTime{} = now, %DateTime{} = then_at) do
    max(DateTime.diff(now, then_at, :millisecond), 0)
  end

  defp age_ms(%DateTime{} = now, %NaiveDateTime{} = then_at) do
    age_ms(now, DateTime.from_naive!(then_at, "Etc/UTC"))
  end

  defp partition_name(%Date{year: year, month: month}) do
    "continuum_events_y#{year}_m#{String.pad_leading(Integer.to_string(month), 2, "0")}"
  end

  defp partition_month_label("continuum_events_y" <> rest), do: String.replace(rest, "_m", "-")
  defp partition_month_label(other), do: other

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")

  defp shift_month(%Date{} = month, offset) do
    absolute_month = month.year * 12 + month.month - 1 + offset
    Date.new!(div(absolute_month, 12), rem(absolute_month, 12) + 1, 1)
  end

  defp result_limit(opts), do: positive_integer(Keyword.get(opts, :limit, @default_limit))

  defp candidate_query_limit(opts, reviews), do: result_limit(opts) + MapSet.size(reviews)

  defp limit_findings(findings, limit) do
    {unreviewed, reviewed} = Enum.split_with(findings, &(not &1.reviewed))
    Enum.take(unreviewed ++ reviewed, limit)
  end

  defp limit_lease_entries(entries, limit) do
    entries
    |> Enum.sort_by(fn entry ->
      cond do
        entry.expired and not entry.reviewed -> 0
        not entry.expired -> 1
        true -> 2
      end
    end)
    |> Enum.take(limit)
  end

  defp positive_integer(value) when is_integer(value) and value > 0, do: value

  defp positive_integer(value),
    do: raise(ArgumentError, "expected a positive integer, got: #{inspect(value)}")

  defp non_negative_integer(value) when is_integer(value) and value >= 0, do: value

  defp non_negative_integer(value),
    do: raise(ArgumentError, "expected a non-negative integer, got: #{inspect(value)}")

  defp fetch_integer!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) -> value
      _ -> raise ArgumentError, "#{key} must be provided as an integer"
    end
  end

  defp fetch_binary!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) and value != "" -> value
      _ -> raise ArgumentError, "#{key} must be provided as a non-empty string"
    end
  end

  defp encode_hash(hash), do: Base.encode16(hash, case: :lower)

  defp decode_term(nil), do: nil

  defp decode_term(binary) when is_binary(binary) do
    :erlang.binary_to_term(binary)
  rescue
    _ -> {:decode_error, Base.encode16(binary, case: :lower)}
  end
end
