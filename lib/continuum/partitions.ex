defmodule Continuum.Partitions do
  @moduledoc """
  Maintains the monthly `continuum_events` partition horizon.

  `ensure/1` is cluster-safe and idempotent. It serializes maintenance with a
  PostgreSQL advisory lock, creates a default partition when one is missing,
  and creates the requested monthly partitions. If the default partition
  already contains rows for a newly-created month, those rows are moved into
  the month partition in the same transaction.

  The database user running this operation needs permission to create and lock
  tables. Applications that separate migration and runtime database roles can
  call this API from a release task instead of enabling the optional runtime
  maintainer.
  """

  alias Continuum.Runtime.Instance
  alias Continuum.Telemetry

  @default_horizon_months 4
  @default_partition "continuum_events_default"
  @max_horizon_months 120
  @maintenance_lock_key 0x434F4E54

  @type summary :: %{
          optional(:reason) => :maintenance_locked,
          status: :ok | :skipped,
          instance: atom() | nil,
          horizon_start: String.t(),
          horizon_end: String.t(),
          required: [String.t()],
          created: [String.t()],
          existing: [String.t()],
          moved_row_count: non_neg_integer(),
          default_partition: String.t() | nil,
          default_created?: boolean(),
          default_row_count: non_neg_integer()
        }

  @doc since: "0.6.2"
  @doc """
  Returns the current partition plan without changing the database.

  Options:

    * `:instance` or `:repo` selects the database.
    * `:months` is the positive future horizon, defaulting to 4 and capped at
      120.
    * `:start_month` accepts a first-of-month `Date` or `YYYY-MM` string. The
      database clock's current month is used by default.
  """
  @spec plan(keyword()) :: {:ok, map()} | {:error, term()}
  def plan(opts \\ []) do
    with {:ok, repo, instance_name} <- resolve_repo(opts),
         {:ok, months} <- validate_months(Keyword.get(opts, :months, @default_horizon_months)),
         {:ok, start_month} <- resolve_start_month(repo, Keyword.get(opts, :start_month)),
         {:ok, attached} <- attached_partitions(repo) do
      required = required_partitions(start_month, months)
      attached_names = MapSet.new(attached, & &1.name)
      missing = Enum.reject(required, &MapSet.member?(attached_names, &1.name))
      default = Enum.find(attached, &(&1.bound == "DEFAULT"))

      {:ok,
       %{
         instance: instance_name,
         horizon_start: month_label(start_month),
         horizon_end: start_month |> shift_month(months - 1) |> month_label(),
         required: Enum.map(required, & &1.name),
         present: required |> Enum.reject(&(&1 in missing)) |> Enum.map(& &1.name),
         missing: Enum.map(missing, & &1.name),
         default_partition: default && default.name,
         default_present?: not is_nil(default)
       }}
    end
  rescue
    error -> {:error, error}
  catch
    :exit, reason -> {:error, reason}
  end

  @doc since: "0.6.2"
  @doc """
  Ensures a future horizon of monthly event partitions.

  This operation is safe to call concurrently from multiple cluster nodes.
  The default lock mode waits for the current maintainer and then rechecks the
  horizon. Pass `lock: :try` for scheduled best-effort maintenance that should
  return `%{status: :skipped, reason: :maintenance_locked}` instead of waiting.

  See `plan/1` for the common options. `:timeout` controls the transaction
  timeout and defaults to 30 seconds. `:source` is copied to telemetry metadata.
  """
  @spec ensure(keyword()) :: {:ok, summary()} | {:error, term()}
  def ensure(opts \\ []) do
    started_at = System.monotonic_time(:millisecond)

    try do
      result = do_ensure(opts)
      emit_result(result, opts, started_at)
      result
    rescue
      error ->
        result = {:error, error}
        emit_result(result, opts, started_at)
        result
    catch
      :exit, reason ->
        result = {:error, reason}
        emit_result(result, opts, started_at)
        result
    end
  end

  @doc false
  def default_partition_name, do: @default_partition

  defp do_ensure(opts) do
    with {:ok, repo, instance_name} <- resolve_repo(opts),
         {:ok, months} <- validate_months(Keyword.get(opts, :months, @default_horizon_months)),
         {:ok, lock} <- validate_lock(Keyword.get(opts, :lock, :wait)),
         {:ok, start_month} <- resolve_start_month(repo, Keyword.get(opts, :start_month)) do
      timeout = Keyword.get(opts, :timeout, 30_000)

      case repo.transaction(
             fn -> maintain(repo, instance_name, start_month, months, lock) end,
             timeout: timeout
           ) do
        {:ok, summary} -> {:ok, summary}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp maintain(repo, instance_name, start_month, months, lock) do
    case acquire_lock(repo, lock) do
      :acquired -> do_maintain(repo, instance_name, start_month, months)
      :locked -> skipped_summary(instance_name, start_month, months)
    end
  end

  defp do_maintain(repo, instance_name, start_month, months) do
    ensure_partitioned_parent!(repo)
    required = required_partitions(start_month, months)
    attached = attached_partitions!(repo)
    attached_names = MapSet.new(attached, & &1.name)
    missing = Enum.reject(required, &MapSet.member?(attached_names, &1.name))
    default = Enum.find(attached, &(&1.bound == "DEFAULT"))

    if is_nil(default) or missing != [] do
      query!(repo, "LOCK TABLE continuum_events IN ACCESS EXCLUSIVE MODE")
    end

    {default, default_created?} = ensure_default_partition!(repo, default)

    if missing != [] do
      query!(repo, """
      CREATE TEMP TABLE IF NOT EXISTS continuum_events_partition_rollover
      (LIKE continuum_events INCLUDING DEFAULTS)
      ON COMMIT DROP
      """)
    end

    {created, moved_row_count} =
      Enum.reduce(missing, {[], 0}, fn partition, {created, moved_count} ->
        moved = create_month_partition!(repo, default.name, partition)
        {[partition.name | created], moved_count + moved}
      end)

    created = Enum.reverse(created)
    created_names = MapSet.new(created)
    existing = required |> Enum.map(& &1.name) |> Enum.reject(&MapSet.member?(created_names, &1))
    default_row_count = default_row_count!(repo, default.name)

    %{
      status: :ok,
      instance: instance_name,
      horizon_start: month_label(start_month),
      horizon_end: start_month |> shift_month(months - 1) |> month_label(),
      required: Enum.map(required, & &1.name),
      created: created,
      existing: existing,
      moved_row_count: moved_row_count,
      default_partition: default.name,
      default_created?: default_created?,
      default_row_count: default_row_count
    }
  end

  defp ensure_default_partition!(_repo, %{name: _name} = default), do: {default, false}

  defp ensure_default_partition!(repo, nil) do
    case relation_exists?(repo, @default_partition) do
      false ->
        query!(repo, """
        CREATE TABLE #{quote_ident(@default_partition)}
        PARTITION OF continuum_events DEFAULT
        """)

        {%{name: @default_partition, bound: "DEFAULT"}, true}

      true ->
        rollback(repo, {:partition_name_conflict, @default_partition})
    end
  end

  defp create_month_partition!(repo, default_partition, partition) do
    if relation_exists?(repo, partition.name) do
      rollback(repo, {:partition_name_conflict, partition.name})
    end

    query!(repo, "TRUNCATE continuum_events_partition_rollover")

    %{num_rows: copied_count} =
      query!(
        repo,
        """
        INSERT INTO continuum_events_partition_rollover
          (run_id, seq, event_type, payload, inserted_at)
        SELECT run_id, seq, event_type, payload, inserted_at
        FROM ONLY #{quote_ident(default_partition)}
        WHERE inserted_at >= $1::date AND inserted_at < $2::date
        """,
        [partition.from, partition.to]
      )

    %{num_rows: deleted_count} =
      query!(
        repo,
        """
        DELETE FROM ONLY #{quote_ident(default_partition)}
        WHERE inserted_at >= $1::date AND inserted_at < $2::date
        """,
        [partition.from, partition.to]
      )

    if copied_count != deleted_count do
      rollback(repo, {:default_partition_copy_mismatch, copied_count, deleted_count})
    end

    query!(repo, """
    CREATE TABLE #{quote_ident(partition.name)}
    PARTITION OF continuum_events
    FOR VALUES FROM ('#{Date.to_iso8601(partition.from)} 00:00:00+00')
    TO ('#{Date.to_iso8601(partition.to)} 00:00:00+00')
    """)

    %{num_rows: inserted_count} =
      query!(repo, """
      INSERT INTO continuum_events (run_id, seq, event_type, payload, inserted_at)
      SELECT run_id, seq, event_type, payload, inserted_at
      FROM continuum_events_partition_rollover
      """)

    if inserted_count != copied_count do
      rollback(repo, {:partition_rollover_insert_mismatch, copied_count, inserted_count})
    end

    inserted_count
  end

  defp ensure_partitioned_parent!(repo) do
    %{rows: [[partitioned?]]} =
      query!(repo, """
      SELECT EXISTS (
        SELECT 1
        FROM pg_partitioned_table pt
        WHERE pt.partrelid = to_regclass('continuum_events')
      )
      """)

    unless partitioned?, do: rollback(repo, :continuum_events_not_partitioned)
  end

  defp acquire_lock(repo, :wait) do
    query!(repo, "SELECT pg_advisory_xact_lock(hashtext(current_database()), $1)", [
      @maintenance_lock_key
    ])

    :acquired
  end

  defp acquire_lock(repo, :try) do
    %{rows: [[acquired?]]} =
      query!(repo, "SELECT pg_try_advisory_xact_lock(hashtext(current_database()), $1)", [
        @maintenance_lock_key
      ])

    if acquired?, do: :acquired, else: :locked
  end

  defp attached_partitions(repo) do
    case repo.query(attached_partitions_sql(), []) do
      {:ok, %{rows: rows}} -> {:ok, decode_attached(rows)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp attached_partitions!(repo) do
    %{rows: rows} = query!(repo, attached_partitions_sql())
    decode_attached(rows)
  end

  defp attached_partitions_sql do
    """
    SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = to_regclass('continuum_events')
    ORDER BY c.relname
    """
  end

  defp decode_attached(rows),
    do: Enum.map(rows, fn [name, bound] -> %{name: name, bound: bound} end)

  defp default_row_count!(repo, partition) do
    %{rows: [[count]]} =
      query!(repo, "SELECT count(*) FROM ONLY #{quote_ident(partition)}")

    count
  end

  defp relation_exists?(repo, relation) do
    %{rows: [[exists?]]} =
      query!(repo, "SELECT to_regclass($1) IS NOT NULL", [relation])

    exists?
  end

  defp required_partitions(start_month, months) do
    Enum.map(0..(months - 1), fn offset ->
      from = shift_month(start_month, offset)
      %{name: partition_name(from), from: from, to: shift_month(from, 1)}
    end)
  end

  defp skipped_summary(instance_name, start_month, months) do
    required = required_partitions(start_month, months)

    %{
      status: :skipped,
      reason: :maintenance_locked,
      instance: instance_name,
      horizon_start: month_label(start_month),
      horizon_end: start_month |> shift_month(months - 1) |> month_label(),
      required: Enum.map(required, & &1.name),
      created: [],
      existing: [],
      moved_row_count: 0,
      default_partition: nil,
      default_created?: false,
      default_row_count: 0
    }
  end

  defp resolve_repo(opts) do
    case Keyword.get(opts, :repo) do
      nil ->
        instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

        if instance.repo,
          do: {:ok, instance.repo, instance.name},
          else: {:error, :repo_not_configured}

      repo ->
        instance_name =
          case Keyword.get(opts, :instance) do
            %Instance{name: name} -> name
            nil -> nil
            name -> name
          end

        {:ok, repo, instance_name}
    end
  end

  defp resolve_start_month(repo, nil) do
    case repo.query("SELECT date_trunc('month', clock_timestamp())::date", []) do
      {:ok, %{rows: [[%Date{} = month]]}} -> {:ok, month}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_start_month(_repo, %Date{day: 1} = month), do: {:ok, month}

  defp resolve_start_month(_repo, %Date{} = date) do
    {:error, {:start_month_must_be_first_day, date}}
  end

  defp resolve_start_month(_repo, <<year::binary-size(4), "-", month::binary-size(2)>> = value) do
    with {year, ""} <- Integer.parse(year),
         {month, ""} <- Integer.parse(month),
         {:ok, date} <- Date.new(year, month, 1) do
      {:ok, date}
    else
      _ -> {:error, {:invalid_start_month, value}}
    end
  end

  defp resolve_start_month(_repo, value), do: {:error, {:invalid_start_month, value}}

  defp validate_months(months)
       when is_integer(months) and months > 0 and months <= @max_horizon_months,
       do: {:ok, months}

  defp validate_months(months), do: {:error, {:invalid_horizon_months, months}}

  defp validate_lock(lock) when lock in [:wait, :try], do: {:ok, lock}
  defp validate_lock(lock), do: {:error, {:invalid_lock_mode, lock}}

  defp query!(repo, sql, params \\ []) do
    case repo.query(sql, params) do
      {:ok, result} -> result
      {:error, reason} -> rollback(repo, reason)
    end
  end

  defp rollback(repo, reason), do: repo.rollback(reason)

  defp emit_result({:ok, summary}, opts, started_at) do
    duration_ms = System.monotonic_time(:millisecond) - started_at

    Telemetry.execute(
      [:continuum, :partition, :maintained],
      %{
        duration_ms: duration_ms,
        created_count: length(summary.created),
        moved_row_count: summary.moved_row_count,
        default_row_count: summary.default_row_count
      },
      %{
        instance: summary.instance,
        source: Keyword.get(opts, :source, :manual),
        status: summary.status,
        created: summary.created,
        horizon_end: summary.horizon_end
      }
    )
  end

  defp emit_result({:error, reason}, opts, started_at) do
    duration_ms = System.monotonic_time(:millisecond) - started_at

    Telemetry.execute(
      [:continuum, :partition, :maintenance_failed],
      %{duration_ms: duration_ms},
      %{
        instance: instance_name(opts),
        source: Keyword.get(opts, :source, :manual),
        reason: reason
      }
    )
  end

  defp instance_name(opts) do
    case Keyword.get(opts, :instance) do
      %Instance{name: name} -> name
      name -> name
    end
  end

  defp shift_month(%Date{} = month, offset) do
    absolute_month = month.year * 12 + month.month - 1 + offset
    Date.new!(div(absolute_month, 12), rem(absolute_month, 12) + 1, 1)
  end

  defp partition_name(%Date{year: year, month: month}) do
    "continuum_events_y#{year}_m#{String.pad_leading(Integer.to_string(month), 2, "0")}"
  end

  defp month_label(%Date{year: year, month: month}) do
    "#{year}-#{String.pad_leading(Integer.to_string(month), 2, "0")}"
  end

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")
end
