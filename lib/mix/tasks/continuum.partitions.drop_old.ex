defmodule Mix.Tasks.Continuum.Partitions.DropOld do
  @moduledoc """
  Drops old `continuum_events` partitions whose rows all belong to expired runs.

      mix continuum.partitions.drop_old
      mix continuum.partitions.drop_old --execute
      mix continuum.partitions.drop_old --repo MyApp.Repo --execute

  The task is a dry run by default. Pass `--execute` to drop eligible
  partitions. A partition is eligible only when it is before the current UTC
  month and it contains no events for runs whose `retention_until` is NULL or
  still in the future.
  """
  use Mix.Task

  @shortdoc "Drops expired continuum_events partitions"

  @impl true
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string, execute: :boolean])
    Mix.Task.run("app.start")

    repo = parse_repo(opts)
    dry_run? = not Keyword.get(opts, :execute, false)
    current_month = current_month()

    expired =
      repo
      |> partitions()
      |> Enum.filter(&old_managed_partition?(&1, current_month))
      |> Enum.filter(&fully_expired?(repo, &1))

    cleanup_activity_results(repo, dry_run?)

    Enum.each(expired, fn partition ->
      if dry_run? do
        Mix.shell().info("Would drop #{partition}")
      else
        repo.query!("DROP TABLE #{quote_ident(partition)}")
        Mix.shell().info("Dropped #{partition}")
      end
    end)

    :telemetry.execute([:continuum, :partition, :dropped], %{count: length(expired)}, %{
      dry_run?: dry_run?,
      partitions: expired
    })
  end

  defp partitions(repo) do
    %{rows: rows} =
      repo.query!("""
      SELECT c.relname
      FROM pg_inherits i
      JOIN pg_class c ON c.oid = i.inhrelid
      JOIN pg_class p ON p.oid = i.inhparent
      WHERE p.relname = 'continuum_events'
      ORDER BY c.relname
      """)

    Enum.map(rows, fn [name] -> name end)
  end

  defp old_managed_partition?("continuum_events_y" <> rest, current_month) do
    with <<year::binary-size(4), "_m", month::binary-size(2)>> <- rest,
         {year, ""} <- Integer.parse(year),
         {month, ""} <- Integer.parse(month),
         {:ok, month_start} <- Date.new(year, month, 1) do
      month_end = month_start |> Date.add(32) |> Date.beginning_of_month()
      Date.compare(month_end, current_month) != :gt
    else
      _ -> false
    end
  end

  defp old_managed_partition?(_partition, _current_month), do: false

  defp fully_expired?(repo, partition) do
    sql = """
    SELECT NOT EXISTS (
      SELECT 1
      FROM ONLY #{quote_ident(partition)} e
      JOIN continuum_runs r ON r.id = e.run_id
      WHERE r.retention_until IS NULL OR r.retention_until >= now()
    )
    """

    %{rows: [[expired?]]} = repo.query!(sql)
    expired?
  end

  defp cleanup_activity_results(repo, true) do
    if table_exists?(repo, "continuum_activity_results") do
      %{rows: [[count]]} =
        repo.query!("""
        SELECT count(*)
        FROM continuum_activity_results ar
        JOIN continuum_runs r ON r.id = ar.run_id
        WHERE r.retention_until < now()
        """)

      Mix.shell().info("Would clean #{count} activity_results rows")
    end
  end

  defp cleanup_activity_results(repo, false) do
    if table_exists?(repo, "continuum_activity_results") do
      %{num_rows: count} =
        repo.query!("""
        DELETE FROM continuum_activity_results ar
        USING continuum_runs r
        WHERE ar.run_id = r.id AND r.retention_until < now()
        """)

      Mix.shell().info("Cleaned #{count} activity_results rows")
    end
  end

  defp table_exists?(repo, table) do
    %{rows: [[exists?]]} = repo.query!("SELECT to_regclass($1) IS NOT NULL", [table])
    exists?
  end

  defp parse_repo(opts) do
    case opts[:repo] do
      nil ->
        Application.get_env(:continuum, :repo) ||
          Mix.raise("no repo configured. Pass --repo MyApp.Repo or set :continuum, :repo")

      repo ->
        Module.concat([repo])
    end
  end

  defp current_month do
    today = Date.utc_today()
    Date.new!(today.year, today.month, 1)
  end

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")
end
