defmodule Mix.Tasks.Continuum.ArchiveContinuedChains do
  @moduledoc """
  Deletes expired non-tail runs from `continue_as_new` chains.

      mix continuum.archive_continued_chains --repo MyApp.Repo --older-than 30d
      mix continuum.archive_continued_chains --repo MyApp.Repo --older-than 30d --execute

  The task is a dry run by default. v0.4 performs deletion, not archival into a
  separate table. A run is eligible only when it is a completed non-tail cycle,
  older than the cutoff, past `retention_until`, and not part of a child chain
  whose parent is still non-terminal.
  """
  use Mix.Task

  import Ecto.Query

  alias Continuum.Schema.{
    ActivityResult,
    ActivityTask,
    Event,
    Run,
    Signal,
    Snapshot,
    Timer
  }

  @shortdoc "Deletes expired non-tail continue_as_new chain runs"
  @non_terminal_states ~w(running suspended stuck_unknown_version)

  @impl true
  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args, switches: [repo: :string, older_than: :string, execute: :boolean])

    Mix.Task.run("app.start")

    repo = parse_repo(opts)
    days = parse_older_than!(opts[:older_than])
    dry_run? = not Keyword.get(opts, :execute, false)
    run_ids = candidate_run_ids(repo, days)

    Mix.shell().info("#{dry_prefix(dry_run?)} #{length(run_ids)} continued-chain runs")

    counts = dependent_counts(repo, run_ids)

    Enum.each(counts, fn {table, count} ->
      Mix.shell().info("#{dry_prefix(dry_run?)} #{table} rows: #{count}")
    end)

    unless dry_run? do
      deleted = delete_run_ids(repo, run_ids)
      Mix.shell().info("Deleted #{deleted} continuum_runs rows")
    end
  end

  defp candidate_run_ids(repo, days) do
    %{rows: rows} =
      repo.query!(
        """
        SELECT r.id::text
        FROM continuum_runs r
        WHERE r.state = 'completed'
          AND r.completed_at < (now() - ($1::int * interval '1 day'))
          AND r.retention_until IS NOT NULL
          AND r.retention_until < now()
          AND EXISTS (
            SELECT 1
            FROM continuum_runs successor
            WHERE successor.continued_from_run_id = r.id
          )
          AND NOT EXISTS (
            SELECT 1
            FROM continuum_runs chain_run
            JOIN continuum_runs parent ON parent.id = chain_run.parent_run_id
            WHERE chain_run.correlation_id = r.correlation_id
              AND parent.state = ANY($2)
          )
        ORDER BY r.correlation_id, r.completed_at, r.id
        """,
        [days, @non_terminal_states]
      )

    Enum.map(rows, fn [run_id] -> run_id end)
  end

  defp dependent_counts(_repo, []), do: table_names() |> Enum.map(&{&1, 0})

  defp dependent_counts(repo, run_ids) do
    [
      {"continuum_events", count(repo, Event, run_ids)},
      {"continuum_snapshots", count(repo, Snapshot, run_ids)},
      {"continuum_timers", count(repo, Timer, run_ids)},
      {"continuum_signals", count(repo, Signal, run_ids)},
      {"continuum_activity_tasks", count(repo, ActivityTask, run_ids)},
      {"continuum_activity_results", count(repo, ActivityResult, run_ids)}
    ]
  end

  defp table_names do
    ~w(
      continuum_events
      continuum_snapshots
      continuum_timers
      continuum_signals
      continuum_activity_tasks
      continuum_activity_results
    )
  end

  defp count(repo, schema, run_ids) do
    repo.aggregate(from(row in schema, where: row.run_id in ^run_ids), :count)
  end

  defp delete_run_ids(_repo, []), do: 0

  defp delete_run_ids(repo, run_ids) do
    repo.transaction(fn ->
      delete_all(repo, Event, run_ids)
      delete_all(repo, Snapshot, run_ids)
      delete_all(repo, Timer, run_ids)
      delete_all(repo, Signal, run_ids)
      delete_all(repo, ActivityTask, run_ids)
      delete_all(repo, ActivityResult, run_ids)
      delete_runs(repo, run_ids)
    end)
    |> case do
      {:ok, deleted_runs} -> deleted_runs
      {:error, reason} -> Mix.raise("archive_continued_chains failed: #{inspect(reason)}")
    end
  end

  defp delete_all(repo, schema, run_ids) do
    {count, _} = repo.delete_all(from(row in schema, where: row.run_id in ^run_ids))
    count
  end

  defp delete_runs(repo, run_ids) do
    {count, _} = repo.delete_all(from(row in Run, where: row.id in ^run_ids))
    count
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

  defp parse_older_than!(nil), do: Mix.raise("pass --older-than Nd, for example --older-than 30d")

  defp parse_older_than!(value) do
    case Integer.parse(value) do
      {days, "d"} when days > 0 -> days
      _ -> Mix.raise("--older-than must be a positive day duration like 30d")
    end
  end

  defp dry_prefix(true), do: "Would delete"
  defp dry_prefix(false), do: "Deleting"
end
