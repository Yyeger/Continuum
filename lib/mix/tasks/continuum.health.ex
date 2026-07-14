defmodule Mix.Tasks.Continuum.Health do
  @moduledoc """
  Reports operational health and plans or executes fenced repairs.

      mix continuum.health --repo MyApp.Repo
      mix continuum.health --repo MyApp.Repo --format json
      mix continuum.health --repo MyApp.Repo --strict

      mix continuum.health --repo MyApp.Repo --repair wake --target RUN_ID \
        --lease-token EPOCH
      mix continuum.health --repo MyApp.Repo --repair wake --target RUN_ID \
        --lease-token EPOCH --execute

  Repairs are dry runs unless `--execute` is present.
  """

  use Mix.Task

  @shortdoc "Reports Continuum health and performs fenced repairs"
  @switches [
    repo: :string,
    format: :string,
    strict: :boolean,
    partition_months: :integer,
    lost_wake_after_ms: :integer,
    limit: :integer,
    repair: :string,
    target: :string,
    lease_token: :integer,
    lease_owner: :string,
    attempt: :integer,
    finding_type: :string,
    fingerprint: :string,
    reviewed_by: :string,
    reason: :string,
    execute: :boolean
  ]

  @impl true
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)
    validate_args!(rest, invalid)
    Mix.Task.run("app.start")
    opts = Keyword.put(opts, :repo, parse_repo(opts))

    case Keyword.get(opts, :repair) do
      nil -> report(opts)
      action -> repair(action, opts)
    end
  end

  defp report(opts) do
    case Continuum.Health.report(opts) do
      {:ok, report} ->
        print(report, Keyword.get(opts, :format, "text"))

        if Keyword.get(opts, :strict, false) and Continuum.Health.degraded?(report) do
          System.halt(1)
        end

      {:error, reason} ->
        Mix.raise("health report failed: #{Exception.format_banner(:error, reason)}")
    end
  end

  defp repair(action, opts) do
    target = Keyword.get(opts, :target) || Mix.raise("--target is required with --repair")

    case Continuum.Health.repair(action, target, opts) do
      {:ok, result} -> print(result, Keyword.get(opts, :format, "text"))
      {:error, reason} -> Mix.raise("health repair failed: #{inspect(reason)}")
    end
  end

  defp print(value, "json"), do: Mix.shell().info(Jason.encode!(value))

  defp print(report, _format) when is_map_key(report, :generated_at) do
    Mix.shell().info("Continuum health: #{report.status}")
    Mix.shell().info("generated_at: #{DateTime.to_iso8601(report.generated_at)}")

    Mix.shell().info(
      "partitions: present=#{length(report.partitions.present)} " <>
        "missing=#{length(report.partitions.missing)} horizon=#{report.partitions.horizon_end}"
    )

    Enum.each(report.partitions.missing, &Mix.shell().info("  missing #{&1}"))

    Mix.shell().info(
      "workflow_versions: loaded=#{report.workflow_versions.loaded_count} " <>
        "durable=#{report.workflow_versions.durable_count} " <>
        "missing=#{length(report.workflow_versions.missing_in_database)}"
    )

    Enum.each(report.workflow_versions.missing_in_database, fn item ->
      Mix.shell().info("  unregistered #{item.workflow} #{item.version_hash}")
    end)

    Mix.shell().info("active_runs: #{report.runs.active_count}")

    Enum.each(report.runs.groups, fn group ->
      Mix.shell().info(
        "  #{group.state}/#{group.reason}: #{group.count}, oldest=#{duration(group.oldest_age_ms)}"
      )
    end)

    Mix.shell().info("lost_wake_candidates: #{report.runs.lost_wake_count}")
    Mix.shell().info("overdue_timers: #{report.timers.overdue_count}")

    Mix.shell().info(
      "leases: active=#{report.leases.active_count} expired=#{report.leases.expired_count}"
    )

    Mix.shell().info(
      "activities: pending=#{report.activities.pending_count} " <>
        "leased=#{report.activities.leased_count} " <>
        "expired=#{report.activities.expired_lease_count} " <>
        "dead_letter=#{report.activities.dead_letter_count} " <>
        "oldest=#{duration(report.activities.oldest_task_age_ms)}"
    )

    Mix.shell().info(
      "signals: pending=#{report.signals.pending_count} " <>
        "catch_up_lag=#{duration(report.signals.catch_up_lag_ms)}"
    )
  end

  defp print(result, _format) do
    Mix.shell().info(
      "#{result.status}: #{result.action} #{result.subject_id}" <>
        if(result.status == :planned, do: " (dry run; pass --execute to apply)", else: "")
    )
  end

  defp duration(nil), do: "-"
  defp duration(ms) when ms < 1_000, do: "#{ms}ms"
  defp duration(ms) when ms < 60_000, do: "#{div(ms, 1_000)}s"
  defp duration(ms), do: "#{div(ms, 60_000)}m"

  defp parse_repo(opts) do
    case opts[:repo] do
      nil ->
        Application.get_env(:continuum, :repo) ||
          Mix.raise("no repo configured. Pass --repo MyApp.Repo or set :continuum, :repo")

      repo ->
        Module.concat([repo])
    end
  end

  defp validate_args!(rest, invalid) do
    if invalid != [], do: Mix.raise("invalid options: #{inspect(invalid)}")
    if rest != [], do: Mix.raise("unexpected arguments: #{Enum.join(rest, " ")}")
  end
end
