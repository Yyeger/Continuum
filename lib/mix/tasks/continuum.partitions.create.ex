defmodule Mix.Tasks.Continuum.Partitions.Create do
  @moduledoc """
  Ensures a horizon of monthly `continuum_events` partitions.

      mix continuum.partitions.create --months 3
      mix continuum.partitions.create 2026-06 --months 3
      mix continuum.partitions.create 2026-06 --months 3 --repo MyApp.Repo --execute

  The task is idempotent and a dry run by default. `--months N` means N
  consecutive partitions beginning with the positional month, or the current
  UTC month when omitted. Pass `--execute` to create them.
  """
  use Mix.Task

  @shortdoc "Ensures a horizon of continuum_events partitions"

  @switches [repo: :string, months: :integer, execute: :boolean]
  @max_months 120

  @impl true
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)
    validate_args!(rest, invalid)
    Mix.Task.run("app.start")

    repo = parse_repo(opts)
    start_month = parse_month(List.first(rest))
    months = parse_months(opts)
    dry_run? = not Keyword.get(opts, :execute, false)

    common_opts = [repo: repo, months: months]

    common_opts =
      if start_month, do: Keyword.put(common_opts, :start_month, start_month), else: common_opts

    {partitions, existing, moved_row_count} =
      if dry_run? do
        plan = unwrap!(Continuum.Partitions.plan(common_opts))
        Enum.each(plan.missing, &Mix.shell().info("Would create #{&1}"))
        Enum.each(plan.present, &Mix.shell().info("Exists #{&1}"))
        {plan.missing, plan.present, 0}
      else
        summary = unwrap!(Continuum.Partitions.ensure(common_opts))
        Enum.each(summary.created, &Mix.shell().info("Created #{&1}"))
        Enum.each(summary.existing, &Mix.shell().info("Exists #{&1}"))

        if summary.default_created? do
          Mix.shell().info("Created #{summary.default_partition}")
        end

        if summary.moved_row_count > 0 do
          Mix.shell().info(
            "Moved #{summary.moved_row_count} overflow rows into monthly partitions"
          )
        end

        {summary.created, summary.existing, summary.moved_row_count}
      end

    :telemetry.execute([:continuum, :partition, :created], %{count: length(partitions)}, %{
      dry_run?: dry_run?,
      months: months,
      partitions: partitions,
      existing: existing,
      moved_row_count: moved_row_count
    })
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

  defp parse_months(opts) do
    case Keyword.get(opts, :months, 1) do
      months when is_integer(months) and months > 0 and months <= @max_months ->
        months

      months ->
        Mix.raise(
          "--months must be an integer between 1 and #{@max_months}, got: #{inspect(months)}"
        )
    end
  end

  defp validate_args!(rest, invalid) do
    if invalid != [], do: Mix.raise("invalid options: #{format_invalid(invalid)}")
    if length(rest) > 1, do: Mix.raise("expected at most one YYYY-MM month argument")
  end

  defp format_invalid(invalid) do
    Enum.map_join(invalid, ", ", fn {option, value} ->
      if is_nil(value), do: to_string(option), else: "#{option}=#{value}"
    end)
  end

  defp parse_month(nil), do: nil

  defp parse_month(<<year::binary-size(4), "-", month::binary-size(2)>>) do
    with {year, ""} <- Integer.parse(year),
         {month, ""} <- Integer.parse(month),
         {:ok, date} <- Date.new(year, month, 1) do
      date
    else
      _ -> Mix.raise("month must be in YYYY-MM format")
    end
  end

  defp parse_month(_), do: Mix.raise("month must be in YYYY-MM format")

  defp unwrap!({:ok, value}), do: value

  defp unwrap!({:error, reason}),
    do: Mix.raise("partition maintenance failed: #{inspect(reason)}")
end
