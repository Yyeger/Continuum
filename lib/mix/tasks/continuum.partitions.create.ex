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
    month = parse_month(List.first(rest))
    months = parse_months(opts)
    dry_run? = not Keyword.get(opts, :execute, false)

    partitions = Enum.map(0..(months - 1), &shift_month(month, &1))

    Enum.each(partitions, fn partition_month ->
      if dry_run? do
        Mix.shell().info("Would create #{partition_name(partition_month)}")
      else
        create_partition(repo, partition_month)
        Mix.shell().info("Created #{partition_name(partition_month)}")
      end
    end)

    :telemetry.execute([:continuum, :partition, :created], %{count: length(partitions)}, %{
      dry_run?: dry_run?,
      months: months,
      partitions: Enum.map(partitions, &partition_name/1)
    })
  end

  defp create_partition(repo, month) do
    next_month = month |> Date.add(32) |> Date.beginning_of_month()

    repo.query!("""
    CREATE TABLE IF NOT EXISTS #{quote_ident(partition_name(month))}
    PARTITION OF continuum_events
    FOR VALUES FROM ('#{Date.to_iso8601(month)} 00:00:00+00')
    TO ('#{Date.to_iso8601(next_month)} 00:00:00+00')
    """)
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

  defp parse_month(nil) do
    today = Date.utc_today()
    Date.new!(today.year, today.month, 1)
  end

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

  defp shift_month(%Date{} = month, offset) do
    absolute_month = month.year * 12 + month.month - 1 + offset
    Date.new!(div(absolute_month, 12), rem(absolute_month, 12) + 1, 1)
  end

  defp partition_name(%Date{year: year, month: month}) do
    "continuum_events_y#{year}_m#{pad2(month)}"
  end

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")

  defp pad2(month) when month < 10, do: "0#{month}"
  defp pad2(month), do: "#{month}"
end
