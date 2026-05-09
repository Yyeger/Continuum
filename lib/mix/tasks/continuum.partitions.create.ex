defmodule Mix.Tasks.Continuum.Partitions.Create do
  @moduledoc """
  Creates a monthly `continuum_events` partition.

      mix continuum.partitions.create
      mix continuum.partitions.create 2026-06
      mix continuum.partitions.create 2026-06 --repo MyApp.Repo

  The task is idempotent. Without a month argument it creates the current
  UTC month partition.
  """
  use Mix.Task

  @shortdoc "Creates a monthly continuum_events partition"

  @impl true
  def run(args) do
    {opts, rest, _} = OptionParser.parse(args, switches: [repo: :string])
    Mix.Task.run("app.start")

    repo = parse_repo(opts)
    month = parse_month(List.first(rest))
    create_partition(repo, month)

    Mix.shell().info("Created #{partition_name(month)}")
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

  defp partition_name(%Date{year: year, month: month}) do
    "continuum_events_y#{year}_m#{pad2(month)}"
  end

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")

  defp pad2(month) when month < 10, do: "0#{month}"
  defp pad2(month), do: "#{month}"
end
