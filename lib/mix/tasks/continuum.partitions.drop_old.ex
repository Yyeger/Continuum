defmodule Mix.Tasks.Continuum.Partitions.DropOld do
  @moduledoc """
  Drops old `continuum_events` partitions whose rows all belong to expired runs.

      mix continuum.partitions.drop_old
      mix continuum.partitions.drop_old --older-than 180d --execute
      mix continuum.partitions.drop_old --repo MyApp.Repo --older-than 180d --execute

  The task is a dry run by default. Pass `--execute` to drop eligible
  partitions. `--older-than Nd` defaults to 180 days. A partition is eligible
  only when its upper bound is before that cutoff and the current UTC month,
  and it contains no events for runs whose `retention_until` is NULL or still
  in the future.
  """
  use Mix.Task

  @shortdoc "Drops expired continuum_events partitions"

  @switches [repo: :string, execute: :boolean, older_than: :string]
  @default_older_than_days 180

  @impl true
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)
    validate_args!(rest, invalid)
    Mix.Task.run("app.start")

    repo = parse_repo(opts)
    dry_run? = not Keyword.get(opts, :execute, false)
    older_than_days = parse_older_than(opts)
    now = DateTime.utc_now()
    current_month = current_month(now)
    cutoff_date = now |> DateTime.add(-older_than_days, :day) |> DateTime.to_date()

    expired =
      repo
      |> partitions()
      |> Enum.filter(&old_managed_partition?(&1, current_month, cutoff_date))
      |> Enum.filter(&fully_expired?(repo, &1))

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
      older_than_days: older_than_days,
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

  defp old_managed_partition?("continuum_events_y" <> rest, current_month, cutoff_date) do
    with <<year::binary-size(4), "_m", month::binary-size(2)>> <- rest,
         {year, ""} <- Integer.parse(year),
         {month, ""} <- Integer.parse(month),
         {:ok, month_start} <- Date.new(year, month, 1) do
      month_end = month_start |> Date.add(32) |> Date.beginning_of_month()

      Date.compare(month_end, current_month) != :gt and
        Date.compare(month_end, cutoff_date) != :gt
    else
      _ -> false
    end
  end

  defp old_managed_partition?(_partition, _current_month, _cutoff_date), do: false

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

  defp parse_repo(opts) do
    case opts[:repo] do
      nil ->
        Application.get_env(:continuum, :repo) ||
          Mix.raise("no repo configured. Pass --repo MyApp.Repo or set :continuum, :repo")

      repo ->
        Module.concat([repo])
    end
  end

  defp parse_older_than(opts) do
    case Keyword.get(opts, :older_than, "#{@default_older_than_days}d") do
      value when is_binary(value) ->
        case Regex.run(~r/^(\d+)d$/, value, capture: :all_but_first) do
          [days] ->
            case Integer.parse(days) do
              {days, ""} when days >= 0 -> days
              _ -> invalid_older_than!(value)
            end

          _ ->
            invalid_older_than!(value)
        end

      value ->
        invalid_older_than!(value)
    end
  end

  defp invalid_older_than!(value) do
    Mix.raise(
      "--older-than must be a non-negative duration in days such as 180d, got: #{inspect(value)}"
    )
  end

  defp validate_args!(rest, invalid) do
    if invalid != [], do: Mix.raise("invalid options: #{format_invalid(invalid)}")
    if rest != [], do: Mix.raise("unexpected arguments: #{Enum.join(rest, " ")}")
  end

  defp format_invalid(invalid) do
    Enum.map_join(invalid, ", ", fn {option, value} ->
      if is_nil(value), do: to_string(option), else: "#{option}=#{value}"
    end)
  end

  defp current_month(now) do
    today = DateTime.to_date(now)
    Date.new!(today.year, today.month, 1)
  end

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")
end
