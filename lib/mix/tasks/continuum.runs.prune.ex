defmodule Mix.Tasks.Continuum.Runs.Prune do
  @moduledoc """
  Safely prunes expired terminal workflow histories in bounded batches.

      mix continuum.runs.prune --repo MyApp.Repo
      mix continuum.runs.prune --repo MyApp.Repo --batch-size 100 --execute
      mix continuum.runs.prune --repo MyApp.Repo --idempotency-policy delete --execute

  The task is a dry run by default. Idempotency records are retained unless an
  operator explicitly selects `delete`.
  """

  use Mix.Task

  @shortdoc "Prunes expired terminal run histories"
  @switches [
    repo: :string,
    batch_size: :integer,
    older_than: :string,
    idempotency_policy: :string,
    execute: :boolean
  ]

  @impl true
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)
    validate_args!(rest, invalid)
    Mix.Task.run("app.start")

    opts =
      opts
      |> Keyword.put(:repo, parse_repo(opts))
      |> Keyword.put(:older_than_days, parse_older_than(opts[:older_than]))
      |> Keyword.put(:idempotency_policy, parse_idempotency_policy(opts[:idempotency_policy]))

    case Continuum.Pruner.plan(opts) do
      {:ok, plan} ->
        print_plan(plan, Keyword.get(opts, :execute, false))

        if Keyword.get(opts, :execute, false) do
          case Continuum.Pruner.execute(plan, opts) do
            {:ok, result} ->
              Mix.shell().info(
                "Deleted #{result.deleted_run_count} runs and " <>
                  "#{result.deleted_idempotency_count} idempotency records"
              )

            {:error, reason} ->
              Mix.raise("run pruning failed: #{inspect(reason)}")
          end
        end

      {:error, reason} ->
        Mix.raise("run pruning plan failed: #{inspect(reason)}")
    end
  end

  defp print_plan(plan, execute?) do
    prefix = if execute?, do: "Pruning", else: "Would prune"

    Mix.shell().info(
      "#{prefix} #{plan.chain_count} logical chains / #{plan.run_count} runs " <>
        "(idempotency=#{plan.idempotency_policy})"
    )

    Enum.each(plan.chains, fn chain ->
      Mix.shell().info("  #{chain.logical_id}: #{length(chain.run_ids)} runs")
    end)

    Enum.each(plan.dependent_counts, fn {name, count} ->
      Mix.shell().info("  #{name}: #{count}")
    end)
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

  defp parse_older_than(nil), do: 0

  defp parse_older_than(value) do
    case Regex.run(~r/^(\d+)d$/, value, capture: :all_but_first) do
      [days] -> String.to_integer(days)
      _ -> Mix.raise("--older-than must be a non-negative day duration such as 30d")
    end
  end

  defp parse_idempotency_policy(nil), do: :keep
  defp parse_idempotency_policy("keep"), do: :keep
  defp parse_idempotency_policy("delete"), do: :delete

  defp parse_idempotency_policy(value) do
    Mix.raise("--idempotency-policy must be keep or delete, got: #{inspect(value)}")
  end

  defp validate_args!(rest, invalid) do
    if invalid != [], do: Mix.raise("invalid options: #{inspect(invalid)}")
    if rest != [], do: Mix.raise("unexpected arguments: #{Enum.join(rest, " ")}")
  end
end
