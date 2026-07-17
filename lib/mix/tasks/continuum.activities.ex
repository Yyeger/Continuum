defmodule Mix.Tasks.Continuum.Activities do
  @moduledoc """
  Inspects activity attempt lineages and performs audited operator actions.

      mix continuum.activities --repo MyApp.Repo --action inspect --target TASK_ID
      mix continuum.activities --repo MyApp.Repo --action classify --target TASK_ID \
        --classification retryable --operator oncall --reason "upstream recovered" --execute
      mix continuum.activities --repo MyApp.Repo --action retry --target TASK_ID \
        --operator oncall --reason "upstream recovered" --max-attempts 3 --execute
      mix continuum.activities --repo MyApp.Repo --action dead-letter --target TASK_ID \
        --operator oncall --reason "invalid account" --execute

  Mutations are dry runs unless `--execute` is present.
  """

  use Mix.Task

  @shortdoc "Inspects, classifies, retries, and dead-letters activities"
  @switches [
    repo: :string,
    instance: :string,
    action: :string,
    target: :string,
    classification: :string,
    operator: :string,
    reason: :string,
    max_attempts: :integer,
    backoff: :string,
    base_ms: :integer,
    max_backoff_ms: :integer,
    max_retry_horizon_ms: :integer,
    timeout: :integer,
    format: :string,
    execute: :boolean
  ]

  @impl true
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)
    validate_args!(rest, invalid)
    Mix.Task.run("app.start")

    opts = opts |> Keyword.put(:repo, parse_repo(opts)) |> parse_instance()
    target = Keyword.get(opts, :target) || Mix.raise("--target is required")

    result =
      case Keyword.get(opts, :action, "inspect") do
        "inspect" ->
          Continuum.ActivityOperations.get(target, opts)

        "classify" ->
          classify(target, opts)

        "retry" ->
          Continuum.ActivityOperations.retry(target, retry_opts(opts))

        action when action in ["dead-letter", "dead_letter"] ->
          Continuum.ActivityOperations.dead_letter(target, opts)

        action ->
          {:error, {:unknown_action, action}}
      end

    case result do
      {:ok, value} -> print(value, Keyword.get(opts, :format, "text"))
      {:error, reason} -> Mix.raise("activity operation failed: #{inspect(reason)}")
    end
  end

  defp classify(target, opts) do
    classification =
      Keyword.get(opts, :classification) || Mix.raise("--classification is required")

    Continuum.ActivityOperations.classify(target, classification, opts)
  end

  defp retry_opts(opts) do
    retry =
      []
      |> maybe_put(:max_attempts, opts[:max_attempts])
      |> maybe_put(:backoff, parse_backoff(opts[:backoff]))
      |> maybe_put(:base_ms, opts[:base_ms])
      |> maybe_put(:max_backoff_ms, opts[:max_backoff_ms])
      |> maybe_put(:max_retry_horizon_ms, opts[:max_retry_horizon_ms])

    policy =
      []
      |> maybe_put(:retry, if(retry == [], do: nil, else: retry))
      |> maybe_put(:timeout, opts[:timeout])

    Keyword.put(opts, :policy, policy)
  end

  defp parse_backoff(nil), do: nil
  defp parse_backoff("constant"), do: :constant
  defp parse_backoff("exponential"), do: :exponential
  defp parse_backoff(value), do: Mix.raise("invalid --backoff: #{value}")

  defp maybe_put(keyword, _key, nil), do: keyword
  defp maybe_put(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp print(value, "json"), do: Mix.shell().info(Jason.encode!(value))

  defp print(%{tasks: tasks, attempts: attempts, operations: operations} = value, _format) do
    Mix.shell().info(
      "Activity lineage #{value.lineage_id}: classification=#{value.classification || "unclassified"}"
    )

    Enum.each(tasks, fn task ->
      Mix.shell().info(
        "  task #{task.task_id} state=#{task.state} attempt=#{task.attempt} parent=#{task.parent_task_id || "-"}"
      )
    end)

    Enum.each(attempts, fn attempt ->
      Mix.shell().info(
        "  attempt #{attempt.task_id}/#{attempt.attempt} outcome=#{attempt.outcome} error=#{inspect(attempt.error)}"
      )
    end)

    Enum.each(operations, fn operation ->
      Mix.shell().info(
        "  operation #{operation.action} by=#{operation.operator} reason=#{operation.reason}"
      )
    end)
  end

  defp print(value, _format) do
    suffix = if value.status == :planned, do: " (dry run; pass --execute to apply)", else: ""
    Mix.shell().info("#{value.status}: #{value.action} #{value.task_id}#{suffix}")
  end

  defp parse_instance(opts) do
    case Keyword.get(opts, :instance) do
      nil -> opts
      name -> Keyword.put(opts, :instance, String.to_existing_atom(name))
    end
  rescue
    ArgumentError -> Mix.raise("unknown Continuum instance: #{opts[:instance]}")
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

  defp validate_args!(rest, invalid) do
    if invalid != [], do: Mix.raise("invalid options: #{inspect(invalid)}")
    if rest != [], do: Mix.raise("unexpected arguments: #{Enum.join(rest, " ")}")
  end
end
