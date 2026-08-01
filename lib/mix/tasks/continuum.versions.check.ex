defmodule Mix.Tasks.Continuum.Versions.Check do
  @moduledoc """
  Verifies that the release contains every workflow version required by live runs.

      mix continuum.versions.check --repo MyApp.Repo
      mix continuum.versions.check --repo MyApp.Repo --strict
      mix continuum.versions.check --repo MyApp.Repo --format json

  `--strict` exits non-zero when any required version is missing.
  """

  use Mix.Task

  @shortdoc "Checks live-run workflow versions against the release"
  @switches [repo: :string, instance: :string, format: :string, strict: :boolean]

  @impl true
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)
    validate_args!(rest, invalid)
    Mix.Task.run("app.start")

    opts = opts |> Keyword.put(:repo, parse_repo(opts)) |> parse_instance()

    case Continuum.Versions.check(opts) do
      {:ok, report} ->
        print(report, Keyword.get(opts, :format, "text"))

        if Keyword.get(opts, :strict, false) and report.missing_count > 0 do
          Mix.raise(
            "workflow version preflight failed: #{report.missing_count} version(s) missing"
          )
        end

      {:error, reason} ->
        Mix.raise("workflow version preflight failed: #{Exception.format_banner(:error, reason)}")
    end
  end

  defp print(report, "json") do
    report =
      update_in(report.requirements, fn requirements ->
        Enum.map(requirements, fn requirement ->
          requirement
          |> Map.update!(:version_hash, &format_hash/1)
          |> Map.update!(:entrypoint, &if(&1, do: inspect(&1), else: nil))
        end)
      end)

    Mix.shell().info(Jason.encode!(report))
  end

  defp print(report, _format) do
    Mix.shell().info(
      "Continuum versions: #{report.status} required=#{report.required_count} " <>
        "loaded=#{report.loaded_count} missing=#{report.missing_count}"
    )

    Enum.each(report.requirements, fn requirement ->
      Mix.shell().info(
        "  #{requirement.status} #{requirement.workflow} " <>
          "#{format_hash(requirement.version_hash)} runs=#{requirement.run_count}" <>
          if(requirement.entrypoint, do: " -> #{inspect(requirement.entrypoint)}", else: "")
      )
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

  defp parse_instance(opts) do
    case Keyword.get(opts, :instance) do
      nil -> opts
      name -> Keyword.put(opts, :instance, String.to_existing_atom(name))
    end
  rescue
    ArgumentError -> Mix.raise("unknown Continuum instance: #{opts[:instance]}")
  end

  defp format_hash(hash) when is_binary(hash), do: Base.encode16(hash, case: :lower)
  defp format_hash(hash), do: inspect(hash)

  defp validate_args!(rest, invalid) do
    if invalid != [], do: Mix.raise("invalid options: #{inspect(invalid)}")
    if rest != [], do: Mix.raise("unexpected arguments: #{Enum.join(rest, " ")}")
  end
end
