defmodule Mix.Tasks.Continuum.GcVersions do
  @moduledoc """
  Lists or deletes unreferenced workflow-version registry rows.

      mix continuum.gc_versions --repo MyApp.Repo
      mix continuum.gc_versions --repo MyApp.Repo --execute

  The task is a dry run by default. A workflow-version row is deletable only
  when no non-terminal run references it and it is not one of the loaded
  versions registered in the current BEAM.
  """
  use Mix.Task

  import Ecto.Query

  alias Continuum.Schema.{Run, WorkflowVersion}

  @shortdoc "Prunes unreferenced continuum_workflow_versions rows"
  @non_terminal_states ~w(running suspended stuck_unknown_version)

  @impl true
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string, execute: :boolean])
    Mix.Task.run("app.start")

    repo = parse_repo(opts)
    dry_run? = not Keyword.get(opts, :execute, false)
    loaded = loaded_hashes_by_workflow()
    pinned = pinned_hashes_by_workflow(repo)

    candidates =
      repo.all(from(v in WorkflowVersion, order_by: [asc: v.workflow, asc: v.registered_at]))
      |> Enum.group_by(& &1.workflow)
      |> Enum.flat_map(fn {workflow, versions} ->
        classify_workflow(workflow, versions, loaded, pinned, dry_run?)
      end)

    if dry_run? do
      Mix.shell().info("Dry run: #{length(candidates)} workflow_versions rows would be deleted")
    else
      deleted = delete_candidates(repo, candidates)
      Mix.shell().info("Deleted #{deleted} workflow_versions rows")
    end
  end

  defp classify_workflow(workflow, versions, loaded, pinned, dry_run?) do
    loaded_hashes = Map.get(loaded, workflow, MapSet.new())
    pinned_hashes = Map.get(pinned, workflow, MapSet.new())

    if MapSet.size(loaded_hashes) == 0 do
      Mix.shell().info(
        "#{workflow}: skipped #{length(versions)} versions because no loaded version is registered"
      )

      []
    else
      candidates =
        Enum.reject(versions, fn version ->
          MapSet.member?(loaded_hashes, version.version_hash) or
            MapSet.member?(pinned_hashes, version.version_hash)
        end)

      Mix.shell().info(
        "#{workflow}: #{length(versions)} versions, #{MapSet.size(loaded_hashes)} loaded, " <>
          "#{MapSet.size(pinned_hashes)} pinned by non-terminal runs, " <>
          "#{length(candidates)} candidates"
      )

      Enum.each(candidates, fn version ->
        prefix = if dry_run?, do: "Would delete", else: "Delete"

        Mix.shell().info(
          "#{prefix} #{version.workflow} #{format_hash(version.version_hash)} -> #{version.entrypoint}"
        )
      end)

      candidates
    end
  end

  defp loaded_hashes_by_workflow do
    Continuum.VersionRegistry.entries()
    |> Enum.group_by(& &1.workflow_string, & &1.version_hash)
    |> Map.new(fn {workflow, hashes} -> {workflow, MapSet.new(hashes)} end)
  end

  defp pinned_hashes_by_workflow(repo) do
    rows =
      repo.all(
        from(r in Run,
          where: r.state in ^@non_terminal_states,
          select: {r.workflow, r.version_hash},
          distinct: true
        )
      )

    rows
    |> Enum.group_by(fn {workflow, _hash} -> workflow end, fn {_workflow, hash} -> hash end)
    |> Map.new(fn {workflow, hashes} -> {workflow, MapSet.new(hashes)} end)
  end

  defp delete_candidates(repo, candidates) do
    Enum.reduce(candidates, 0, fn candidate, count ->
      {deleted, _} =
        repo.delete_all(
          from(v in WorkflowVersion,
            where:
              v.workflow == ^candidate.workflow and
                v.version_hash == ^candidate.version_hash
          )
        )

      count + deleted
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

  defp format_hash(hash) when is_binary(hash) do
    if String.printable?(hash) and byte_size(hash) <= 80 do
      hash
    else
      Base.encode16(hash, case: :lower)
    end
  end

  defp format_hash(hash), do: inspect(hash)
end
