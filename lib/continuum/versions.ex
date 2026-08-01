defmodule Continuum.Versions do
  @moduledoc """
  Deployment preflight for workflow versions required by live runs.

  A release is ready only when every distinct `(workflow, version_hash)` used
  by a non-terminal run resolves to a loaded entrypoint in the current BEAM.
  """

  import Ecto.Query

  alias Continuum.Runtime.Instance
  alias Continuum.Schema.Run

  @non_terminal_states ~w(running suspended stuck_unknown_version)

  @type requirement :: %{
          workflow: String.t(),
          version_hash: binary(),
          run_count: non_neg_integer(),
          oldest_started_at: DateTime.t() | nil,
          entrypoint: module() | nil,
          status: :loaded | :missing
        }

  @type report :: %{
          status: :ready | :missing_versions,
          required_count: non_neg_integer(),
          loaded_count: non_neg_integer(),
          missing_count: non_neg_integer(),
          requirements: [requirement()]
        }

  @doc """
  Compares non-terminal run versions in Postgres with loaded workflow
  entrypoints for a Continuum instance.
  """
  @spec check(keyword()) :: {:ok, report()} | {:error, term()}
  def check(opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      manifest = loaded_manifest(instance)

      requirements =
        instance.repo.all(
          from(r in Run,
            where: r.state in ^@non_terminal_states,
            group_by: [r.workflow, r.version_hash],
            order_by: [asc: r.workflow, asc: r.version_hash],
            select: %{
              workflow: r.workflow,
              version_hash: r.version_hash,
              run_count: count(r.id),
              oldest_started_at: min(r.started_at)
            }
          )
        )
        |> Enum.map(&classify_requirement(&1, manifest))

      missing_count = Enum.count(requirements, &(&1.status == :missing))
      required_count = length(requirements)

      {:ok,
       %{
         status: if(missing_count == 0, do: :ready, else: :missing_versions),
         required_count: required_count,
         loaded_count: required_count - missing_count,
         missing_count: missing_count,
         requirements: requirements
       }}
    end
  rescue
    error -> {:error, error}
  end

  defp loaded_manifest(instance) do
    # Force configured workflow modules to register, then include every
    # discovered generated entrypoint that is actually loadable in this BEAM.
    configured = Continuum.VersionRegistry.entries(instance)

    (configured ++ Continuum.VersionRegistry.entries())
    |> Enum.uniq_by(&{&1.workflow_string, &1.version_hash})
    |> Enum.reduce(%{}, fn entry, manifest ->
      case Code.ensure_loaded(entry.entrypoint) do
        {:module, _module} ->
          Map.put(manifest, {entry.workflow_string, entry.version_hash}, entry.entrypoint)

        _other ->
          manifest
      end
    end)
  end

  defp classify_requirement(requirement, manifest) do
    case Map.get(manifest, {requirement.workflow, requirement.version_hash}) do
      nil -> Map.merge(requirement, %{entrypoint: nil, status: :missing})
      entrypoint -> Map.merge(requirement, %{entrypoint: entrypoint, status: :loaded})
    end
  end

  defp repo_instance(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    instance = if repo = opts[:repo], do: %{instance | repo: repo}, else: instance

    case instance.repo do
      nil -> {:error, :repo_not_configured}
      _repo -> {:ok, instance}
    end
  end
end
