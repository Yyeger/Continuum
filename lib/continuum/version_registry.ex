defmodule Continuum.VersionRegistry do
  @moduledoc """
  Registry for workflow version hashes and callable entrypoints.

  The hot path is process-independent: loaded workflow metadata is cached in
  `:persistent_term` so module-load/start/resume registration never depends on
  a GenServer being alive. The supervised child is only a short-lived boot task
  that upserts loaded versions into the configured instance's repo.
  """

  import Ecto.Query

  alias Continuum.Runtime.Instance
  alias Continuum.Schema.{Run, WorkflowVersion}

  @registry_key {__MODULE__, :entries}
  @snapshot_hint_key {__MODULE__, :any_snapshot_threshold}

  @type entry :: %{
          workflow: module(),
          workflow_string: String.t(),
          version: term(),
          hash: binary(),
          version_hash: binary(),
          entrypoint: module()
        }

  @doc false
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.get(opts, :instance, Continuum)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @doc false
  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    Task.start_link(fn ->
      if instance.repo do
        upsert_instance(instance, Keyword.get(opts, :workflow_modules, instance.workflow_modules))
      end
    end)
  end

  @doc since: "0.3.0"
  @doc """
  Register the current module metadata if the module is a Continuum workflow.
  """
  @spec ensure_registered(module()) :: {:ok, entry()} | {:error, term()}
  def ensure_registered(module) when is_atom(module) do
    cond do
      function_exported?(module, :__continuum_workflow__, 0) ->
        register_loaded(module)

      match?({:module, ^module}, Code.ensure_loaded(module)) ->
        register_loaded(module)

      true ->
        {:error, :not_a_workflow}
    end
  end

  @doc since: "0.4.0"
  @doc """
  Return the loaded workflow-version entries currently known to this BEAM.
  """
  @spec entries() :: [entry()]
  def entries do
    entry_map()
    |> Map.values()
  end

  @doc since: "0.3.0"
  @doc """
  Backwards-compatible registration helper for tests and old callers.
  """
  @spec register(module(), term(), binary()) :: :ok
  def register(module, version, hash) do
    register(module, version, hash, module)
  end

  @doc since: "0.3.0"
  @doc """
  Register a logical workflow/version hash to a concrete entrypoint module.
  """
  @spec register(module(), term(), binary(), module()) :: :ok
  def register(workflow, version, hash, entrypoint) do
    put_entry(%{
      workflow: workflow,
      workflow_string: inspect(workflow),
      version: version,
      hash: hash,
      version_hash: hash,
      entrypoint: entrypoint
    })

    :ok
  end

  @doc since: "0.3.0"
  @doc """
  Look up registered metadata for a workflow module.

  This preserves the v0.1 helper shape for callers that only have one loaded
  version. When multiple entrypoints are registered for the same logical
  workflow, there is intentionally no "latest" ordering in the content-addressed
  registry; resume dispatch must use `resolve/2` with the journaled hash.
  """
  @spec lookup(module()) :: nil | map()
  def lookup(module) when is_atom(module) do
    case ensure_registered(module) do
      {:ok, entry} ->
        entry

      {:error, _reason} ->
        entry_map()
        |> Map.values()
        |> Enum.find(&(&1.workflow == module))
    end
  end

  @doc since: "0.3.0"
  @doc """
  Resolve a journaled `(workflow, version_hash)` pair to a loaded entrypoint.
  """
  @spec resolve(module() | String.t(), binary()) :: {:ok, entry()} | {:error, term()}
  def resolve(workflow, version_hash) do
    workflow_string = workflow_string(workflow)

    case Map.get(entry_map(), {workflow_string, version_hash}) ||
           discover(workflow_string, version_hash) do
      nil ->
        {:error, :unknown_version}

      %{entrypoint: entrypoint} = entry ->
        case Code.ensure_loaded(entrypoint) do
          {:module, ^entrypoint} -> {:ok, entry}
          _ -> {:error, :unknown_version}
        end
    end
  end

  @doc since: "0.3.0"
  @doc """
  Upsert loaded workflow versions for an instance into `continuum_workflow_versions`.
  """
  @spec upsert_instance(Instance.t(), [module()] | nil) :: :ok
  def upsert_instance(instance, workflow_modules \\ nil)

  def upsert_instance(%Instance{repo: nil}, _workflow_modules), do: :ok

  def upsert_instance(%Instance{} = instance, workflow_modules) do
    rows =
      workflow_modules
      |> configured_modules()
      |> Enum.flat_map(fn module ->
        case ensure_registered(module) do
          {:ok, entry} -> [entry]
          {:error, _} -> []
        end
      end)
      |> Enum.uniq_by(&{&1.workflow_string, &1.version_hash})
      |> Enum.map(&workflow_version_row/1)

    if rows != [] do
      instance.repo.insert_all(WorkflowVersion, rows,
        on_conflict: {:replace, [:entrypoint, :registered_at]},
        conflict_target: [:workflow, :version_hash]
      )

      recover_stuck_runs(instance, rows)
    end

    :ok
  rescue
    error in Postgrex.Error ->
      if missing_workflow_versions_table?(error), do: :ok, else: reraise(error, __STACKTRACE__)
  end

  # Runs marked stuck_unknown_version by pre-0.5.2 nodes become runnable the
  # moment their version is registered again: flip them back to suspended with
  # a cleared lease so the dispatcher can claim them.
  defp recover_stuck_runs(instance, rows) do
    Enum.each(rows, fn %{workflow: workflow, version_hash: version_hash} ->
      instance.repo.update_all(
        from(r in Run,
          where:
            r.state == "stuck_unknown_version" and r.workflow == ^workflow and
              r.version_hash == ^version_hash
        ),
        set: [
          state: "suspended",
          error: nil,
          lease_owner: nil,
          lease_token: nil,
          lease_expires_at: nil,
          next_wakeup_at: nil
        ]
      )
    end)
  end

  defp configured_modules(nil) do
    case Application.get_env(:continuum, :workflow_modules) do
      modules when is_list(modules) and modules != [] -> modules
      _ -> loaded_workflow_modules()
    end
  end

  defp configured_modules(modules) when is_list(modules), do: modules

  defp loaded_workflow_modules do
    :code.all_loaded()
    |> Enum.map(fn {module, _path} -> module end)
    |> Enum.filter(&function_exported?(&1, :__continuum_workflow__, 0))
  end

  defp workflow_metadata(module) do
    if function_exported?(module, :__continuum_workflow__, 0) do
      metadata = module.__continuum_workflow__()
      workflow = Map.get(metadata, :module, module)
      entrypoint = Map.get(metadata, :entrypoint, module)

      {:ok,
       %{
         workflow: workflow,
         workflow_string: inspect(workflow),
         version: Map.get(metadata, :version),
         hash: Map.fetch!(metadata, :version_hash),
         version_hash: Map.fetch!(metadata, :version_hash),
         entrypoint: entrypoint
       }}
    else
      {:error, :not_a_workflow}
    end
  end

  defp register_loaded(module) do
    case workflow_metadata(module) do
      {:ok, metadata} -> {:ok, put_entry(metadata)}
      {:error, _} = error -> error
    end
  end

  defp put_entry(%{workflow_string: workflow, version_hash: hash} = entry) do
    :persistent_term.put(@registry_key, Map.put(entry_map(), {workflow, hash}, entry))
    maybe_flag_snapshot_threshold(entry)
    entry
  end

  # Sticky fast-path hint for the Snapshotter: with the app-level threshold at
  # :infinity, the per-event maybe_snapshot cast only pays the run lookup when
  # at least one registered entrypoint declares its own snapshot_threshold.
  @doc false
  def any_snapshot_threshold? do
    :persistent_term.get(@snapshot_hint_key, false)
  end

  defp maybe_flag_snapshot_threshold(%{entrypoint: entrypoint}) do
    with false <- any_snapshot_threshold?(),
         true <- function_exported?(entrypoint, :__continuum_workflow__, 0),
         threshold when not is_nil(threshold) <-
           Map.get(entrypoint.__continuum_workflow__(), :snapshot_threshold) do
      :persistent_term.put(@snapshot_hint_key, true)
    end

    :ok
  end

  defp discover(workflow_string, version_hash) do
    loaded_workflow_modules()
    |> Enum.find_value(fn module ->
      case ensure_registered(module) do
        {:ok, %{workflow_string: ^workflow_string, version_hash: ^version_hash} = entry} -> entry
        _ -> nil
      end
    end)
  end

  defp entry_map do
    :persistent_term.get(@registry_key, %{})
  end

  defp workflow_string(workflow) when is_atom(workflow), do: inspect(workflow)
  defp workflow_string(workflow) when is_binary(workflow), do: workflow

  defp workflow_version_row(entry) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %{
      workflow: entry.workflow_string,
      version_hash: entry.version_hash,
      entrypoint: inspect(entry.entrypoint),
      registered_at: now
    }
  end

  defp missing_workflow_versions_table?(%Postgrex.Error{postgres: %{code: :undefined_table}}),
    do: true

  defp missing_workflow_versions_table?(_error), do: false
end
