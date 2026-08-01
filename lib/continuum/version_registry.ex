defmodule Continuum.VersionRegistry do
  @moduledoc """
  Registry for workflow version hashes and callable entrypoints.

  Loaded workflow metadata is cached in `:persistent_term`. Each durable
  instance also owns a registrar process that upserts configured versions at
  boot, retries transient failures, and records versions discovered on first
  use.
  """

  use GenServer

  import Ecto.Query
  require Logger

  alias Continuum.Runtime.Instance
  alias Continuum.Schema.{Run, WorkflowVersion}
  alias Continuum.Telemetry

  @registry_key {__MODULE__, :entries}
  @snapshot_hint_key {__MODULE__, :any_snapshot_threshold}
  @default_retry_base_ms 1_000
  @default_retry_max_ms 30_000

  @type entry :: %{
          workflow: module(),
          workflow_string: String.t(),
          version: term(),
          hash: binary(),
          version_hash: binary(),
          entrypoint: module(),
          retention: non_neg_integer() | :infinity
        }

  @doc false
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.get(opts, :instance, Continuum)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  @doc false
  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    GenServer.start_link(__MODULE__, {instance, opts}, name: server_name(instance))
  end

  @impl true
  def init({instance, opts}) do
    state = %{
      instance: instance,
      workflow_modules: Keyword.get(opts, :workflow_modules, instance.workflow_modules),
      registration_fun: Keyword.get(opts, :registration_fun, &persist_entries/2),
      retry_base_ms: Keyword.get(opts, :retry_base_ms, @default_retry_base_ms),
      retry_max_ms: Keyword.get(opts, :retry_max_ms, @default_retry_max_ms),
      retry_attempt: 0,
      retry_ref: nil,
      registered: MapSet.new(),
      pending: %{},
      state: :starting,
      last_error: nil,
      last_success_at: nil
    }

    {:ok, state, {:continue, :register_boot}}
  end

  @impl true
  def handle_continue(:register_boot, state) do
    entries = configured_entries(state.workflow_modules)
    {:noreply, register_entries(state, entries)}
  end

  @impl true
  def handle_call(:status, _from, state), do: {:reply, status_map(state), state}

  def handle_call({:ensure_durable, entries}, _from, state) do
    missing = Enum.reject(entries, &MapSet.member?(state.registered, entry_key(&1)))

    case missing do
      [] ->
        {:reply, :ok, state}

      entries ->
        state = register_entries(state, entries)

        case state.state do
          :ready -> {:reply, :ok, state}
          :degraded -> {:reply, {:error, {:registration_failed, state.last_error}}, state}
        end
    end
  end

  @impl true
  def handle_info(:retry_registration, state) do
    entries =
      state.pending |> Map.values() |> Kernel.++(configured_entries(state.workflow_modules))

    {:noreply, register_entries(%{state | retry_ref: nil}, entries)}
  end

  def handle_info(_message, state), do: {:noreply, state}

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

  @doc since: "0.6.2"
  @doc """
  Register workflow metadata locally and ensure it is durable for an instance.
  """
  @spec ensure_registered(module(), Instance.t()) :: {:ok, entry()} | {:error, term()}
  def ensure_registered(module, %Instance{} = instance) when is_atom(module) do
    with {:ok, entry} <- ensure_registered(module),
         :ok <- ensure_durable(instance, [entry]) do
      {:ok, entry}
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
      entrypoint: entrypoint,
      retention: :infinity
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

  @doc since: "0.6.2"
  @doc """
  Resolve a workflow version and ensure the discovered entry is durable for an
  instance.
  """
  @spec resolve(module() | String.t(), binary(), Instance.t()) ::
          {:ok, entry()} | {:error, term()}
  def resolve(workflow, version_hash, %Instance{} = instance) do
    with {:ok, entry} <- resolve(workflow, version_hash),
         :ok <- ensure_durable(instance, [entry]) do
      {:ok, entry}
    end
  end

  @doc since: "0.6.2"
  @doc "Return the durable registrar status for an instance."
  @spec status(Instance.t() | atom()) :: map()
  def status(instance_or_name \\ Continuum) do
    instance = Instance.lookup(instance_or_name)

    case GenServer.whereis(server_name(instance)) do
      nil ->
        %{state: :not_running, registered_count: 0, pending_count: 0, last_error: nil}

      _pid ->
        GenServer.call(server_name(instance), :status)
    end
  catch
    :exit, _ -> %{state: :not_running, registered_count: 0, pending_count: 0, last_error: nil}
  end

  @doc since: "0.3.0"
  @doc """
  Upsert loaded workflow versions for an instance into `continuum_workflow_versions`.
  """
  @spec upsert_instance(Instance.t(), [module()] | nil) :: :ok
  def upsert_instance(instance, workflow_modules \\ nil)

  def upsert_instance(%Instance{repo: nil}, _workflow_modules), do: :ok

  def upsert_instance(%Instance{} = instance, workflow_modules) do
    persist_entries(instance, configured_entries(workflow_modules))
  rescue
    error in Postgrex.Error ->
      if missing_workflow_versions_table?(error), do: :ok, else: reraise(error, __STACKTRACE__)
  end

  defp ensure_durable(%Instance{repo: nil}, _entries), do: :ok

  defp ensure_durable(%Instance{} = instance, entries) do
    case GenServer.whereis(server_name(instance)) do
      nil -> safe_persist_entries(instance, entries)
      _pid -> GenServer.call(server_name(instance), {:ensure_durable, entries}, 15_000)
    end
  catch
    :exit, _ -> safe_persist_entries(instance, entries)
  end

  defp safe_persist_entries(instance, entries) do
    persist_entries(instance, entries)
  rescue
    error -> {:error, {:registration_failed, Exception.message(error)}}
  catch
    kind, reason -> {:error, {:registration_failed, Exception.format_banner(kind, reason)}}
  end

  defp configured_entries(workflow_modules) do
    workflow_modules
    |> configured_modules()
    |> Enum.flat_map(fn module ->
      case ensure_registered(module) do
        {:ok, entry} -> [entry]
        {:error, _} -> []
      end
    end)
    |> Enum.uniq_by(&entry_key/1)
  end

  defp persist_entries(%Instance{repo: nil}, _entries), do: :ok

  defp persist_entries(%Instance{} = instance, entries) do
    rows = Enum.map(entries, &workflow_version_row/1)

    if rows != [] do
      instance.repo.insert_all(WorkflowVersion, rows,
        on_conflict: {:replace, [:entrypoint, :registered_at]},
        conflict_target: [:workflow, :version_hash]
      )

      recover_stuck_runs(instance, rows)
    end

    :ok
  end

  defp register_entries(state, entries) do
    entries =
      (entries ++ Map.values(state.pending))
      |> Enum.uniq_by(&entry_key/1)

    case invoke_registration(state.registration_fun, state.instance, entries) do
      :ok -> registration_succeeded(state, entries)
      {:error, error} -> registration_failed(state, entries, error)
    end
  end

  defp invoke_registration(registration_fun, instance, entries) do
    case registration_fun.(instance, entries) do
      :ok -> :ok
      {:error, _} = error -> error
      other -> {:error, {:unexpected_registration_result, other}}
    end
  rescue
    error -> {:error, Exception.message(error)}
  catch
    kind, reason -> {:error, Exception.format_banner(kind, reason)}
  end

  defp registration_succeeded(state, entries) do
    cancel_retry(state.retry_ref)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    Telemetry.execute([:continuum, :version_registry, :registered], %{count: length(entries)}, %{
      instance: state.instance.name
    })

    %{
      state
      | state: :ready,
        registered: Enum.reduce(entries, state.registered, &MapSet.put(&2, entry_key(&1))),
        pending: %{},
        retry_attempt: 0,
        retry_ref: nil,
        last_error: nil,
        last_success_at: now
    }
  end

  defp registration_failed(state, entries, error) do
    cancel_retry(state.retry_ref)
    delay = retry_delay(state)
    retry_ref = Process.send_after(self(), :retry_registration, delay)
    error = inspect(error)

    Logger.warning(
      "Continuum workflow version registration failed for #{inspect(state.instance.name)}; " <>
        "retrying in #{delay}ms: #{error}"
    )

    Telemetry.execute([:continuum, :version_registry, :failed], %{retry_in_ms: delay}, %{
      instance: state.instance.name,
      error: error
    })

    pending = Map.new(entries, &{entry_key(&1), &1})

    %{
      state
      | state: :degraded,
        pending: Map.merge(state.pending, pending),
        retry_attempt: state.retry_attempt + 1,
        retry_ref: retry_ref,
        last_error: error
    }
  end

  defp retry_delay(state) do
    multiplier = trunc(:math.pow(2, min(state.retry_attempt, 10)))
    min(state.retry_max_ms, state.retry_base_ms * multiplier)
  end

  defp cancel_retry(nil), do: :ok
  defp cancel_retry(ref), do: Process.cancel_timer(ref, async: true, info: false)

  defp status_map(state) do
    %{
      state: state.state,
      registered_count: MapSet.size(state.registered),
      pending_count: map_size(state.pending),
      retry_attempt: state.retry_attempt,
      last_error: state.last_error,
      last_success_at: state.last_success_at
    }
  end

  defp server_name(instance), do: Instance.child_name(instance, __MODULE__)
  defp entry_key(entry), do: {entry.workflow_string, entry.version_hash}

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
         entrypoint: entrypoint,
         retention: Map.get(metadata, :retention, :infinity)
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
