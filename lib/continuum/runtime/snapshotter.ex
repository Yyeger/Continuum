defmodule Continuum.Runtime.Snapshotter do
  @moduledoc false

  use GenServer
  require Logger

  alias Continuum.{Runtime.Instance, Snapshot, Telemetry}

  @default_max_size_bytes 1_000_000

  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    GenServer.start_link(__MODULE__, opts, name: instance.snapshotter)
  end

  @doc false
  def maybe_snapshot(instance_or_name, run_id, lease_token \\ nil, journal \\ nil) do
    instance = Instance.lookup(instance_or_name)

    case Process.whereis(instance.snapshotter) do
      nil ->
        :ok

      pid ->
        GenServer.cast(pid, {:maybe_snapshot, run_id, lease_token, journal})
    end
  end

  @doc false
  def snapshot_once(instance_or_name, run_id, opts \\ []) do
    instance = Instance.lookup(instance_or_name)
    config = config(opts, instance)
    take_snapshot(instance, run_id, Keyword.get(opts, :lease_token), config)
  end

  @impl true
  def init(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    {:ok, %{instance: instance, config: config(opts, instance)}}
  end

  @impl true
  def handle_cast({:maybe_snapshot, run_id, lease_token, journal}, state) do
    # The journal adapter that wrote the run's events identifies itself; the
    # snapshot must land in the same journal regardless of the instance-level
    # default (an InMemory-default instance can still own durable runs started
    # with `journal: Postgres`).
    config = if journal, do: %{state.config | journal: journal}, else: state.config

    # The instance-level threshold alone cannot gate the cast: per-workflow
    # `snapshot_threshold:` only resolves inside take_snapshot, after the run
    # row is loaded. The registry hint keeps the default configuration
    # (:infinity, no per-workflow opt-ins) free of a per-event run lookup.
    if config.threshold != :infinity or Continuum.VersionRegistry.any_snapshot_threshold?() do
      take_snapshot(state.instance, run_id, lease_token, config)
    end

    {:noreply, state}
  end

  defp take_snapshot(instance, run_id, lease_token, config) do
    run = config.journal.get_run(instance, run_id)
    threshold = threshold_for_run(run, config)

    with %{version_hash: version_hash} <- run,
         true <- threshold != :infinity,
         {base_snapshot, events} <-
           load_snapshot_window(config.journal, instance, run_id, lease_token),
         {base_snapshot, events} <-
           discard_incompatible_base(
             config.journal,
             instance,
             run_id,
             version_hash,
             base_snapshot,
             events
           ),
         true <- length(events) >= threshold,
         {:ok, snapshot} <- Snapshot.compact(run_id, version_hash, events, base: base_snapshot),
         :ok <- persist_snapshot(config.journal, instance, snapshot, config) do
      Telemetry.execute(
        [:continuum, :snapshot, :taken],
        %{
          event_count: map_size(snapshot.steps_by_seq),
          size_bytes: Snapshot.encoded_size(snapshot)
        },
        %{
          instance: instance.name,
          run_id: run_id,
          through_seq: snapshot.through_seq,
          format_version: Snapshot.format_version(),
          compacted_prefix_length: map_size(snapshot.steps_by_seq)
        }
      )
    else
      nil ->
        :ok

      false ->
        :ok

      {:skip, _reason} ->
        :ok

      {:error, reason} ->
        Logger.warning("Continuum snapshot skipped for #{run_id}: #{inspect(reason)}")

        Telemetry.execute([:continuum, :snapshot, :skipped], %{}, %{
          instance: instance.name,
          run_id: run_id,
          reason: reason
        })

        :ok
    end
  rescue
    error ->
      Logger.warning("Continuum snapshot skipped for #{run_id}: #{Exception.message(error)}")

      Telemetry.execute([:continuum, :snapshot, :skipped], %{}, %{
        instance: instance.name,
        run_id: run_id,
        reason: error
      })

      :ok
  catch
    # Snapshots are best-effort: a pool/connection exit must not take the
    # Snapshotter down with it (events are never pruned, so a skipped
    # snapshot only defers compaction).
    :exit, reason ->
      Logger.warning("Continuum snapshot skipped for #{run_id}: #{inspect(reason)}")

      Telemetry.execute([:continuum, :snapshot, :skipped], %{}, %{
        instance: instance.name,
        run_id: run_id,
        reason: reason
      })

      :ok
  end

  defp load_snapshot_window(journal, instance, run_id, lease_token) do
    journal.load_with_snapshot(instance, run_id, lease_token)
  end

  defp discard_incompatible_base(journal, instance, run_id, _version_hash, nil, events) do
    {nil, events_for_full_history(journal, instance, run_id, events)}
  end

  defp discard_incompatible_base(_journal, _instance, _run_id, version_hash, base, events)
       when base.version_hash == version_hash do
    {base, events}
  end

  defp discard_incompatible_base(journal, instance, run_id, _version_hash, _base, _events) do
    # The preceding load_with_snapshot call already validated the run lease.
    # Re-reading full history after a version mismatch is intentionally only a
    # best-effort snapshot attempt; a stale snapshotter can at worst waste work
    # or write a snapshot that replay later discards by version_hash.
    {nil, journal.load(instance, run_id)}
  end

  defp events_for_full_history(_journal, _instance, _run_id, events), do: events

  defp persist_snapshot(journal, instance, snapshot, config) do
    size = Snapshot.encoded_size(snapshot)

    if size <= config.max_size_bytes do
      journal.take_snapshot!(instance, snapshot)
    else
      {:error, {:snapshot_too_large, size, config.max_size_bytes}}
    end
  end

  defp config(opts, instance) do
    %{
      threshold:
        opts
        |> Keyword.get(
          :snapshot_threshold,
          Application.get_env(:continuum, :snapshot_threshold, :infinity)
        )
        |> normalize_threshold!(),
      max_size_bytes:
        Keyword.get(
          opts,
          :snapshot_max_size_bytes,
          Application.get_env(:continuum, :snapshot_max_size_bytes, @default_max_size_bytes)
        ),
      journal: Keyword.get(opts, :journal, default_journal(instance))
    }
  end

  @doc false
  def normalize_threshold!(:infinity), do: :infinity

  def normalize_threshold!(threshold) when is_integer(threshold) and threshold > 0 do
    threshold
  end

  def normalize_threshold!(other) do
    raise ArgumentError,
          "expected :snapshot_threshold to be :infinity or a positive integer, got: #{inspect(other)}"
  end

  defp threshold_for_run(nil, _config), do: :infinity

  defp threshold_for_run(run, config) do
    workflow_snapshot_threshold(run) || config.threshold
  end

  defp workflow_snapshot_threshold(%{workflow: workflow, version_hash: version_hash}) do
    with {:ok, %{entrypoint: entrypoint}} <-
           Continuum.VersionRegistry.resolve(workflow, version_hash),
         true <- function_exported?(entrypoint, :__continuum_workflow__, 0),
         threshold <- Map.get(entrypoint.__continuum_workflow__(), :snapshot_threshold),
         true <- not is_nil(threshold) do
      threshold
    else
      _ -> nil
    end
  end

  # One source of truth for journal resolution (post-2.2 audit fix): named
  # instances pin their journal at construction, the default instance follows
  # config — deriving it from repo presence here could disagree with the rest
  # of the runtime.
  defp default_journal(instance), do: Continuum.Runtime.Instance.journal(instance)
end
