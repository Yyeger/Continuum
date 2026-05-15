defmodule Continuum.Runtime.Snapshotter do
  @moduledoc false

  use GenServer
  require Logger

  alias Continuum.{Runtime.Instance, Runtime.Journal, Snapshot, Telemetry}

  @default_max_size_bytes 1_000_000

  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    GenServer.start_link(__MODULE__, opts, name: instance.snapshotter)
  end

  @doc false
  def maybe_snapshot(instance_or_name, run_id, lease_token \\ nil) do
    instance = Instance.lookup(instance_or_name)

    case Process.whereis(instance.snapshotter) do
      nil ->
        :ok

      pid ->
        GenServer.cast(pid, {:maybe_snapshot, run_id, lease_token})
    end
  end

  @doc false
  def snapshot_once(instance_or_name, run_id, opts \\ []) do
    instance = Instance.lookup(instance_or_name)
    config = config(opts, instance)

    if config.threshold == :infinity do
      :ok
    else
      take_snapshot(instance, run_id, Keyword.get(opts, :lease_token), config)
    end
  end

  @impl true
  def init(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    {:ok, %{instance: instance, config: config(opts, instance)}}
  end

  @impl true
  def handle_cast({:maybe_snapshot, run_id, lease_token}, state) do
    if state.config.threshold != :infinity do
      take_snapshot(state.instance, run_id, lease_token, state.config)
    end

    {:noreply, state}
  end

  defp take_snapshot(instance, run_id, lease_token, config) do
    run = config.journal.get_run(instance, run_id)

    with %{version_hash: version_hash} <- run,
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
         true <- length(events) >= config.threshold,
         {:ok, snapshot} <- Snapshot.compact(run_id, version_hash, events, base: base_snapshot),
         :ok <- persist_snapshot(config.journal, instance, snapshot, config) do
      Telemetry.execute(
        [:continuum, :snapshot, :taken],
        %{
          event_count: map_size(snapshot.steps_by_seq),
          size_bytes: Snapshot.encoded_size(snapshot)
        },
        %{instance: instance.name, run_id: run_id, through_seq: snapshot.through_seq}
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

  defp normalize_threshold!(:infinity), do: :infinity

  defp normalize_threshold!(threshold) when is_integer(threshold) and threshold > 0 do
    threshold
  end

  defp normalize_threshold!(other) do
    raise ArgumentError,
          "expected :snapshot_threshold to be :infinity or a positive integer, got: #{inspect(other)}"
  end

  defp default_journal(%{repo: nil}), do: Journal.InMemory
  defp default_journal(_instance), do: Journal.Postgres
end
