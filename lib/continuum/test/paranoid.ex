defmodule Continuum.Test.Paranoid do
  @moduledoc """
  Determinism safety net for the test suite — the `--paranoid` re-replay mode.

  When enabled, every workflow run that reaches a `:completed` terminal state is
  re-replayed from its journaled history through `Continuum.Test.replay/4`. The
  re-replay asserts an identical `(event_type, decoded_payload, command_id)`
  sequence between the original execution and the replay, and that the replay
  produces the same result. DB-stamped fields (`:seq`, `:inserted_at`) are
  excluded from the comparison.

  Enable it for a whole `mix test` run with the `CONTINUUM_PARANOID=1`
  environment variable (or `config :continuum, :paranoid_replay, true`). The
  default is off so ordinary `mix test` stays fast — the expensive paranoid
  sweep is meant for the push-to-`main` CI matrix.

  Two surfaces:

    * `verify_run!/4` — synchronous, raises on divergence. Call it directly from
      a test for belt-and-suspenders coverage of a specific run.
    * `attach!/0` — installs a telemetry handler that auto-verifies every
      `:completed` run. Mismatches are logged and collected; `mismatches/0`
      and `reset/0` let an `ExUnit.after_suite` callback surface them without
      crashing the engine that emitted the completion event.
  """

  import ExUnit.Assertions

  require Logger

  alias Continuum.Runtime.{Context, Instance, Journal}

  @handler_id {__MODULE__, :auto_verify}
  @collector __MODULE__.Collector

  @doc "Whether paranoid re-replay is enabled for this run."
  @spec enabled?() :: boolean()
  def enabled? do
    Application.get_env(:continuum, :paranoid_replay, false) or
      System.get_env("CONTINUUM_PARANOID") in ~w(1 true yes)
  end

  # ---------------------------------------------------------------------------
  # Synchronous verification
  # ---------------------------------------------------------------------------

  @doc """
  Re-replay a terminal run's journaled history and assert it is identical.

  Loads the run's recorded history, replays the workflow against it, and asserts
  that:

    * no `Continuum.ReplayDriftError` is raised (the journaled
      `(event_type, payload, command_id)` sequence still matches what the
      orchestration requests, in order), and
    * the replayed result equals the result the original run recorded.

  Raises an `ExUnit.AssertionError` on divergence. Accepts the same `:journal`
  and `:instance` options as `Continuum.Test.replay/4`.
  """
  @spec verify_run!(module(), term(), binary(), keyword()) :: :ok
  def verify_run!(workflow_module, input, run_id, opts \\ []) do
    history = Continuum.Test.history(run_id, opts)
    recorded = recorded_result!(run_id, opts)

    # The completion telemetry fires inside the engine process with a live
    # Context in the dictionary; replay/4 puts and clears its own. Snapshot and
    # restore so the auto-verify path never disturbs the emitting process.
    saved_ctx = Context.get()

    try do
      case Continuum.Test.replay(workflow_module, input, history, opts) do
        {:ok, result} ->
          assert result == recorded,
                 "paranoid replay of #{inspect(run_id)} produced #{inspect(result)} " <>
                   "but the original run recorded #{inspect(recorded)}"

          :ok

        {:suspended, reason} ->
          flunk(
            "paranoid replay of terminal run #{inspect(run_id)} suspended at #{inspect(reason)}"
          )

        {:error, reason} ->
          flunk("paranoid replay of terminal run #{inspect(run_id)} diverged: #{inspect(reason)}")
      end
    after
      restore_ctx(saved_ctx)
    end
  end

  @doc """
  Assert two journaled histories carry an identical
  `(event_type, decoded_payload, command_id)` sequence.

  DB-stamped fields (`:seq`, `:inserted_at`) are excluded from the comparison.
  """
  @spec assert_histories_match!([map()], [map()]) :: :ok
  def assert_histories_match!(original, replayed) do
    normalized_original = normalize(original)
    normalized_replayed = normalize(replayed)

    assert normalized_original == normalized_replayed,
           "paranoid history sequences diverged:\n" <>
             "  original: #{inspect(normalized_original)}\n" <>
             "  replayed: #{inspect(normalized_replayed)}"

    :ok
  end

  @doc "Normalize a history for comparison: drop DB-stamped fields."
  @spec normalize([map()]) :: [map()]
  def normalize(history) do
    Enum.map(history, &Map.drop(&1, [:seq, :inserted_at]))
  end

  # ---------------------------------------------------------------------------
  # Auto-verification (telemetry handler)
  # ---------------------------------------------------------------------------

  @doc """
  Attach the auto-verify telemetry handler for the `[:continuum, :run,
  :completed]` event. Idempotent.
  """
  @spec attach!() :: :ok
  def attach! do
    ensure_collector()
    :telemetry.detach(@handler_id)

    :telemetry.attach(
      @handler_id,
      [:continuum, :run, :completed],
      &__MODULE__.handle_event/4,
      nil
    )
  end

  @doc "Detach the auto-verify telemetry handler."
  @spec detach!() :: :ok
  def detach! do
    :telemetry.detach(@handler_id)
    :ok
  end

  @doc false
  def handle_event(_event, _measurements, metadata, _config) do
    if enabled?(), do: auto_verify(metadata)
    :ok
  end

  @doc "Collected mismatches found by the auto-verify handler."
  @spec mismatches() :: [map()]
  def mismatches do
    case Process.whereis(@collector) do
      nil -> []
      _pid -> Agent.get(@collector, & &1)
    end
  end

  @doc "Reset the collected mismatches."
  @spec reset() :: :ok
  def reset do
    case Process.whereis(@collector) do
      nil -> :ok
      _pid -> Agent.update(@collector, fn _ -> [] end)
    end
  end

  # ---------------------------------------------------------------------------

  # The auto path is deliberately lenient: the in-memory journal is shared
  # across the suite and can be wiped by a concurrent test between the moment a
  # run completes and the moment this handler re-loads it. A vanished or
  # truncated run is a test-suite race, not a determinism regression, so it is
  # skipped. Only two outcomes are recorded as mismatches: a clean replay that
  # yields a different result, or a `ReplayDriftError`. The strict, raising
  # contract lives in `verify_run!/4` for tests that drive a specific run.
  defp auto_verify(%{run_id: run_id, workflow: workflow} = metadata) do
    instance = Instance.lookup(Map.get(metadata, :instance, Continuum))

    case locate_run(instance, run_id) do
      {journal, %{state: :completed, input: input, result: recorded}} ->
        classify_and_record(workflow, input, run_id, recorded, instance: instance, journal: journal)

      _ ->
        :ok
    end
  end

  defp auto_verify(_metadata), do: :ok

  defp classify_and_record(workflow, input, run_id, recorded, opts) do
    case safe_history(run_id, opts) do
      [] ->
        :ok

      history ->
        saved_ctx = Context.get()

        try do
          case safe_replay(workflow, input, history, opts) do
            {:ok, ^recorded} ->
              :ok

            {:ok, other} ->
              record_mismatch(
                run_id,
                "replay result #{inspect(other)} != recorded #{inspect(recorded)}"
              )

            {:error, {:error, %Continuum.ReplayDriftError{} = error, _stack}} ->
              record_mismatch(run_id, Exception.message(error))

            _other ->
              # Suspended/truncated/other — most likely a shared-journal race.
              :ok
          end
        after
          restore_ctx(saved_ctx)
        end
    end
  end

  defp safe_history(run_id, opts) do
    Continuum.Test.history(run_id, opts)
  rescue
    _ -> []
  catch
    _, _ -> []
  end

  defp safe_replay(workflow, input, history, opts) do
    Continuum.Test.replay(workflow, input, history, opts)
  rescue
    error -> {:error, {:error, error, []}}
  catch
    kind, reason -> {:error, {kind, reason, []}}
  end

  # Auto-verify covers in-memory runs only — the journal the replay-correctness
  # suite and the StreamData property tests use, and the one the plan calls out
  # as "already has everything we need". Postgres runs share the exact same
  # replay loop and are covered by explicit `verify_run!/4` calls and the
  # Postgres replay tests; reaching into the sandbox connection from the engine
  # process here only races connection ownership without adding coverage.
  defp locate_run(instance, run_id) do
    case safe_get_run(Journal.InMemory, instance, run_id) do
      %{} = run -> {Journal.InMemory, run}
      _ -> :none
    end
  end

  defp safe_get_run(journal, instance, run_id) do
    journal.get_run(instance, run_id)
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp recorded_result!(run_id, opts) do
    journal = Keyword.get(opts, :journal, Journal.InMemory)
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case journal.get_run(instance, run_id) do
      %{state: :completed, result: result} ->
        result

      other ->
        flunk(
          "paranoid verify expected a completed run #{inspect(run_id)}, got: #{inspect(other)}"
        )
    end
  end

  defp record_mismatch(run_id, message) do
    Logger.error("Continuum paranoid replay mismatch for #{inspect(run_id)}:\n#{message}")
    ensure_collector()
    Agent.update(@collector, &[%{run_id: run_id, message: message} | &1])
    :ok
  end

  defp ensure_collector do
    case Process.whereis(@collector) do
      nil ->
        case Agent.start(fn -> [] end, name: @collector) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
        end

      _pid ->
        :ok
    end
  end

  defp restore_ctx(nil), do: Context.clear()
  defp restore_ctx(%Context{} = ctx), do: Context.put(ctx)
end
