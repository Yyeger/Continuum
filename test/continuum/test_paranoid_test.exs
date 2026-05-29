defmodule Continuum.TestParanoidTest do
  @moduledoc """
  Exercises the `Continuum.Test --paranoid` re-replay harness (V0.3 PR 2).

  These tests run regardless of whether paranoid mode is enabled for the suite;
  they call the harness directly so the safety net itself is covered.
  """

  use ExUnit.Case, async: false

  alias Continuum.Test.Paranoid

  defmodule DeterministicFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      a = Continuum.side_effect(fn -> input.x * 2 end)
      b = Continuum.side_effect(fn -> a + input.y end)
      {:ok, b}
    end
  end

  defmodule SourceFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: Continuum.side_effect(fn -> :original end)
  end

  defmodule ChangedFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: Continuum.side_effect(fn -> :changed end)
  end

  setup do
    Continuum.Test.reset_in_memory!()
    Paranoid.reset()
    :ok
  end

  describe "enabled?/0" do
    test "honours the application env override" do
      Application.put_env(:continuum, :paranoid_replay, true)
      on_exit(fn -> Application.delete_env(:continuum, :paranoid_replay) end)
      assert Paranoid.enabled?()
    end
  end

  describe "verify_run!/4" do
    test "re-replays a completed run to the same result" do
      {:ok, run_id} = Continuum.Test.start_synchronous(DeterministicFlow, %{x: 3, y: 4})
      assert {:ok, %{state: :completed, result: {:ok, 10}}} = Continuum.await(run_id, 1_000)

      assert :ok = Paranoid.verify_run!(DeterministicFlow, %{x: 3, y: 4}, run_id)
    end

    test "flunks when the replayed workflow drifts from the recorded history" do
      {:ok, run_id} = Continuum.Test.start_synchronous(SourceFlow, %{})
      assert {:ok, %{state: :completed, result: :original}} = Continuum.await(run_id, 1_000)

      assert_raise ExUnit.AssertionError, fn ->
        Paranoid.verify_run!(ChangedFlow, %{}, run_id)
      end
    end
  end

  describe "assert_histories_match!/2" do
    test "two live runs of a deterministic flow have identical normalized histories" do
      {:ok, r1} = Continuum.Test.start_synchronous(DeterministicFlow, %{x: 3, y: 4})
      {:ok, _} = Continuum.await(r1, 1_000)
      {:ok, r2} = Continuum.Test.start_synchronous(DeterministicFlow, %{x: 3, y: 4})
      {:ok, _} = Continuum.await(r2, 1_000)

      assert :ok =
               Paranoid.assert_histories_match!(
                 Continuum.Test.history(r1),
                 Continuum.Test.history(r2)
               )
    end

    test "ignores DB-stamped fields when comparing" do
      {:ok, run_id} = Continuum.Test.start_synchronous(DeterministicFlow, %{x: 1, y: 1})
      {:ok, _} = Continuum.await(run_id, 1_000)

      original = Continuum.Test.history(run_id)
      reseq = Enum.map(original, &Map.put(&1, :seq, &1.seq + 1000))

      assert :ok = Paranoid.assert_histories_match!(original, reseq)
    end

    test "raises when the command_id sequence diverges" do
      {:ok, run_id} = Continuum.Test.start_synchronous(DeterministicFlow, %{x: 1, y: 1})
      {:ok, _} = Continuum.await(run_id, 1_000)

      original = Continuum.Test.history(run_id)
      tampered = List.update_at(original, 0, &Map.put(&1, :command_id, {:bogus}))

      assert_raise ExUnit.AssertionError, fn ->
        Paranoid.assert_histories_match!(original, tampered)
      end
    end
  end

  describe "auto-verify telemetry handler" do
    setup do
      Application.put_env(:continuum, :paranoid_replay, true)

      on_exit(fn ->
        Paranoid.reset()
        Application.delete_env(:continuum, :paranoid_replay)
      end)

      :ok
    end

    test "records no mismatch for a faithfully completed run" do
      {:ok, run_id} = Continuum.Test.start_synchronous(DeterministicFlow, %{x: 2, y: 5})
      {:ok, _} = Continuum.await(run_id, 1_000)

      # Drive the handler deterministically rather than racing the engine's
      # own completion broadcast.
      Paranoid.handle_event(
        [:continuum, :run, :completed],
        %{},
        %{run_id: run_id, workflow: DeterministicFlow, instance: Continuum},
        nil
      )

      assert Paranoid.mismatches() == []
    end

    test "records a mismatch when replay drifts" do
      {:ok, run_id} = Continuum.Test.start_synchronous(SourceFlow, %{})
      {:ok, _} = Continuum.await(run_id, 1_000)

      Paranoid.handle_event(
        [:continuum, :run, :completed],
        %{},
        %{run_id: run_id, workflow: ChangedFlow, instance: Continuum},
        nil
      )

      assert [%{run_id: ^run_id} | _] = Paranoid.mismatches()
    end
  end
end
