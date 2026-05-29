defmodule Continuum.ReplayPropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  defmodule SideEffectChainFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      result =
        Enum.reduce(input.steps, 0, fn step, acc ->
          Continuum.side_effect(fn -> acc + step end)
        end)

      {:ok, result}
    end
  end

  defmodule PropertyActivity do
    def run(acc, step), do: acc + step
  end

  defmodule PatchedMixFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      base = Continuum.side_effect(fn -> input.seed end)
      bonus = if Continuum.patched?(:bonus), do: 100, else: 0
      {:ok, base + bonus}
    end
  end

  defmodule MixedOperationFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      result =
        input.ops
        |> Enum.with_index(1)
        |> Enum.reduce(0, fn
          {{:side_effect, step}, _index}, acc ->
            Continuum.side_effect(fn -> acc + step end)

          {{:activity, step}, index}, acc ->
            activity(PropertyActivity.run(acc, step + index))
        end)

      {:ok, result}
    end
  end

  property "side-effect histories replay to the same result" do
    check all(steps <- list_of(integer(-100..100), max_length: 20), max_runs: 50) do
      Continuum.Test.reset_in_memory!()

      {:ok, run_id} = Continuum.Test.start_synchronous(SideEffectChainFlow, %{steps: steps})
      expected = {:ok, Enum.sum(steps)}

      assert {:ok, %{state: :completed, result: ^expected}} = Continuum.await(run_id, 1_000)

      history = Continuum.Test.history(run_id)
      assert length(history) == length(steps)

      assert Continuum.Test.assert_replays(SideEffectChainFlow, %{steps: steps}, history) ==
               expected

      assert_snapshot_replays(SideEffectChainFlow, %{steps: steps}, history, expected)
    end
  end

  property "mixed activity and side-effect histories replay to the same result" do
    op =
      bind(member_of([:activity, :side_effect]), fn kind ->
        map(integer(-100..100), &{kind, &1})
      end)

    check all(ops <- list_of(op, max_length: 20), max_runs: 50) do
      Continuum.Test.reset_in_memory!()

      {:ok, run_id} = Continuum.Test.start_synchronous(MixedOperationFlow, %{ops: ops})
      expected = {:ok, expected_mixed_result(ops)}

      assert {:ok, %{state: :completed, result: ^expected}} = Continuum.await(run_id, 1_000)

      history = Continuum.Test.history(run_id)
      assert length(history) == length(ops)

      assert Enum.map(history, & &1.type) ==
               Enum.map(ops, fn
                 {:activity, _step} -> :activity_completed
                 {:side_effect, _step} -> :side_effect
               end)

      assert Continuum.Test.assert_replays(MixedOperationFlow, %{ops: ops}, history) == expected
      assert_snapshot_replays(MixedOperationFlow, %{ops: ops}, history, expected)
    end
  end

  property "patched + side_effect histories replay identically and survive snapshotting" do
    check all(seed <- integer(-1_000..1_000), max_runs: 25) do
      Continuum.Test.reset_in_memory!()

      {:ok, run_id} = Continuum.Test.start_synchronous(PatchedMixFlow, %{seed: seed})
      expected = {:ok, seed + 100}

      assert {:ok, %{state: :completed, result: ^expected}} = Continuum.await(run_id, 1_000)

      history = Continuum.Test.history(run_id)
      assert Enum.any?(history, &(&1.type == :patched))

      assert Continuum.Test.assert_replays(PatchedMixFlow, %{seed: seed}, history) == expected
      assert_snapshot_replays(PatchedMixFlow, %{seed: seed}, history, expected)
    end
  end

  defp assert_snapshot_replays(_workflow, _input, [], _expected), do: :ok

  defp assert_snapshot_replays(workflow, input, history, expected) do
    version_hash = workflow.__continuum_workflow__().version_hash

    thresholds =
      [1, max(1, div(length(history), 2)), length(history)]
      |> Enum.uniq()

    for threshold <- thresholds do
      prefix = Enum.take(history, threshold)
      {:ok, snapshot} = Continuum.Snapshot.compact("property-snapshot", version_hash, prefix)
      remaining = Enum.drop(history, snapshot.through_seq + 1)

      assert {:ok, ^expected} =
               Continuum.Test.replay(workflow, input, remaining, snapshot: snapshot)
    end
  end

  defp expected_mixed_result(ops) do
    ops
    |> Enum.with_index(1)
    |> Enum.reduce(0, fn
      {{:side_effect, step}, _index}, acc -> acc + step
      {{:activity, step}, index}, acc -> acc + step + index
    end)
  end
end
