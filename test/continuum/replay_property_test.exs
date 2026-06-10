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

  defmodule SagaPropertyActivity do
    def run(step), do: {:ok, step}
  end

  defmodule SagaPropertyComp do
    def undo(step), do: {:undone, step}
  end

  defmodule SagaPropertyFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      Enum.each(input.steps, fn step ->
        {:ok, _ref} =
          activity(SagaPropertyActivity.run(step), compensate: {SagaPropertyComp, :undo, [step]})
      end)

      compensate_all()
      {:ok, :done}
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

  property "long side-effect histories replay to the same result" do
    check all(steps <- constant(Enum.to_list(1..1_000)), max_runs: 1) do
      Continuum.Test.reset_in_memory!()

      {:ok, run_id} = Continuum.Test.start_synchronous(SideEffectChainFlow, %{steps: steps})
      expected = {:ok, Enum.sum(steps)}

      assert {:ok, %{state: :completed, result: ^expected}} = Continuum.await(run_id, 5_000)

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

  property "saga histories (compensated activities + compensate_all) replay identically and snapshot" do
    check all(steps <- list_of(integer(-50..50), min_length: 1, max_length: 8), max_runs: 20) do
      Continuum.Test.reset_in_memory!()

      {:ok, run_id} = Continuum.Test.start_synchronous(SagaPropertyFlow, %{steps: steps})
      assert {:ok, %{state: :completed, result: {:ok, :done}}} = Continuum.await(run_id, 1_000)

      history = Continuum.Test.history(run_id)
      assert Enum.count(history, &(&1.type == :compensation_completed)) == length(steps)

      assert Continuum.Test.assert_replays(SagaPropertyFlow, %{steps: steps}, history) ==
               {:ok, :done}

      assert_snapshot_replays(SagaPropertyFlow, %{steps: steps}, history, {:ok, :done})
    end
  end

  defmodule ChildLeafPropertyFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: {:ok, :leaf}
  end

  defmodule OtherLeafPropertyFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: {:ok, :other}
  end

  defmodule ChildPropertyFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      ref = start_child(ChildLeafPropertyFlow, input.child_input)
      await_child(ref)
    end
  end

  defmodule OtherChildPropertyFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      ref = start_child(OtherLeafPropertyFlow, input.child_input)
      await_child(ref)
    end
  end

  defmodule ContinuePropertyFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      continue_as_new(%{n: input.n + 1})
    end
  end

  property "child_started replay validates the journaled workflow and input hash" do
    child_input =
      one_of([
        integer(),
        binary(max_length: 16),
        map_of(atom(:alphanumeric), integer(), max_length: 3)
      ])

    check all(input <- child_input, max_runs: 30) do
      history = [
        %{
          type: :child_started,
          child_run_id: "child-prop-1",
          workflow: ChildLeafPropertyFlow,
          input_hash: hash_term(input),
          command_id: nil,
          seq: 0
        },
        %{
          type: :child_completed,
          child_run_id: "child-prop-1",
          result: {:ok, :leaf},
          command_id: nil,
          seq: 1
        }
      ]

      assert {:ok, {:ok, :leaf}} =
               Continuum.Test.replay(ChildPropertyFlow, %{child_input: input}, history)

      # Same call site, different child workflow module: drift.
      assert {:error, {:error, %Continuum.ReplayDriftError{}, _}} =
               Continuum.Test.replay(OtherChildPropertyFlow, %{child_input: input}, history)

      # Same workflow, different commanded input: drift.
      assert {:error, {:error, %Continuum.ReplayDriftError{}, _}} =
               Continuum.Test.replay(
                 ChildPropertyFlow,
                 %{child_input: {:mutated, input}},
                 history
               )
    end
  end

  property "continue_as_new replay validates the journaled next input hash" do
    check all(n <- integer(), max_runs: 30) do
      history = [
        %{
          type: :run_continued_as_new,
          next_run_id: "next-prop-1",
          next_input_hash: hash_term(%{n: n + 1}),
          command_id: nil,
          seq: 0
        }
      ]

      assert {:continued, "next-prop-1"} =
               Continuum.Test.replay(ContinuePropertyFlow, %{n: n}, history)

      assert {:error, {:error, %Continuum.ReplayDriftError{}, _}} =
               Continuum.Test.replay(ContinuePropertyFlow, %{n: n + 1}, history)
    end
  end

  defp hash_term(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
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
