defmodule Continuum.SideEffectCompositionTest do
  @moduledoc """
  Regression coverage for `side_effect/1` composed with v0.3 effect families.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{ActivityWorker, Dispatcher, Instance}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}

  defmodule SagaActivity do
    def run(value), do: {:ok, value * 2}
  end

  defmodule SagaCompensation do
    def undo(value), do: {:ok, {:undone, value}}
  end

  defmodule SagaPatchFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      base = Continuum.side_effect(fn -> input.seed end)
      bonus = if Continuum.patched?(:bonus), do: 10, else: 0

      {:ok, ref} =
        activity(SagaActivity.run(base + bonus),
          compensate: {SagaCompensation, :undo, [base + bonus]}
        )

      compensated = compensate(ref)
      final = Continuum.side_effect(fn -> ref.result + 1 end)

      {:ok, {final, compensated}}
    end
  end

  defmodule ContinuedChildFlow do
    use Continuum.Workflow, version: 1

    def run(%{n: n, max: max}) do
      current = Continuum.side_effect(fn -> n end)

      if current < max do
        continue_as_new(%{n: current + 1, max: max})
      else
        {:ok, {:child_done, current}}
      end
    end
  end

  defmodule ParentFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      before_child = Continuum.side_effect(fn -> {:before, input.max} end)
      child_result = await(child(ContinuedChildFlow.run(%{n: 1, max: input.max})))
      after_child = Continuum.side_effect(fn -> {:after, elem(before_child, 1)} end)

      {:ok, {before_child, child_result, after_child}}
    end
  end

  setup do
    Continuum.Test.reset_in_memory!()
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "side_effect composes with patched, compensation, and snapshots" do
    {:ok, run_id} = Continuum.Test.start_synchronous(SagaPatchFlow, %{seed: 7})

    expected = {:ok, {35, {:ok, {:ok, {:undone, 17}}}}}

    assert {:ok, %{state: :completed, result: ^expected}} =
             Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)

    assert [:side_effect, :patched, :activity_completed, :compensation_completed, :side_effect] =
             Enum.map(history, & &1.type)

    assert Continuum.Test.assert_replays(SagaPatchFlow, %{seed: 7}, history) == expected

    assert_snapshot_replays(SagaPatchFlow, %{seed: 7}, history, expected)
  end

  test "side_effect composes with child awaits and continued child chains" do
    {:ok, parent_id} = Continuum.start(ParentFlow, %{max: 2}, journal: Postgres)

    pump_until(fn -> run_state(parent_id) in ["completed", "failed"] end)

    expected = {:ok, {{:before, 2}, {:ok, {:child_done, 2}}, {:after, 2}}}

    assert {:ok, %{state: :completed, result: ^expected}} =
             Continuum.await(parent_id, 2_000, journal: Postgres)

    parent_history = Postgres.load(Instance.default(), parent_id)

    assert Enum.map(parent_history, & &1.type) == [
             :side_effect,
             :child_started,
             :child_completed,
             :side_effect
           ]

    assert {:ok, ^expected} = Continuum.Test.replay(ParentFlow, %{max: 2}, parent_history)
    assert_snapshot_replays(ParentFlow, %{max: 2}, parent_history, expected)

    assert Repo.aggregate(from(r in Run, where: r.parent_run_id == ^parent_id), :count) == 2
  end

  defp assert_snapshot_replays(workflow, input, history, expected) do
    {:ok, snapshot} =
      Continuum.Snapshot.compact(
        "side-effect-composition",
        workflow.__continuum_workflow__().version_hash,
        history
      )

    assert {:ok, ^expected} = Continuum.Test.replay(workflow, input, [], snapshot: snapshot)
  end

  defp pump_until(fun, attempts \\ 300)

  defp pump_until(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Dispatcher.dispatch_once(owner: "side-effect-composition", batch_size: 10)

      ActivityWorker.Dispatcher.dispatch_once(
        owner: "side-effect-composition-act",
        batch_size: 10
      )

      Process.sleep(5)
      pump_until(fun, attempts - 1)
    end
  end

  defp pump_until(_fun, 0), do: flunk("condition did not become true")

  defp run_state(run_id), do: Repo.one(from(r in Run, where: r.id == ^run_id, select: r.state))
end
