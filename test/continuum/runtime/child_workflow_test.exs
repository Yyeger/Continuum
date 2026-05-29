defmodule Continuum.Runtime.ChildWorkflowTest do
  @moduledoc """
  Parent/child workflows (V0.3 PR 5, §3.2): sequential await, fan-out, failure
  propagation, cancellation cascade, and replay safety.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{ActivityWorker, Dispatcher, Instance}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}

  defmodule LeafFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, {:leaf, input.id}}
  end

  defmodule FailingLeafFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: raise("leaf boom")
  end

  defmodule SlowLeafFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: {:ok, await(signal(:never))}
  end

  defmodule SequentialParentFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, leaf} = await(child(LeafFlow.run(%{id: input.id})))
      {:ok, {:parent_saw, leaf}}
    end
  end

  defmodule FanOutParentFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      results =
        input.ids
        |> Enum.map(fn id -> start_child(LeafFlow, %{id: id}, id: "leaf-#{id}") end)
        |> Enum.map(fn ref -> await_child(ref) end)

      {:ok, results}
    end
  end

  defmodule FailingParentFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      result = await(child(FailingLeafFlow.run(%{})))
      {:ok, {:child_result, result}}
    end
  end

  defmodule CancelParentFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      {:ok, _} = await(child(SlowLeafFlow.run(%{})))
      {:ok, :should_not_reach}
    end
  end

  setup do
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "sequential await child suspends the parent, runs the child, resumes with the result" do
    {:ok, parent_id} = Continuum.start(SequentialParentFlow, %{id: "a"}, journal: Postgres)

    pump(parent_id)

    assert {:ok, %{state: :completed, result: {:ok, {:parent_saw, {:leaf, "a"}}}}} =
             Continuum.await(parent_id, 2_000, journal: Postgres)

    [child] = children_of(parent_id)
    assert child.state == "completed"
  end

  test "fan-out start_child + await_child returns each child's result" do
    {:ok, parent_id} =
      Continuum.start(FanOutParentFlow, %{ids: [1, 2, 3, 4, 5]}, journal: Postgres)

    pump(parent_id)

    assert {:ok, %{state: :completed, result: {:ok, results}}} =
             Continuum.await(parent_id, 3_000, journal: Postgres)

    assert results == Enum.map(1..5, &{:ok, {:leaf, &1}})
    assert length(children_of(parent_id)) == 5
    assert Enum.all?(children_of(parent_id), &(&1.state == "completed"))
  end

  test "child failure propagates to the parent's await_child as an error term" do
    {:ok, parent_id} = Continuum.start(FailingParentFlow, %{}, journal: Postgres)

    pump(parent_id)

    assert {:ok, %{state: :completed, result: {:ok, {:child_result, {:error, _error}}}}} =
             Continuum.await(parent_id, 2_000, journal: Postgres)

    events = Postgres.load(Instance.default(), parent_id)
    assert Enum.any?(events, &(&1.type == :child_failed))
  end

  test "cancelling a parent cascades cancellation to in-flight children" do
    {:ok, parent_id} = Continuum.start(CancelParentFlow, %{}, journal: Postgres)

    # Drive until the child run exists and is itself suspended (awaiting a
    # signal that never arrives).
    assert_eventually(fn -> children_of(parent_id) != [] end)
    Dispatcher.dispatch_once(owner: "child-cancel", batch_size: 10)

    assert_eventually(fn ->
      match?([%{state: "suspended"}], children_of(parent_id))
    end)

    :ok = Continuum.cancel(parent_id, journal: Postgres)

    assert_eventually(fn ->
      case children_of(parent_id) do
        [%{state: "failed", error: error}] -> decode(error) == :parent_cancelled
        _ -> false
      end
    end)
  end

  test "parent history replays to an identical result with deterministic child ids" do
    {:ok, parent_id} = Continuum.start(SequentialParentFlow, %{id: "replay"}, journal: Postgres)
    pump(parent_id)
    assert {:ok, %{result: result}} = Continuum.await(parent_id, 2_000, journal: Postgres)

    history = Postgres.load(Instance.default(), parent_id)
    assert Enum.any?(history, &(&1.type == :child_started))
    assert Enum.any?(history, &(&1.type == :child_completed))

    assert {:ok, ^result} = Continuum.Test.replay(SequentialParentFlow, %{id: "replay"}, history)
  end

  defp children_of(parent_id) do
    Repo.all(
      from(r in Run,
        where: r.parent_run_id == ^parent_id,
        order_by: [asc: r.started_at],
        select: %{id: r.id, state: r.state, error: r.error}
      )
    )
  end

  defp decode(nil), do: nil
  defp decode(binary) when is_binary(binary), do: :erlang.binary_to_term(binary)

  defp pump(parent_id, attempts \\ 300)

  defp pump(parent_id, attempts) when attempts > 0 do
    if terminal?(parent_id) do
      :ok
    else
      Dispatcher.dispatch_once(owner: "child-pump", batch_size: 10)
      ActivityWorker.Dispatcher.dispatch_once(owner: "child-pump-act", batch_size: 10)
      Process.sleep(5)
      pump(parent_id, attempts - 1)
    end
  end

  defp pump(_parent_id, 0), do: flunk("pump did not reach a terminal parent run")

  defp terminal?(parent_id) do
    Repo.one(from(r in Run, where: r.id == ^parent_id, select: r.state)) in [
      "completed",
      "failed",
      "cancelled"
    ]
  end

  defp assert_eventually(fun, attempts \\ 100)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
