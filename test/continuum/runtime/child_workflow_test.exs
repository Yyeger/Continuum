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

  defmodule DeepFlow do
    use Continuum.Workflow, version: 1

    def run(%{depth: d}) do
      if d > 0 do
        await(child(DeepFlow.run(%{depth: d - 1})))
      else
        {:ok, :bottom}
      end
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

  test "start_child fails loudly when it would exceed max_child_depth" do
    previous = Application.get_env(:continuum, :max_child_depth)
    Application.put_env(:continuum, :max_child_depth, 2)
    on_exit(fn -> restore_env(:max_child_depth, previous) end)

    {:ok, root} = Continuum.start(DeepFlow, %{depth: 5}, journal: Postgres)

    pump(root)

    # Depths 1 and 2 are created; the grandchild's attempt to create depth 3
    # fails its run at creation time instead of outrunning the cancel cascade.
    failed =
      Repo.all(from(r in Run, where: r.state == "failed", select: r.error))
      |> Enum.map(&decode/1)

    assert Enum.any?(failed, fn
             {_kind, %RuntimeError{message: message}, _stack} ->
               message =~ "max_child_depth_exceeded"

             _other ->
               false
           end)
  end

  test "a cancel cascade deeper than max_child_depth is loud, not silent" do
    previous = Application.get_env(:continuum, :max_child_depth)
    Application.put_env(:continuum, :max_child_depth, 2)
    on_exit(fn -> restore_env(:max_child_depth, previous) end)

    # Hand-build a chain deeper than the bound (legacy data / lowered config).
    [root, c1, c2, c3] = insert_chain(4)

    handler_id = "cascade-truncated-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :run, :cancel_cascade_truncated],
        fn event, measurements, metadata, test_pid ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert :ok = Continuum.cancel(root, journal: Postgres)

    assert_receive {:telemetry, [:continuum, :run, :cancel_cascade_truncated], %{count: 1},
                    %{run_id: ^root, max_child_depth: 2}},
                   1_000

    states = Map.new(Repo.all(from(r in Run, select: {r.id, r.state})))
    assert states[c1] == "failed"
    assert states[c2] == "failed"
    # The descendant beyond the bound keeps running — that is the documented
    # truncation the telemetry makes visible.
    assert states[c3] == "suspended"
  end

  test "children inherit the parent's namespace and attributes" do
    {:ok, parent_id} =
      Continuum.start(SequentialParentFlow, %{id: "ns"},
        journal: Postgres,
        namespace: "tenant-a",
        attributes: %{tenant: "acme"}
      )

    pump(parent_id)

    [child] = children_of(parent_id)
    child_row = Repo.one!(from(r in Run, where: r.id == ^child.id))
    assert child_row.namespace == "tenant-a"
    assert child_row.attributes == %{"tenant" => "acme"}
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

  defp insert_chain(count) do
    Enum.reduce(1..count, [], fn _i, acc ->
      run_id = Ecto.UUID.generate()
      parent_id = List.first(acc)

      %Run{}
      |> Ecto.Changeset.change(%{
        id: run_id,
        workflow: inspect(SlowLeafFlow),
        version_hash: "chain-fixture",
        state: "suspended",
        input: :erlang.term_to_binary(%{}),
        parent_run_id: parent_id
      })
      |> Repo.insert!()

      [run_id | acc]
    end)
    |> Enum.reverse()
  end

  defp restore_env(key, nil), do: Application.delete_env(:continuum, key)
  defp restore_env(key, value), do: Application.put_env(:continuum, key, value)

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
