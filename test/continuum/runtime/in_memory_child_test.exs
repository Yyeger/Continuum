defmodule Continuum.Runtime.InMemoryChildTest do
  @moduledoc """
  Child workflows on the in-memory journal, and their parity with Postgres.

  Parity is the point of the last test here: `Snapshot.step_from/2` reads
  `input_hash` off child pairs, so an in-memory event shape that drifts from
  the durable one produces golden histories that pass in test and drift in
  production — worse than having no test.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{ActivityWorker, Dispatcher, Instance}
  alias Continuum.Runtime.Journal.{InMemory, Postgres}
  alias Continuum.Schema.{ActivityTask, Event, Run}
  alias Continuum.Test

  defmodule Leaf do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, {:leaf, input.id}}
  end

  defmodule FailingLeaf do
    use Continuum.Workflow, version: 1

    def run(_input), do: raise("leaf boom")
  end

  defmodule SequentialParent do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, leaf} = await(child(Leaf.run(%{id: input.id})))
      {:ok, {:parent_saw, leaf}}
    end
  end

  defmodule FanOutParent do
    use Continuum.Workflow, version: 1

    def run(input) do
      results =
        input.ids
        |> Enum.map(fn id -> start_child(Leaf, %{id: id}, id: "leaf-#{id}") end)
        |> Enum.map(&await_child/1)

      {:ok, results}
    end
  end

  defmodule FailingParent do
    use Continuum.Workflow, version: 1

    def run(_input) do
      {:ok, {:child_result, await(child(FailingLeaf.run(%{})))}}
    end
  end

  defmodule DeepParent do
    use Continuum.Workflow, version: 1

    def run(%{depth: d}) do
      if d > 0 do
        await(child(DeepParent.run(%{depth: d - 1})))
      else
        {:ok, :bottom}
      end
    end
  end

  setup do
    Test.reset_in_memory!()
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "a parent awaits an in-memory child and resumes with its result" do
    {:ok, parent_id} = Test.start_synchronous(SequentialParent, %{id: "a"})

    assert {:ok, %{state: :completed, result: {:ok, {:parent_saw, {:leaf, "a"}}}}} =
             Continuum.await(parent_id, 2_000)
  end

  test "a parent fans out to several in-memory children" do
    {:ok, parent_id} = Test.start_synchronous(FanOutParent, %{ids: ["x", "y", "z"]})

    assert {:ok, %{state: :completed, result: {:ok, results}}} =
             Continuum.await(parent_id, 2_000)

    assert results == [{:ok, {:leaf, "x"}}, {:ok, {:leaf, "y"}}, {:ok, {:leaf, "z"}}]
  end

  test "a failing child surfaces to the parent as an error, not a crash" do
    {:ok, parent_id} = Test.start_synchronous(FailingParent, %{})

    assert {:ok, %{state: :completed, result: {:ok, {:child_result, {:error, _}}}}} =
             Continuum.await(parent_id, 2_000)
  end

  test "max_child_depth is enforced in memory as it is durably" do
    previous = Application.get_env(:continuum, :max_child_depth)
    Application.put_env(:continuum, :max_child_depth, 2)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:continuum, :max_child_depth)
        value -> Application.put_env(:continuum, :max_child_depth, value)
      end
    end)

    {:ok, root} = Test.start_synchronous(DeepParent, %{depth: 5})

    # Depths 1 and 2 are created; the attempt to create depth 3 fails its run at
    # creation time, and that failure propagates back up through the awaits.
    assert {:ok, %{state: :completed, result: {:error, failure}}} =
             Continuum.await(root, 2_000)

    assert %Continuum.RunFailure{
             reason: %Continuum.Runtime.JournalError{
               op: :start_child!,
               reason: {:max_child_depth_exceeded, depth: 3, max_child_depth: 2}
             }
           } = failure
  end

  test "in-memory and Postgres child event streams are identical modulo run ids" do
    {:ok, memory_parent} = Test.start_synchronous(SequentialParent, %{id: "parity"})
    {:ok, _} = Continuum.await(memory_parent, 2_000)

    {:ok, durable_parent} = Continuum.start(SequentialParent, %{id: "parity"}, journal: Postgres)
    pump(durable_parent)
    {:ok, _} = Continuum.await(durable_parent, 2_000, journal: Postgres)

    memory = normalize(Test.history(memory_parent, journal: InMemory))
    durable = normalize(Postgres.load(Instance.default(), durable_parent))

    assert memory == durable
  end

  test "durable command ids name the generated entrypoint, in-memory ones the module" do
    {:ok, memory_parent} = Test.start_synchronous(SequentialParent, %{id: "c3"})
    {:ok, _} = Continuum.await(memory_parent, 2_000)

    {:ok, durable_parent} = Continuum.start(SequentialParent, %{id: "c3"}, journal: Postgres)
    pump(durable_parent)
    {:ok, _} = Continuum.await(durable_parent, 2_000, journal: Postgres)

    assert command_module(Test.history(memory_parent, journal: InMemory)) == SequentialParent

    assert command_module(Postgres.load(Instance.default(), durable_parent)) ==
             SequentialParent.__continuum_entrypoint__()
  end

  defp command_module(history) do
    history |> hd() |> Map.fetch!(:command_id) |> elem(1)
  end

  # Run ids, the child run ids derived from them, and the module inside the
  # command id are the only differences the parity assertion tolerates.
  # Everything else — event types, order, seq, and the `input_hash` the snapshot
  # compactor reads — must match.
  defp normalize(history) do
    Enum.map(history, fn event ->
      event
      |> Map.drop([:inserted_at])
      |> Map.replace_lazy(:child_run_id, fn _ -> :child_run_id end)
      |> Map.replace_lazy(:command_id, &normalize_command_id/1)
    end)
  end

  # `Continuum.Workflow.rewrite_self_references/3` rewrites the module atom
  # inside the escaped command tuple, so a durable run — dispatched through the
  # generated `V_<hash>` entrypoint — journals `MyFlow.V_<hash>` where an
  # in-memory run journals `MyFlow`. That is register item C3, whose v0.8
  # disposition is "document, decide at v0.10"; the test above pins the current
  # behaviour so a fix cannot land here silently.
  defp normalize_command_id(command_id) when is_tuple(command_id) do
    command_id
    |> Tuple.to_list()
    |> Enum.map(&strip_generated_entrypoint/1)
    |> List.to_tuple()
  end

  defp normalize_command_id(other), do: other

  defp strip_generated_entrypoint(value) when is_atom(value) and not is_nil(value) do
    parts = Module.split(value)

    case List.last(parts) do
      "V_" <> _hash -> Module.concat(Enum.drop(parts, -1))
      _ -> value
    end
  rescue
    ArgumentError -> value
  end

  defp strip_generated_entrypoint(value), do: value

  defp pump(parent_id, attempts \\ 300)

  defp pump(parent_id, attempts) when attempts > 0 do
    if terminal?(parent_id) do
      :ok
    else
      Dispatcher.dispatch_once(owner: "memory-child-pump", batch_size: 10)
      ActivityWorker.Dispatcher.dispatch_once(owner: "memory-child-pump-act", batch_size: 10)
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
end
