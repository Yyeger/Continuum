defmodule Continuum.Integration.ChildCrashResumeTest do
  @moduledoc """
  Kill a parent engine while a child is in flight; the resumed parent must find
  the same child's terminal event and complete (V0.3 PR 5, §8).
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{Dispatcher, Instance, Recovery}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}

  defmodule LeafFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, {:leaf, input.id}}
  end

  defmodule ParentFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, leaf} = await(child(LeafFlow.run(%{id: input.id})))
      {:ok, {:parent_saw, leaf}}
    end
  end

  setup do
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "resumed parent finds the child's terminal event after the parent crashes mid-flight" do
    {:ok, parent_id} = Continuum.start(ParentFlow, %{id: "x"}, journal: Postgres)

    # The parent starts the child and suspends awaiting it.
    assert_eventually(fn -> child_id(parent_id) != nil end)
    assert_eventually(fn -> run_state(parent_id) == "suspended" end)

    # Kill the parent engine while the child run is still in flight.
    [{engine_pid, _}] = Registry.lookup(Continuum.Runtime.Registry, parent_id)
    ref = Process.monitor(engine_pid)
    Process.exit(engine_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^engine_pid, :killed}, 1_000
    assert_eventually(fn -> Registry.lookup(Continuum.Runtime.Registry, parent_id) == [] end)

    # The child runs to completion independently (its own lease).
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "child-resume-child", batch_size: 5)
    child = child_id(parent_id)
    assert_eventually(fn -> run_state(child) == "completed" end)

    # Recover and resume the parent; it must find the child's terminal event.
    # The child set the parent's next_wakeup_at to wall-clock `now`, but the
    # sandbox transaction freezes Postgres `now()` at its start, so force the
    # wakeup due (the production dispatcher's `now()` advances on its own).
    make_dispatchable(parent_id)
    assert {:ok, _} = Recovery.recover_once()
    assert {:ok, _} = Dispatcher.dispatch_once(owner: "child-resume-parent", batch_size: 5)

    assert {:ok, %{state: :completed, result: {:ok, {:parent_saw, {:leaf, "x"}}}}} =
             Continuum.await(parent_id, 2_000, journal: Postgres)

    events = Postgres.load(Instance.default(), parent_id)
    assert Enum.any?(events, &(&1.type == :child_completed and &1.child_run_id == child))
  end

  defp child_id(parent_id) do
    Repo.one(from(r in Run, where: r.parent_run_id == ^parent_id, select: r.id))
  end

  defp run_state(run_id), do: Repo.one(from(r in Run, where: r.id == ^run_id, select: r.state))

  defp make_dispatchable(run_id) do
    past = DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)

    Repo.update_all(from(r in Run, where: r.id == ^run_id),
      set: [lease_expires_at: past, next_wakeup_at: past]
    )
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
