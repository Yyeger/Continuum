defmodule Continuum.Runtime.ParallelCompensationTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.ActivityWorker.Dispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.Event

  defmodule Activities do
    use Continuum.Activity, retry: [max_attempts: 1]

    def first(pid), do: send(pid, {:activity, :first}) && {:ok, :first}
    def second(pid), do: send(pid, {:activity, :second}) && {:ok, :second}
    def undo_first(pid), do: send(pid, {:compensated, :first}) && :undone_first
    def undo_second(pid), do: send(pid, {:compensated, :second}) && :undone_second
  end

  defmodule ParallelFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, _first} =
        activity(Activities.first(input.pid), compensate: {Activities, :undo_first, [input.pid]})

      {:ok, _second} =
        activity(Activities.second(input.pid),
          compensate: {Activities, :undo_second, [input.pid]}
        )

      raise "boom"
    rescue
      e ->
        compensate_all(mode: :parallel)
        reraise e, __STACKTRACE__
    end
  end

  test "parallel compensate_all schedules all compensations before any complete" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ParallelFlow, %{pid: self()}, journal: Postgres)

    assert_eventually(fn -> event_count(run_id, "activity_scheduled") == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "parallel-comp", batch_size: 1)
    assert_receive {:activity, :first}

    assert_eventually(fn -> event_count(run_id, "activity_scheduled") == 2 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "parallel-comp", batch_size: 1)
    assert_receive {:activity, :second}

    assert_eventually(fn ->
      event_count(run_id, "compensation_scheduled") == 2 and
        event_count(run_id, "compensation_completed") == 0
    end)

    assert {:ok, 2} = Dispatcher.dispatch_once(owner: "parallel-comp", batch_size: 2)
    assert_receive {:compensated, :second}
    assert_receive {:compensated, :first}

    assert {:error, %{state: :failed}} = Continuum.await(run_id, 1_000, journal: Postgres)

    types = event_types(run_id)
    scheduled_indexes = indexes(types, "compensation_scheduled")
    completed_indexes = indexes(types, "compensation_completed")

    assert length(scheduled_indexes) == 2
    assert length(completed_indexes) == 2
    assert Enum.max(scheduled_indexes) < Enum.min(completed_indexes)
  end

  defp event_count(run_id, type) do
    Repo.aggregate(from(e in Event, where: e.run_id == ^run_id and e.event_type == ^type), :count)
  end

  defp event_types(run_id) do
    Repo.all(from(e in Event, where: e.run_id == ^run_id, order_by: e.seq, select: e.event_type))
  end

  defp indexes(values, target) do
    values
    |> Enum.with_index()
    |> Enum.flat_map(fn
      {^target, index} -> [index]
      _ -> []
    end)
  end

  defp assert_eventually(fun, attempts \\ 40)

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
