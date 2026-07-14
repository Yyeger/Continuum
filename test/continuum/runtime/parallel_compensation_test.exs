defmodule Continuum.Runtime.ParallelCompensationTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.ActivityWorker.Dispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}

  defmodule Activities do
    use Continuum.Activity, retry: [max_attempts: 1]

    def first(probe), do: notify(probe, {:activity, :first}, {:ok, :first})
    def second(probe), do: notify(probe, {:activity, :second}, {:ok, :second})
    def undo_first(probe), do: notify(probe, {:compensated, :first}, :undone_first)
    def undo_second(probe), do: notify(probe, {:compensated, :second}, :undone_second)

    defp notify(probe, message, result) do
      Continuum.Test.ImpureProbe.notify(probe, message)
      result
    end
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

  defmodule SingletonParallelFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, _first} =
        activity(Activities.first(input.pid), compensate: {Activities, :undo_first, [input.pid]})

      compensate_all(mode: :parallel)
      after_compensation = Continuum.side_effect(fn -> :after_parallel_compensation end)
      {:ok, after_compensation}
    end
  end

  test "parallel compensate_all schedules all compensations before any complete" do
    probe = Continuum.Test.ImpureProbe.register()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ParallelFlow, %{pid: probe}, journal: Postgres)

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

  test "single parallel compensation replays from compacted snapshot step" do
    probe = Continuum.Test.ImpureProbe.register()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SingletonParallelFlow, %{pid: probe}, journal: Postgres)

    assert_eventually(fn -> event_count(run_id, "activity_scheduled") == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "parallel-singleton", batch_size: 1)
    assert_receive {:activity, :first}

    assert_eventually(fn -> event_count(run_id, "compensation_scheduled") == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "parallel-singleton", batch_size: 1)
    assert_receive {:compensated, :first}

    assert {:ok, %{state: :completed, result: {:ok, :after_parallel_compensation}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    history = Postgres.load(Continuum.Runtime.Instance.default(), run_id)

    assert Enum.map(history, & &1.type) == [
             :activity_scheduled,
             :activity_completed,
             :compensation_scheduled,
             :compensation_completed,
             :side_effect
           ]

    prefix = Enum.take(history, 4)
    remaining = Enum.drop(history, 4)

    {:ok, snapshot} =
      Continuum.Snapshot.compact(
        "parallel-singleton",
        SingletonParallelFlow.__continuum_workflow__().version_hash,
        prefix
      )

    assert {:ok, {:ok, :after_parallel_compensation}} =
             Continuum.Test.replay(
               SingletonParallelFlow,
               %{pid: probe},
               remaining,
               snapshot: snapshot,
               journal: Postgres
             )
  end

  test "catch-up recovers a committed compensation when the immediate wake is lost" do
    probe = Continuum.Test.ImpureProbe.register()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SingletonParallelFlow, %{pid: probe}, journal: Postgres)

    assert_eventually(fn -> event_count(run_id, "activity_scheduled") == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "lost-compensation-wake", batch_size: 1)
    assert_receive {:activity, :first}

    assert_eventually(fn -> event_count(run_id, "compensation_scheduled") == 1 end)

    task =
      Repo.one!(from(t in ActivityTask, where: t.run_id == ^run_id and t.state == "available"))

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "lost-compensation-wake",
               30
             )

    lease_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

    assert :ok =
             Postgres.complete_compensation_task!(
               Continuum.Runtime.Instance.default(),
               claimed,
               :undone_first,
               lease_token
             )

    next_wakeup_at =
      Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.next_wakeup_at))

    assert %DateTime{} = next_wakeup_at
    %{rows: [[db_now]]} = Repo.query!("SELECT clock_timestamp()")
    assert DateTime.compare(next_wakeup_at, db_now) in [:lt, :eq]
    assert :ok = Continuum.Runtime.SignalRouter.catch_up_once()

    assert {:ok, %{state: :completed, result: {:ok, :after_parallel_compensation}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
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
