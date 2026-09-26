defmodule Continuum.ActivityMapTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.ActivityWorker.Dispatcher, as: ActivityDispatcher
  alias Continuum.Runtime.{Engine, Instance}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}
  alias Continuum.Test

  defmodule Work do
    use Continuum.Activity, retry: [max_attempts: 1]
    def process(%{fail: true}), do: {:error, :rejected}

    def process(%{wait: true, id: id}) do
      send(Process.whereis(Continuum.ActivityMapTest), {:started, id, self()})

      receive do
        :finish -> {:ok, id}
      end
    end

    def process(%{id: id}), do: {:ok, id}
  end

  defmodule Flow do
    use Continuum.Workflow

    def run(input) do
      activity_map(input.items, &Work.process/1, concurrency: input.concurrency, key: :id)
    end
  end

  defmodule PositionalFlow do
    use Continuum.Workflow
    def run(items), do: activity_map(items, &Work.process/1, concurrency: 2)
  end

  defmodule BadInputFlow do
    use Continuum.Workflow

    def run(_input),
      do: activity_map([%{id: 1, value: fn -> :bad end}], &Work.process/1, concurrency: 2)
  end

  setup do
    Test.reset_in_memory!()
    Process.register(self(), __MODULE__)
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "empty inputs still record membership and duplicate values use separate positions" do
    for inputs <- [[], [%{id: 1}, %{id: 1}]] do
      {:ok, id} = Test.start_synchronous(PositionalFlow, inputs)
      expected = Enum.map(inputs, &{:ok, &1.id})
      assert {:ok, %{result: ^expected}} = Continuum.await(id)
      history = Test.history(id)
      assert hd(history).type == :activity_map_started
      assert {:ok, ^expected} = Continuum.Replay.run(PositionalFlow, inputs, history)
    end
  end

  test "invalid concurrency, nondurable input and duplicate explicit keys fail before scheduling" do
    for input <- [
          %{items: [%{id: 1}, %{id: 1}], concurrency: 2},
          %{items: [], concurrency: 0},
          %{items: [], concurrency: 1_001}
        ] do
      {:ok, id} = Test.start_synchronous(Flow, input)
      assert {:error, %{state: :failed}} = Continuum.await(id)
      assert Test.history(id) == []
    end

    {:ok, id} = Test.start_synchronous(BadInputFlow, nil)
    assert {:error, %{state: :failed}} = Continuum.await(id)
    assert Test.history(id) == []
  end

  test "full membership and concurrency are validated in event and snapshot replay" do
    input = %{items: [%{id: 1}, %{id: 2}, %{id: 3}], concurrency: 2}
    {:ok, id} = Test.start_synchronous(Flow, input)
    assert {:ok, %{result: expected}} = Continuum.await(id)
    history = Test.history(id)

    {:ok, snapshot} =
      Continuum.Snapshot.compact(id, Flow.__continuum_workflow__().version_hash, history)

    assert {:ok, ^expected} = Continuum.Replay.run(Flow, input, [], snapshot: snapshot)

    for changed <- [
          %{input | items: [%{id: 1}, %{id: 2}, %{id: 9}]},
          %{input | items: Enum.reverse(input.items)},
          %{input | items: []},
          %{input | concurrency: 1}
        ],
        {events, opts} <- [{history, []}, {[], [snapshot: snapshot]}] do
      assert {:error, {:error, %Continuum.ReplayDriftError{}, _}} =
               Continuum.Replay.run(Flow, changed, events, opts)
    end
  end

  test "partial replay never schedules the remaining windows" do
    input = %{items: [%{id: 1}, %{id: 2}, %{id: 3}], concurrency: 2}
    {:ok, id} = Continuum.start(Flow, input, journal: Postgres)
    wait_until(fn -> task_count(id) == 2 end)
    {:ok, report} = Continuum.Replay.of_run(id)
    assert report.outcome == :suspended
    assert task_count(id) == 2
    :ok = Continuum.cancel(id, journal: Postgres)
  end

  test "windows bound pending tasks and out-of-order completions retain input order" do
    input = %{items: for(id <- 1..5, do: %{id: id, wait: true}), concurrency: 2}
    {:ok, id} = Continuum.start(Flow, input, journal: Postgres)
    wait_until(fn -> task_count(id) == 2 end)
    assert {:ok, 2} = ActivityDispatcher.dispatch_once()
    assert_receive {:started, 1, first}, 1_000
    assert_receive {:started, 2, second}, 1_000
    send(second, :finish)
    wait_until(fn -> completed_count(id) == 1 end)
    Engine.wake(Instance.default(), id)
    settled(id)
    assert task_count(id) == 2
    assert {:ok, 0} = ActivityDispatcher.dispatch_once()
    send(first, :finish)
    wait_until(fn -> task_count(id) == 4 end)
    assert {:ok, 2} = ActivityDispatcher.dispatch_once()
    assert_receive {:started, 3, third}, 1_000
    assert_receive {:started, 4, fourth}, 1_000
    send(fourth, :finish)
    send(third, :finish)
    wait_until(fn -> task_count(id) == 5 end)
    assert {:ok, 1} = ActivityDispatcher.dispatch_once()
    assert_receive {:started, 5, fifth}, 1_000
    send(fifth, :finish)

    assert {:ok, %{result: [{:ok, 1}, {:ok, 2}, {:ok, 3}, {:ok, 4}, {:ok, 5}]}} =
             Continuum.await(id, 1_000, journal: Postgres)
  end

  test "crash resume preserves queued membership, failures, and snapshot replay" do
    input = %{items: [%{id: 1}, %{id: 2, fail: true}, %{id: 3}], concurrency: 2}
    {:ok, id} = Continuum.start(Flow, input, journal: Postgres)
    wait_until(fn -> task_count(id) == 2 end)
    settled(id)
    :ok = Test.crash!(id)
    :ok = Test.expire_lease!(id)
    assert {:ok, %{result: [{:ok, 1}, {:error, :rejected}, {:ok, 3}]}} = Test.drive(id)
    assert task_count(id) == 3
    assert {:ok, report} = Continuum.Replay.of_run(id)
    assert report.outcome == :completed
    history = Postgres.load(Instance.default(), id)
    hash = Flow.__continuum_workflow__().version_hash
    {:ok, snapshot} = Continuum.Snapshot.compact(id, hash, history)
    :ok = Postgres.take_snapshot!(Instance.default(), snapshot)
    assert {:ok, snapshot_report} = Continuum.Replay.of_run(id)
    assert snapshot_report.outcome == :completed
  end

  test "cancelled maps discard the active window and never enqueue later members" do
    input = %{items: Enum.map(1..5, &%{id: &1}), concurrency: 2}
    {:ok, id} = Continuum.start(Flow, input, journal: Postgres)
    wait_until(fn -> task_count(id) == 2 end)
    :ok = Continuum.cancel(id, journal: Postgres)
    assert {:ok, 0} = ActivityDispatcher.dispatch_once()
    assert task_count(id) == 2

    assert Repo.all(from(t in ActivityTask, where: t.run_id == ^id, select: t.state)) ==
             ["discarded", "discarded"]
  end

  defp settled(id) do
    [{pid, _}] = Registry.lookup(Instance.default().registry, id)
    :sys.get_state(pid)
  end

  defp task_count(id), do: Repo.aggregate(from(t in ActivityTask, where: t.run_id == ^id), :count)

  defp completed_count(id),
    do:
      Repo.aggregate(
        from(t in ActivityTask, where: t.run_id == ^id and t.state == "completed"),
        :count
      )

  defp wait_until(fun, attempts \\ 200)
  defp wait_until(_fun, 0), do: flunk("condition did not become true")

  defp wait_until(fun, attempts) do
    if fun.(),
      do: :ok,
      else:
        (
          Process.sleep(5)
          wait_until(fun, attempts - 1)
        )
  end
end
