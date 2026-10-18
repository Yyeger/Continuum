defmodule Continuum.SQLite.RuntimeTest do
  @moduledoc """
  End-to-end runs on a full SQLite runtime: the dispatcher, activity
  dispatcher, timer wheel, signal router, heartbeater, schedule runner, and
  version registry all run against the SQLite test database.
  """

  use Continuum.Test.SQLiteCase

  alias Continuum.Runtime.Instance
  alias Continuum.Schema.{ActivityTask, Event, Run}
  alias Continuum.Test.ImpureProbe

  @attempts __MODULE__.Attempts

  @doc false
  def bump(probe, tag), do: :ets.update_counter(@attempts, {probe, tag}, 1, {{probe, tag}, 0})

  @doc false
  def count(probe, tag) do
    case :ets.lookup(@attempts, {probe, tag}) do
      [{_key, count}] -> count
      [] -> 0
    end
  end

  defmodule Flaky do
    use Continuum.Activity, retry: [max_attempts: 3, backoff: :constant, base_ms: 10]

    def run(probe, value) do
      # A raised failure is retried; a returned {:error, _} is a result.
      if Continuum.SQLite.RuntimeTest.bump(probe, :flaky) == 1, do: raise("transient")
      {:ok, value * 2}
    end
  end

  defmodule Step do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(value), do: {:ok, value + 1}
  end

  defmodule Blocking do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(probe) do
      Continuum.SQLite.RuntimeTest.bump(probe, :blocking)
      ImpureProbe.notify_with_self(probe, :blocking_started)

      receive do
        :release -> {:ok, :released}
      end
    end
  end

  defmodule ActivityFlow do
    use Continuum.Workflow, version: 1

    def run(%{probe: probe, value: value}) do
      {:ok, doubled} = activity(Flaky.run(probe, value))
      [{:ok, a}, {:ok, b}] = activity_all([Step.run(doubled), Step.run(doubled + 10)])
      {:ok, a + b}
    end
  end

  defmodule TimerFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      timer(200)
      {:ok, :woke}
    end
  end

  defmodule SignalFlow do
    use Continuum.Workflow, version: 1

    def run(%{timeout: timeout}) do
      case await(signal(:go, timeout: timeout)) do
        :timeout -> {:ok, :timed_out}
        payload -> {:ok, payload}
      end
    end
  end

  defmodule ChildFlow do
    use Continuum.Workflow, version: 1

    def run(%{n: n}) do
      {:ok, value} = activity(Step.run(n))
      {:ok, value}
    end
  end

  defmodule ParentFlow do
    use Continuum.Workflow, version: 1

    def run(%{n: n}) do
      {:ok, child} = await(child(ChildFlow.run(%{n: n})))
      {:ok, {:child, child}}
    end
  end

  defmodule LoopFlow do
    use Continuum.Workflow, version: 1

    def run(%{left: 0, total: total}), do: {:ok, total}

    def run(%{left: left, total: total}) do
      {:ok, total} = activity(Step.run(total))
      continue_as_new(%{left: left - 1, total: total})
    end
  end

  defmodule CrashFlow do
    use Continuum.Workflow, version: 1

    def run(%{probe: probe}) do
      {:ok, :released} = activity(Blocking.run(probe))
      {:ok, value} = activity(Step.run(1))
      {:ok, value}
    end
  end

  @workflows [ActivityFlow, TimerFlow, SignalFlow, ChildFlow, ParentFlow, LoopFlow, CrashFlow]

  setup do
    :ets.new(@attempts, [:named_table, :public, :set])
    name = :"sqlite_runtime_#{System.unique_integer([:positive])}"

    children =
      Continuum.children(
        name: name,
        repo: SQLiteRepo,
        workflow_modules: @workflows,
        heartbeater: [interval_ms: 200],
        recovery: false,
        dispatcher: [enabled?: true, interval_ms: 25],
        activity_dispatcher: [enabled?: true, interval_ms: 25],
        timer_wheel: [enabled?: true, refresh_ms: 60_000],
        schedule_runner: [interval_ms: 25],
        signal_router: [catch_up_interval_ms: 100]
      )

    start_supervised!(%{
      id: name,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
      type: :supervisor
    })

    on_exit(fn -> Instance.register(%{Instance.lookup(name) | repo: nil}) end)
    %{name: name}
  end

  test "activities retry, fan out, and complete", %{name: name} do
    probe = ImpureProbe.register()
    {:ok, run_id} = Continuum.start(ActivityFlow, %{probe: probe, value: 5}, instance: name)

    assert {:ok, %{state: :completed, result: {:ok, 32}}} =
             Continuum.await(run_id, 10_000, instance: name)

    assert count(probe, :flaky) == 2

    # Retries reuse their task row; the batch adds one row per member.
    assert SQLiteRepo.aggregate(from(t in ActivityTask, where: t.run_id == ^run_id), :count) ==
             3
  end

  test "a timer fires from the after-commit wheel notification", %{name: name} do
    {:ok, run_id} = Continuum.start(TimerFlow, %{}, instance: name)

    # The wheel refreshes only every 60 seconds; finishing well inside that
    # proves the arming transaction's local notification reached it.
    assert {:ok, %{state: :completed, result: {:ok, :woke}}} =
             Continuum.await(run_id, 5_000, instance: name)
  end

  test "signals are delivered once and timeouts win when nothing arrives", %{name: name} do
    {:ok, waiting} = Continuum.start(SignalFlow, %{timeout: 60_000}, instance: name)
    {:ok, timing_out} = Continuum.start(SignalFlow, %{timeout: 200}, instance: name)

    eventually(fn -> run_state(waiting) == "suspended" end)

    # Deduplicate on a mailbox the run is not consuming, so it stays live.
    assert {:ok, :delivered} =
             Continuum.signal_unique(waiting, :noise, 1, "noise-1", instance: name)

    assert {:ok, :duplicate} =
             Continuum.signal_unique(waiting, :noise, 1, "noise-1", instance: name)

    assert {:ok, :delivered} =
             Continuum.signal_unique(waiting, :go, %{by: "ops"}, "go-1", instance: name)

    assert {:ok, %{state: :completed, result: {:ok, %{by: "ops"}}}} =
             Continuum.await(waiting, 5_000, instance: name)

    assert {:ok, %{state: :completed, result: {:ok, :timed_out}}} =
             Continuum.await(timing_out, 5_000, instance: name)
  end

  test "a parent awaits its child to completion", %{name: name} do
    {:ok, run_id} = Continuum.start(ParentFlow, %{n: 41}, instance: name)

    assert {:ok, %{state: :completed, result: {:ok, {:child, 42}}}} =
             Continuum.await(run_id, 10_000, instance: name)

    assert [%Run{state: "completed"}] =
             SQLiteRepo.all(from(r in Run, where: r.parent_run_id == ^run_id))
  end

  test "cancelling a waiting run cancels it durably", %{name: name} do
    {:ok, run_id} = Continuum.start(SignalFlow, %{timeout: 60_000}, instance: name)
    eventually(fn -> run_state(run_id) == "suspended" end)

    assert :ok = Continuum.cancel(run_id, instance: name)
    assert {:ok, %{state: :cancelled}} = Continuum.get_run(run_id, instance: name)
    assert {:error, :run_terminal} = Continuum.signal(run_id, :go, :late, instance: name)
  end

  test "continue_as_new chains resolve to their final result", %{name: name} do
    {:ok, run_id} = Continuum.start(LoopFlow, %{left: 3, total: 0}, instance: name)

    assert {:ok, %{state: :completed, result: {:ok, 3}}} =
             Continuum.await(run_id, 10_000, instance: name)

    assert SQLiteRepo.aggregate(from(r in Run, where: r.correlation_id == ^run_id), :count) == 4
  end

  test "idempotent starts converge on one run", %{name: name} do
    opts = [instance: name, idempotency_key: "order-7"]

    assert {:ok, run_id, :started} = Continuum.start_unique(TimerFlow, %{}, opts)
    assert {:ok, ^run_id, :existing} = Continuum.start_unique(TimerFlow, %{}, opts)
  end

  test "one-shot and recurring schedules start their runs", %{name: name} do
    {:ok, schedule_id} =
      Continuum.schedule_at(TimerFlow, %{}, DateTime.add(DateTime.utc_now(), 50, :millisecond),
        instance: name
      )

    {:ok, recurring_id} =
      Continuum.schedule_every(TimerFlow, %{}, 60_000,
        instance: name,
        overlap: :allow,
        missed: :skip,
        starts_at: DateTime.utc_now()
      )

    eventually(fn ->
      match?({:ok, %{state: :started}}, Continuum.Schedules.get(schedule_id, instance: name))
    end)

    {:ok, %{run_id: run_id}} = Continuum.Schedules.get(schedule_id, instance: name)
    assert {:ok, %{state: :completed}} = Continuum.await(run_id, 5_000, instance: name)

    eventually(fn ->
      case Continuum.Schedules.list_occurrences(recurring_id, instance: name) do
        {:ok, %{entries: [%{state: "started"}]}} -> true
        _other -> false
      end
    end)

    {:ok, definition} = Continuum.Schedules.get_recurring(recurring_id, instance: name)
    assert DateTime.compare(definition.next_occurrence_at, DateTime.utc_now()) == :gt
  end

  test "a run whose engine dies is reclaimed and finishes without repeating work",
       %{name: name} do
    probe = ImpureProbe.register()
    {:ok, run_id} = Continuum.start(CrashFlow, %{probe: probe}, instance: name)

    assert_receive {:blocking_started, worker}, 5_000
    [{engine, _}] = Registry.lookup(Instance.lookup(name).registry, run_id)

    # Lose the node's engine and its lease, then let the activity finish: its
    # result commits under the run's fencing token, which nobody else holds.
    Process.exit(engine, :kill)
    send(worker, :release)
    eventually(fn -> activity_completed?(run_id) end)

    SQLiteRepo.update_all(from(r in Run, where: r.id == ^run_id),
      set: [lease_expires_at: DateTime.add(DateTime.utc_now(), -60, :second)]
    )

    assert {:ok, %{state: :completed, result: {:ok, 2}}} =
             Continuum.await(run_id, 10_000, instance: name)

    assert count(probe, :blocking) == 1
  end

  defp run_state(run_id) do
    SQLiteRepo.one(from(r in Run, where: r.id == ^run_id, select: r.state))
  end

  defp activity_completed?(run_id) do
    SQLiteRepo.exists?(
      from(e in Event, where: e.run_id == ^run_id and e.event_type == "activity_completed")
    )
  end

  defp eventually(fun, attempts \\ 200) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition was not met in time")

      true ->
        Process.sleep(25)
        eventually(fun, attempts - 1)
    end
  end
end
