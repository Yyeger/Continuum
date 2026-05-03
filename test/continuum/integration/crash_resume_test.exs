defmodule Continuum.Integration.CrashResumeTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.ActivityWorker
  alias Continuum.Runtime.Dispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Recovery
  alias Continuum.Runtime.TimerWheel
  alias Continuum.Schema.{ActivityTask, Event, Run, Signal, Timer}

  defmodule StepActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(value), do: {:ok, value}
  end

  defmodule ActivityTimerActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, first} = activity(StepActivity.run(input.seed + 1))
      timer(input.timer_ms)
      {:ok, second} = activity(StepActivity.run(first + 1))
      {:ok, second}
    end
  end

  setup do
    Repo.delete_all(Signal)
    Repo.delete_all(Timer)
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "killed engine resumes from Postgres history and completes after a pending timer" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityTimerActivityFlow, %{seed: 40, timer_ms: 60_000},
        journal: Postgres
      )

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)

    assert {:ok, 1} =
             ActivityWorker.Dispatcher.dispatch_once(
               owner: "crash-resume-activity",
               batch_size: 1
             )

    assert_eventually(fn ->
      event_types(run_id) == ["activity_scheduled", "activity_completed", "timer_started"]
    end)

    [{engine_pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run_id)
    ref = Process.monitor(engine_pid)

    Process.exit(engine_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^engine_pid, :killed}, 1_000
    assert_eventually(fn -> Registry.lookup(Continuum.Runtime.Registry, run_id) == [] end)

    run_before_recovery = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert run_before_recovery.state == "suspended"
    assert is_integer(run_before_recovery.lease_token)

    force_timer_due(run_id)
    expire_lease(run_id)

    assert {:ok, %{runs: 1, timers: 1}} = Recovery.recover_once()
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "crash-resume-dispatcher", batch_size: 1)

    assert_eventually(fn ->
      [{pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run_id)
      pid != engine_pid
    end)

    assert {:ok, 1} = TimerWheel.fire_due_once(batch_size: 1)

    assert_eventually(fn ->
      event_types(run_id) == [
        "activity_scheduled",
        "activity_completed",
        "timer_started",
        "timer_fired",
        "activity_scheduled"
      ]
    end)

    assert {:ok, 1} =
             ActivityWorker.Dispatcher.dispatch_once(
               owner: "crash-resume-activity",
               batch_size: 1
             )

    assert {:ok, %{state: :completed, result: {:ok, 42}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert event_types(run_id) == [
             "activity_scheduled",
             "activity_completed",
             "timer_started",
             "timer_fired",
             "activity_scheduled",
             "activity_completed"
           ]
  end

  defp event_types(run_id) do
    Repo.all(
      from(e in Event,
        where: e.run_id == ^run_id,
        order_by: [asc: e.seq],
        select: e.event_type
      )
    )
  end

  defp force_timer_due(run_id) do
    Repo.update_all(
      from(t in Timer, where: t.run_id == ^run_id and t.fired == false),
      set: [fires_at: past_time()]
    )
  end

  defp expire_lease(run_id) do
    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [lease_expires_at: past_time()]
    )
  end

  defp past_time do
    DateTime.utc_now()
    |> DateTime.add(-60, :second)
    |> DateTime.truncate(:microsecond)
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
