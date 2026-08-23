defmodule Continuum.Runtime.TimerOwnerSeqTest do
  @moduledoc """
  Firing a timer reads two events, not the run's whole history.

  `timer_winner/2` used to `load_events/1` and decode every event for the run,
  then scan it twice, inside the `FOR UPDATE` transaction — worst on exactly
  the workload timers exist for, a `timer(days(30))` in a loop. The owning
  event's seq is now stored on the timer row by both writers.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run, Timer}

  defmodule TimerFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      timer(days(30))
      {:ok, :fired}
    end
  end

  defmodule TimeoutFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      {:ok, await(signal(:approve, timeout: days(30)))}
    end
  end

  setup do
    Repo.delete_all(Timer)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "timer/1 records the seq of the timer_started event that armed it" do
    {:ok, run_id} = Continuum.start(TimerFlow, %{}, journal: Postgres)
    :ok = Continuum.Test.drive_until_state(run_id, [:suspended])

    timer = Repo.one!(from(t in Timer, where: t.run_id == ^run_id))
    started = Repo.one!(from(e in Event, where: e.run_id == ^run_id and e.seq == 0))

    assert timer.owner_seq == started.seq
    assert started.event_type == "timer_started"
  end

  test "a signal timeout records the seq of the signal_awaited event" do
    {:ok, run_id} = Continuum.start(TimeoutFlow, %{}, journal: Postgres)
    :ok = Continuum.Test.drive_until_state(run_id, [:suspended])

    timer = Repo.one!(from(t in Timer, where: t.run_id == ^run_id))
    awaited = Repo.one!(from(e in Event, where: e.run_id == ^run_id and e.seq == 0))

    assert timer.owner_seq == awaited.seq
    assert awaited.event_type == "signal_awaited"
  end

  test "firing works from the recorded seq" do
    {:ok, run_id} = Continuum.start(TimerFlow, %{}, journal: Postgres)
    :ok = Continuum.Test.drive_until_state(run_id, [:suspended])
    :ok = Continuum.Test.elapse_timers!(run_id)

    assert {:ok, %{state: :completed, result: {:ok, :fired}}} = Continuum.Test.drive(run_id)
  end

  test "a timer armed before the migration still fires through the full scan" do
    {:ok, run_id} = Continuum.start(TimerFlow, %{}, journal: Postgres)
    :ok = Continuum.Test.drive_until_state(run_id, [:suspended])

    # Exactly what a row written by v0.7.2 looks like.
    Repo.update_all(from(t in Timer, where: t.run_id == ^run_id), set: [owner_seq: nil])

    :ok = Continuum.Test.elapse_timers!(run_id)

    assert {:ok, %{state: :completed, result: {:ok, :fired}}} = Continuum.Test.drive(run_id)
  end
end
