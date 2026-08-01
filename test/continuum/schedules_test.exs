defmodule Continuum.SchedulesTest do
  use Continuum.Test.DataCase, async: false

  import Ecto.Query

  alias Continuum.Runtime.{Instance, ScheduleRunner}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Run, RunIngressKey, Schedule}

  defmodule ScheduledFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:scheduled, input.value}
  end

  setup do
    Repo.delete_all(Schedule)
    Repo.delete_all(RunIngressKey)
    Repo.delete_all(Run)
    :ok
  end

  test "future schedules remain pending" do
    future = DateTime.utc_now() |> DateTime.add(1, :hour)
    assert {:ok, schedule_id} = Continuum.schedule_at(ScheduledFlow, %{value: 1}, future)

    assert {:ok, 0} = ScheduleRunner.dispatch_once()

    assert {:ok, %{id: ^schedule_id, state: :scheduled, attempt: 0}} =
             Continuum.Schedules.get(schedule_id)

    assert Repo.aggregate(Run, :count) == 0
  end

  test "due schedules start exactly one durable run with their metadata" do
    due = DateTime.utc_now() |> DateTime.add(-1, :second)

    assert {:ok, schedule_id} =
             Continuum.schedule_at(ScheduledFlow, %{value: 42}, due,
               namespace: "billing",
               attributes: %{invoice: "inv-42"}
             )

    assert {:ok, %{run_id: run_id}} = Continuum.Schedules.get(schedule_id)
    assert {:ok, 1} = ScheduleRunner.dispatch_once()

    assert {:ok, %{state: :completed, result: {:scheduled, 42}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert {:ok, schedule} = Continuum.Schedules.get(schedule_id)
    assert schedule.state == :started
    assert schedule.attempt == 1
    assert %DateTime{} = schedule.started_at

    run = Repo.get!(Run, run_id)
    assert run.namespace == "billing"
    assert run.attributes == %{"invoice" => "inv-42"}

    assert {:ok, 0} = ScheduleRunner.dispatch_once()
    assert Repo.aggregate(Run, :count) == 1
  end

  test "a crash after run insertion converges on the preallocated run" do
    due = DateTime.utc_now() |> DateTime.add(-1, :second)
    assert {:ok, schedule_id} = Continuum.schedule_at(ScheduledFlow, %{value: 7}, due)
    assert {:ok, %{run_id: run_id}} = Continuum.Schedules.get(schedule_id)

    :ok = Postgres.start_run(Instance.default(), run_id, ScheduledFlow, %{value: 7})

    assert {:ok, 1} = ScheduleRunner.dispatch_once()
    assert {:ok, %{state: :started, run_id: ^run_id}} = Continuum.Schedules.get(schedule_id)
    assert Repo.aggregate(from(r in Run, where: r.id == ^run_id), :count) == 1
  end

  test "pending schedules can be cancelled" do
    future = DateTime.utc_now() |> DateTime.add(1, :hour)
    assert {:ok, schedule_id} = Continuum.schedule_at(ScheduledFlow, %{value: 1}, future)

    assert :ok = Continuum.Schedules.cancel(schedule_id)
    assert {:ok, %{state: :cancelled}} = Continuum.Schedules.get(schedule_id)
    assert {:error, :already_started} = Continuum.Schedules.cancel(schedule_id)
    assert {:ok, 0} = ScheduleRunner.dispatch_once()
  end

  test "schedule inputs obey durable-term validation" do
    assert {:error, %Continuum.DurableTermError{}} =
             Continuum.schedule_at(ScheduledFlow, %{owner: self()}, DateTime.utc_now())

    assert Repo.aggregate(Schedule, :count) == 0
  end
end
