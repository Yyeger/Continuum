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

  test "a failing schedule backs off instead of retrying at a fixed interval" do
    schedule_id = unstartable_schedule!()

    assert {:ok, 1} = ScheduleRunner.dispatch_once()
    assert {:ok, first} = Continuum.Schedules.get(schedule_id)
    assert first.state == :scheduled
    assert first.attempt == 1
    assert is_binary(first.last_error)

    first_delay_ms = DateTime.diff(first.scheduled_at, DateTime.utc_now(), :millisecond)
    assert first_delay_ms > 0

    # It is not due yet, so it no longer occupies a claim slot every poll.
    assert {:ok, 0} = ScheduleRunner.dispatch_once()

    # Advance through a few more attempts; the delay has to widen.
    later_delay_ms =
      Enum.reduce(1..4, first_delay_ms, fn _, _acc ->
        force_due!(schedule_id)
        assert {:ok, 1} = ScheduleRunner.dispatch_once()
        {:ok, schedule} = Continuum.Schedules.get(schedule_id)
        DateTime.diff(schedule.scheduled_at, DateTime.utc_now(), :millisecond)
      end)

    assert later_delay_ms > first_delay_ms
    assert {:ok, %{attempt: 5, state: :scheduled}} = Continuum.Schedules.get(schedule_id)
  end

  test "a schedule that exhausts its attempts fails terminally and stops claiming" do
    schedule_id = unstartable_schedule!()

    # One claim short of the cap.
    Repo.update_all(from(s in Schedule, where: s.id == ^schedule_id), set: [attempt: 11])

    events = attach_schedule_telemetry()

    assert {:ok, 1} = ScheduleRunner.dispatch_once()

    assert {:ok, schedule} = Continuum.Schedules.get(schedule_id)
    assert schedule.state == :failed
    assert schedule.attempt == 12
    assert is_binary(schedule.last_error)

    assert_receive {:schedule_telemetry, [:continuum, :schedule, :failed], _measurements,
                    %{schedule_id: ^schedule_id, attempt: 12}}

    # A terminal schedule is never claimed again, at any interval.
    force_due!(schedule_id)
    assert {:ok, 0} = ScheduleRunner.dispatch_once()
    assert {:ok, %{state: :failed, attempt: 12}} = Continuum.Schedules.get(schedule_id)

    :telemetry.detach(events)
  end

  test "a terminally failed schedule is an actionable health finding" do
    schedule_id = unstartable_schedule!()
    Repo.update_all(from(s in Schedule, where: s.id == ^schedule_id), set: [attempt: 11])
    assert {:ok, 1} = ScheduleRunner.dispatch_once()

    assert {:ok, report} = Continuum.Health.report(repo: Repo, partition_months: 1)
    assert report.schedules.failed_count == 1
    assert [%{schedule_id: ^schedule_id, attempt: 12}] = report.schedules.failed
    assert report.status == :degraded
  end

  defp unstartable_schedule!(value \\ 1) do
    due = DateTime.utc_now() |> DateTime.add(-1, :second)
    {:ok, schedule_id} = Continuum.schedule_at(ScheduledFlow, %{value: value}, due)

    # No node can resolve this version, which is the failure mode that used to
    # retry every five seconds for the lifetime of the deployment.
    Repo.update_all(
      from(s in Schedule, where: s.id == ^schedule_id),
      set: [version_hash: :crypto.strong_rand_bytes(32)]
    )

    schedule_id
  end

  defp force_due!(schedule_id) do
    Repo.update_all(
      from(s in Schedule, where: s.id == ^schedule_id),
      set: [scheduled_at: DateTime.utc_now() |> DateTime.add(-1, :second)]
    )
  end

  defp attach_schedule_telemetry do
    handler = "schedule-telemetry-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach_many(
      handler,
      [[:continuum, :schedule, :failed], [:continuum, :schedule, :retried]],
      fn event, measurements, metadata, _config ->
        send(test, {:schedule_telemetry, event, measurements, metadata})
      end,
      nil
    )

    handler
  end

  test "schedule inputs obey durable-term validation" do
    assert {:error, %Continuum.DurableTermError{}} =
             Continuum.schedule_at(ScheduledFlow, %{owner: self()}, DateTime.utc_now())

    assert Repo.aggregate(Schedule, :count) == 0
  end
end
