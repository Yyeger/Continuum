defmodule Continuum.RecurringSchedulesTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.RecurringSchedules
  alias Continuum.Runtime.{Instance, ScheduleRunner}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{RecurringSchedule, Run, Schedule}

  defmodule Flow do
    use Continuum.Workflow
    def run(input), do: input
  end

  defmodule ContinuingFlow do
    use Continuum.Workflow
    def run(:root), do: continue_as_new(:wait)
    def run(:wait), do: await(signal(:done))
  end

  test "catch-up is bounded globally and per definition, and cursor advances atomically" do
    first = DateTime.add(DateTime.utc_now(), -10, :minute)
    {:ok, id} = create(first, missed: :catch_up, max_catch_up: 2)
    assert {:ok, 2} = RecurringSchedules.materialize(Instance.default(), 10)
    assert [_, _] = occurrences(id)
    assert {:ok, %{next_occurrence_at: next}} = Continuum.Schedules.get_recurring(id)
    assert next == DateTime.add(first, 2, :minute)
    assert {:ok, 1} = RecurringSchedules.materialize(Instance.default(), 1)
    assert [_, _, _] = occurrences(id)

    assert {:error, :crash} =
             Repo.transaction(fn ->
               assert {:ok, 2} = RecurringSchedules.materialize(Instance.default(), 10)
               Repo.rollback(:crash)
             end)

    assert [_, _, _] = occurrences(id)
    assert Repo.get!(RecurringSchedule, id).next_occurrence_at == DateTime.add(first, 3, :minute)
  end

  test "missed skip coalesces to the last due time and pause/resume retains policy" do
    first = DateTime.add(DateTime.utc_now(), -605, :second)
    {:ok, id} = create(first, missed: :skip)
    :ok = Continuum.Schedules.pause_recurring(id)
    assert {:ok, 0} = RecurringSchedules.materialize(Instance.default(), 25)

    assert {:ok, %{state: "paused", next_occurrence_at: ^first}} =
             Continuum.Schedules.get_recurring(id)

    :ok = Continuum.Schedules.resume_recurring(id)
    assert {:ok, 1} = RecurringSchedules.materialize(Instance.default(), 25)
    [occurrence] = occurrences(id)
    assert occurrence.occurrence_at == DateTime.add(first, 600, :second)

    assert Repo.get!(RecurringSchedule, id).next_occurrence_at ==
             DateTime.add(first, 660, :second)

    assert {:ok, 0} = RecurringSchedules.materialize(Instance.default(), 25)
  end

  test "overlap skip includes queued work and continued runs" do
    first = DateTime.add(DateTime.utc_now(), -125, :second)

    {:ok, id} =
      Continuum.schedule_every(ContinuingFlow, :root, 60_000,
        starts_at: first,
        overlap: :skip,
        missed: :catch_up
      )

    assert {:ok, 3} = RecurringSchedules.materialize(Instance.default(), 25)
    assert Enum.frequencies_by(occurrences(id), & &1.state) == %{"scheduled" => 1, "skipped" => 2}
    [scheduled] = Enum.filter(occurrences(id), &(&1.state == "scheduled"))
    assert {:ok, 1} = ScheduleRunner.dispatch_once()

    wait_until(fn ->
      Repo.exists?(from(r in Run, where: r.continued_from_run_id == ^scheduled.run_id))
    end)

    Repo.update_all(from(s in RecurringSchedule, where: s.id == ^id),
      set: [next_occurrence_at: DateTime.add(DateTime.utc_now(), -1, :second)]
    )

    assert {:ok, 1} = RecurringSchedules.materialize(Instance.default(), 25)
    assert Enum.count(occurrences(id), &(&1.state == "skipped")) == 3
    :ok = Continuum.cancel(scheduled.run_id, journal: Postgres)
  end

  test "occurrences retain run IDs across concurrent generation and runner restart" do
    first = DateTime.add(DateTime.utc_now(), -5, :second)
    {:ok, id} = create(first, missed: :catch_up)

    results =
      1..4
      |> Task.async_stream(fn _ -> RecurringSchedules.materialize(Instance.default(), 25) end)
      |> Enum.to_list()

    assert Enum.all?(results, &match?({:ok, {:ok, _}}, &1))
    [occurrence] = occurrences(id)
    # Simulate loss after occurrence creation but before dispatch.
    assert {:ok, 0} = RecurringSchedules.materialize(Instance.default(), 25)
    assert {:ok, 1} = ScheduleRunner.dispatch_once()
    assert {:ok, %{result: :input}} = Continuum.await(occurrence.run_id, 1_000, journal: Postgres)
    Repo.update_all(from(s in Schedule, where: s.id == ^occurrence.id), set: [state: "scheduled"])
    assert {:ok, 1} = ScheduleRunner.dispatch_once()
    assert Repo.aggregate(from(r in Run, where: r.id == ^occurrence.run_id), :count) == 1
    assert hd(occurrences(id)).run_id == occurrence.run_id
  end

  test "unknown-version retries keep occurrence identity and deployment checks include paused definitions" do
    first = DateTime.add(DateTime.utc_now(), -1, :second)
    {:ok, id} = create(first, missed: :skip)
    missing = "missing-recurring-version"

    Repo.update_all(from(s in RecurringSchedule, where: s.id == ^id),
      set: [version_hash: missing]
    )

    assert {:ok, 1} = ScheduleRunner.dispatch_once()
    [occurrence] = occurrences(id)
    assert occurrence.state == "scheduled"
    assert occurrence.last_error =~ "unknown_version"
    assert occurrence.attempt == 1
    refute Repo.get(Run, occurrence.run_id)
    :ok = Continuum.Schedules.pause_recurring(id)
    assert {:ok, report} = Continuum.Versions.check()

    assert Enum.any?(
             report.requirements,
             &(&1.version_hash == missing and &1.status == :missing and &1.schedule_count == 2)
           )
  end

  test "listing is bounded and invalid policies are rejected" do
    for _ <- 1..3, do: create(DateTime.add(DateTime.utc_now(), 1, :hour), missed: :skip)
    {:ok, page} = Continuum.Schedules.list_recurring(limit: 2)
    assert [_, _] = page.entries
    assert page.next_cursor
    {:ok, next} = Continuum.Schedules.list_recurring(limit: 2, cursor: page.next_cursor)
    assert [_] = next.entries
    refute next.next_cursor
    assert {:error, :invalid_interval} = Continuum.schedule_every(Flow, :input, 0)
    assert {:error, :invalid_overlap_policy} = Continuum.schedule_every(Flow, :input, 60_000)
    assert {:error, :utc_start_required} = create(~N[2026-01-01 00:00:00], missed: :skip)

    assert {:error, :invalid_max_catch_up} =
             create(DateTime.utc_now(), missed: :catch_up, max_catch_up: 0)
  end

  defp create(first, opts),
    do:
      Continuum.schedule_every(
        Flow,
        :input,
        60_000,
        Keyword.merge([starts_at: first, overlap: :allow], opts)
      )

  defp occurrences(id),
    do:
      Repo.all(
        from(s in Schedule, where: s.recurring_schedule_id == ^id, order_by: s.occurrence_at)
      )

  defp wait_until(fun, attempts \\ 100)
  defp wait_until(_fun, 0), do: flunk("condition did not become true")

  defp wait_until(fun, attempts) do
    if fun.(),
      do: :ok,
      else:
        (
          Process.sleep(10)
          wait_until(fun, attempts - 1)
        )
  end
end
