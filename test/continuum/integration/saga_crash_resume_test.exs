defmodule Continuum.Integration.SagaCrashResumeTest do
  @moduledoc """
  Kill the engine mid-`compensate_all` and assert recovery completes the
  remaining LIFO compensations (V0.3 PR 4, §8).
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{ActivityWorker, Dispatcher, Instance, Recovery}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}

  defmodule ChargeActivity do
    use Continuum.Activity, retry: [max_attempts: 1]
    def run(id), do: {:ok, {:charged, id}}
  end

  defmodule ReserveActivity do
    use Continuum.Activity, retry: [max_attempts: 1]
    def run(id), do: {:ok, {:reserved, id}}
  end

  defmodule Compensations do
    def refund(id), do: {:refunded, id}
    def release(id), do: {:released, id}
  end

  defmodule SagaFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, _charge} =
        activity(ChargeActivity.run(input.id), compensate: {Compensations, :refund, [input.id]})

      {:ok, _reserve} =
        activity(ReserveActivity.run(input.id),
          compensate: {Compensations, :release, [input.id]}
        )

      compensate_all()
      {:error, :rolled_back}
    end
  end

  setup do
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "killed engine mid-compensate_all resumes and finishes remaining LIFO compensations" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SagaFlow, %{id: "order-1"}, journal: Postgres)

    # Both forward activities complete.
    assert_eventually(fn -> available_tasks() == 1 end)
    drive_worker()
    assert_eventually(fn -> count(run_id, "activity_completed") == 1 end)

    assert_eventually(fn -> available_tasks() == 1 end)
    drive_worker()
    assert_eventually(fn -> count(run_id, "activity_completed") == 2 end)

    # The engine reaches compensate_all, schedules the first (reserve)
    # compensation, and suspends — but we do NOT run that compensation yet.
    assert_eventually(fn ->
      count(run_id, "compensation_scheduled") == 1 and run_state(run_id) == "suspended"
    end)

    # Kill the engine mid-compensate_all.
    [{engine_pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run_id)
    ref = Process.monitor(engine_pid)
    Process.exit(engine_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^engine_pid, :killed}, 1_000
    assert_eventually(fn -> Registry.lookup(Continuum.Runtime.Registry, run_id) == [] end)

    # Recover and resume onto a fresh engine.
    expire_lease(run_id)
    assert {:ok, %{runs: 1}} = Recovery.recover_once()
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "saga-resume", batch_size: 1)
    assert_eventually(fn -> Registry.lookup(Continuum.Runtime.Registry, run_id) != [] end)

    # Finish the remaining compensations: reserve first (still pending), then charge.
    assert_eventually(fn -> available_tasks() == 1 end)
    drive_worker()
    assert_eventually(fn -> count(run_id, "compensation_completed") == 1 end)

    assert_eventually(fn -> count(run_id, "compensation_scheduled") == 2 end)
    assert_eventually(fn -> available_tasks() == 1 end)
    drive_worker()

    assert {:ok, %{state: :completed, result: {:error, :rolled_back}}} =
             Continuum.await(run_id, 2_000, journal: Postgres)

    events = Postgres.load(Instance.default(), run_id)

    [charge_id, reserve_id] =
      events |> Enum.filter(&(&1.type == :activity_completed)) |> Enum.map(& &1.command_id)

    compensated_order =
      events
      |> Enum.filter(&(&1.type == :compensation_completed))
      |> Enum.sort_by(& &1.seq)
      |> Enum.map(& &1.target_activity_id)

    # LIFO: the most-recent activity (reserve) is compensated first.
    assert compensated_order == [reserve_id, charge_id]
  end

  defp drive_worker do
    ActivityWorker.Dispatcher.dispatch_once(owner: "saga-worker", batch_size: 1)
  end

  defp available_tasks do
    Repo.aggregate(from(t in ActivityTask, where: t.state == "available"), :count)
  end

  defp count(run_id, event_type) do
    Repo.aggregate(
      from(e in Event, where: e.run_id == ^run_id and e.event_type == ^event_type),
      :count
    )
  end

  defp run_state(run_id) do
    Repo.one(from(r in Run, where: r.id == ^run_id, select: r.state))
  end

  defp expire_lease(run_id) do
    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [
        lease_expires_at:
          DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)
      ]
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
