defmodule Continuum.Runtime.RecoveryTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.ActivityWorker
  alias Continuum.Runtime.Dispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease
  alias Continuum.Runtime.Recovery
  alias Continuum.Schema.{ActivityTask, Event, Run, Signal, Timer}

  defmodule RecoverFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      value = Continuum.side_effect(fn -> input.seed + 1 end)
      {:ok, value}
    end
  end

  defmodule RecoverActivity do
    use Continuum.Activity, retry: [max_attempts: 2]

    def run(n), do: {:ok, n * 2}
  end

  defmodule ActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, value} = activity(RecoverActivity.run(input.seed))
      {:ok, value}
    end
  end

  defmodule TimerFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      timer(input.ms)
      {:ok, :fired}
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

  test "clears orphaned run leases so the dispatcher can resume them" do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, RecoverFlow, %{seed: 4})

    assert {:ok, %Lease{token: token}} = Lease.acquire(run_id, owner: "dead-node")
    :ok = Postgres.suspend!(Continuum.Runtime.Instance.default(), run_id, token)

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [lease_expires_at: past_time()]
    )

    assert {:ok, %{runs: 1}} = Recovery.recover_once()

    recovered = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert recovered.state == "suspended"
    assert recovered.lease_owner == nil
    assert recovered.lease_token == nil

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "recovery-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 5}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "does not clear another node's live run lease" do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, RecoverFlow, %{seed: 4})

    assert {:ok, %Lease{token: token}} = Lease.acquire(run_id, owner: "live-node")
    :ok = Postgres.suspend!(Continuum.Runtime.Instance.default(), run_id, token)

    assert {:ok, %{runs: 0}} = Recovery.recover_once()

    recovered = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert recovered.state == "suspended"
    assert recovered.lease_owner == "live-node"
    assert recovered.lease_token == token
    assert DateTime.compare(recovered.lease_expires_at, DateTime.utc_now()) == :gt
  end

  test "does not requeue activity tasks with a live worker lease" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 6}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [state: "leased", lease_owner: "dead-node", lease_expires_at: future_time()]
    )

    assert {:ok, %{activity_tasks: 0}} = Recovery.recover_once()

    recovered = Repo.one!(ActivityTask)
    assert recovered.state == "leased"
    assert recovered.lease_owner == "dead-node"
    assert recovered.lease_expires_at != nil

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [lease_expires_at: past_time()]
    )

    assert {:ok, %{activity_tasks: 1}} = Recovery.recover_once()

    recovered = Repo.one!(ActivityTask)
    assert recovered.state == "available"
    assert recovered.lease_owner == nil
    assert recovered.lease_expires_at == nil
    # The interrupted execution consumed an attempt.
    assert recovered.attempt == 2

    assert {:ok, 1} =
             ActivityWorker.Dispatcher.dispatch_once(owner: "recovery-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 12}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "nudges due timers for dispatcher pickup" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(Timer, :count) == 1
    end)

    timer = Repo.one!(Timer)
    due_at = past_time()

    Repo.update_all(
      from(t in Timer, where: t.id == ^timer.id),
      set: [fires_at: due_at]
    )

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [next_wakeup_at: future_time()]
    )

    assert {:ok, %{timers: 1}} = Recovery.recover_once()

    recovered = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert DateTime.compare(recovered.next_wakeup_at, DateTime.utc_now()) != :gt
  end

  defp future_time do
    DateTime.utc_now()
    |> DateTime.add(60, :second)
    |> DateTime.truncate(:microsecond)
  end

  defp past_time do
    DateTime.utc_now()
    |> DateTime.add(-60, :second)
    |> DateTime.truncate(:microsecond)
  end

  defp assert_eventually(fun, attempts \\ 20)

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
