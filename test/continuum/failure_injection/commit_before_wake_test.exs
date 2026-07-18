defmodule Continuum.FailureInjection.CommitBeforeWakeTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.ActivityWorker.Dispatcher, as: ActivityDispatcher
  alias Continuum.Runtime.{Instance, Journal.Postgres, SignalRouter}
  alias Continuum.Schema.{ActivityTask, Run, Timer}

  @moduletag :failure_injection

  defmodule Activity do
    use Continuum.Activity, retry: [max_attempts: 1]
    def run(value), do: {:ok, value * 2}
  end

  defmodule ActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(Activity.run(input.value))
    end
  end

  defmodule TimerFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      timer(60_000)
      :timer_fired
    end
  end

  defmodule SignalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      await(signal(:continue))
    end
  end

  test "activity commit survives writer death before the process wake" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{value: 21}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             ActivityDispatcher.claim_one(
               Instance.default(),
               task.id,
               task.attempt,
               "ci-kill",
               30
             )

    commit_then_kill(fn ->
      Postgres.complete_activity_task!(
        Instance.default(),
        claimed,
        {:ok, 42},
        claimed.run_lease_token
      )
    end)

    assert Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
    assert :ok = SignalRouter.catch_up_once()

    assert {:ok, %{state: :completed, result: {:ok, 42}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "timer commit survives writer death before the process wake" do
    {:ok, run_id} = Continuum.Runtime.Engine.start_run(TimerFlow, %{}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(Timer, :count) == 1 end)
    timer = Repo.one!(Timer)
    lease_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

    commit_then_kill(fn ->
      Postgres.fire_timer!(Instance.default(), run_id, timer.id, lease_token)
    end)

    assert Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
    assert :ok = SignalRouter.catch_up_once()

    assert {:ok, %{state: :completed, result: :timer_fired}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "signal commit survives writer death before the process wake" do
    {:ok, run_id} = Continuum.Runtime.Engine.start_run(SignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
    end)

    commit_then_kill(fn ->
      Postgres.deliver_signal!(Instance.default(), run_id, :continue, :signalled)
    end)

    assert Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
    assert :ok = SignalRouter.catch_up_once()

    assert {:ok, %{state: :completed, result: :signalled}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  defp commit_then_kill(commit) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        result = commit.()
        send(parent, {:failure_injection_committed, self(), result})

        receive do
          :kill_before_wake -> Process.exit(self(), :kill)
        end
      end)

    assert_receive {:failure_injection_committed, ^pid, result}, 1_000

    assert (case result do
              :ok -> true
              {:ok, run_id} -> is_binary(run_id)
              _ -> false
            end)

    send(pid, :kill_before_wake)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
    :ok
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(fun, 0), do: assert(fun.())

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
