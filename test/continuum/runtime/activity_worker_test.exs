defmodule Continuum.Runtime.ActivityWorkerTest do
  use Continuum.Test.DataCase, async: false

  import ExUnit.CaptureLog

  alias Continuum.Runtime.ActivityWorker.Dispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}

  defmodule DoubleActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(n), do: {:ok, n * 2}

    def idempotency_key([n]), do: "double:#{n}"
  end

  defmodule ActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, value} = activity(DoubleActivity.run(input.seed))
      {:ok, value + 1}
    end
  end

  defmodule FlakyActivity do
    use Continuum.Activity, retry: [max_attempts: 2, backoff: :exponential, base_ms: 1]

    def run(n) do
      attempt = Agent.get_and_update(__MODULE__, fn current -> {current + 1, current + 1} end)

      if attempt == 1 do
        raise "not yet"
      else
        {:ok, n}
      end
    end
  end

  defmodule RetryFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, value} = activity(FlakyActivity.run(input.seed))
      {:ok, value}
    end
  end

  setup do
    start_supervised!(%{
      id: FlakyActivity,
      start: {Agent, :start_link, [fn -> 0 end, [name: FlakyActivity]]}
    })

    :ok
  end

  test "runs a scheduled activity and wakes the workflow" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    [task] = Repo.all(ActivityTask)
    decoded = decode_term(task.mfa)

    assert decoded.idempotency_key == "double:5"
    assert decoded.retry == [max_attempts: 1]

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 11}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert Repo.one!(ActivityTask).state == "completed"
  end

  test "retries failed activities with backoff" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(RetryFlow, %{seed: 9}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    log =
      capture_log(fn ->
        assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)
      end)

    refute log =~ "terminating"

    assert_eventually(fn ->
      Repo.one!(ActivityTask).state == "available"
    end)

    task = Repo.one!(ActivityTask)
    assert task.state == "available"
    assert task.attempt == 2
    assert task.available_at != nil
    assert {:error, :timeout} = Continuum.await(run_id, 25, journal: Postgres)

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [available_at: DateTime.add(DateTime.utc_now(), -1, :second)]
    )

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 9}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "activity completion rejects stale task authority before appending an event" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [state: "leased", lease_owner: "worker-b", lease_expires_at: future_time()]
    )

    stale_task =
      task.mfa
      |> decode_term()
      |> Map.merge(%{
        id: task.id,
        run_id: task.run_id,
        seq: task.seq,
        attempt: task.attempt,
        lease_owner: "worker-a"
      })

    lease_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

    assert_raise RuntimeError, ~r/activity_task_lease_mismatch/, fn ->
      Postgres.complete_activity_task!(stale_task, {:ok, 10}, lease_token)
    end

    assert ["activity_scheduled"] = event_types(run_id)
    assert Repo.one!(ActivityTask).state == "leased"
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

  defp decode_term(%{"__term__" => encoded}) when is_binary(encoded) do
    :erlang.binary_to_term(Base.decode64!(encoded))
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

  defp future_time do
    DateTime.utc_now()
    |> DateTime.add(60, :second)
    |> DateTime.truncate(:microsecond)
  end
end
