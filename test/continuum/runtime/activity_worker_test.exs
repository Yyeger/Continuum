defmodule Continuum.Runtime.ActivityWorkerTest do
  use Continuum.Test.DataCase, async: false

  import ExUnit.CaptureLog

  alias Continuum.Runtime.ActivityWorker.Dispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityResult, ActivityTask, Event, Run}

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

  defmodule IdempotentActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(n) do
      Agent.update(__MODULE__, &(&1 + 1))
      {:ok, {:live, n}}
    end

    def idempotency_key([n]), do: "idempotent:#{n}"
  end

  defmodule IdempotentFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, value} = activity(IdempotentActivity.run(input.seed))
      {:ok, value}
    end
  end

  defmodule NilKeyActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(n) do
      Agent.update(__MODULE__, &(&1 + 1))
      {:ok, n}
    end

    def idempotency_key([_n]), do: nil
  end

  defmodule NilKeyFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, value} = activity(NilKeyActivity.run(input.seed))
      {:ok, value}
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

  defmodule DeclinedError do
    defexception [:reason, :code, message: "payment declined"]
  end

  defmodule DeclinedActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(_n) do
      raise %DeclinedError{reason: :insufficient_funds, code: "card_declined"}
    end
  end

  defmodule DeclinedFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      case activity(DeclinedActivity.run(input.seed)) do
        {:error, %DeclinedError{reason: reason, code: code}} ->
          {:ok, {reason, code}}

        other ->
          {:unexpected, other}
      end
    end
  end

  setup do
    start_supervised!(%{
      id: FlakyActivity,
      start: {Agent, :start_link, [fn -> 0 end, [name: FlakyActivity]]}
    })

    start_supervised!(%{
      id: IdempotentActivity,
      start: {Agent, :start_link, [fn -> 0 end, [name: IdempotentActivity]]}
    })

    start_supervised!(%{
      id: NilKeyActivity,
      start: {Agent, :start_link, [fn -> 0 end, [name: NilKeyActivity]]}
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

  test "activity idempotency hit skips the MFA and journals the committed result" do
    committed_result = {:ok, {:cached, 5}}
    insert_activity_result(IdempotentActivity, "idempotent:5", committed_result)

    handler_id = "activity-idempotency-hit-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :activity, :idempotency_hit],
        fn event, measurements, metadata, test_pid ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(IdempotentFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, {:cached, 5}}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert Agent.get(IdempotentActivity, & &1) == 0
    assert_received {:telemetry, [:continuum, :activity, :idempotency_hit], %{}, metadata}
    assert metadata.idempotency_key == "idempotent:5"

    [%{type: :activity_scheduled}, %{type: :activity_completed, payload: ^committed_result}] =
      Postgres.load(Continuum.Runtime.Instance.default(), run_id)
  end

  test "activity completion uses the side-table winner when the idempotency insert conflicts" do
    committed_result = {:ok, {:winner, 7}}
    insert_activity_result(IdempotentActivity, "idempotent:7", committed_result)

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(IdempotentFlow, %{seed: 7}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [state: "leased", lease_owner: "worker-a", lease_expires_at: future_time()]
    )

    claimed_task =
      task.mfa
      |> decode_term()
      |> Map.merge(%{
        id: task.id,
        run_id: task.run_id,
        instance: Continuum.Runtime.Instance.default(),
        seq: task.seq,
        attempt: task.attempt,
        lease_owner: "worker-a"
      })

    lease_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

    assert :ok =
             Postgres.complete_activity_task!(
               Continuum.Runtime.Instance.default(),
               claimed_task,
               {:ok, {:loser, 7}},
               lease_token,
               idempotency: [module: IdempotentActivity, key: "idempotent:7"]
             )

    [%{type: :activity_scheduled}, %{type: :activity_completed, payload: ^committed_result}] =
      Postgres.load(Continuum.Runtime.Instance.default(), run_id)

    assert Repo.one!(ActivityTask).result |> decode_term() == committed_result
    assert Repo.aggregate(ActivityResult, :count) == 1
  end

  test "nil idempotency keys do not write activity_results rows" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(NilKeyFlow, %{seed: 11}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 11}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert Agent.get(NilKeyActivity, & &1) == 1
    assert Repo.aggregate(ActivityResult, :count) == 0
  end

  test "replay of an idempotent activity reads the journal, not the side table" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(IdempotentFlow, %{seed: 9}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, {:live, 9}}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    history = Postgres.load(Continuum.Runtime.Instance.default(), run_id)
    Repo.delete_all(ActivityResult)

    assert {:ok, {:ok, {:live, 9}}} =
             Continuum.Test.replay(IdempotentFlow, %{seed: 9}, history, journal: Postgres)
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

  test "preserves exception structs from failed activities" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DeclinedFlow, %{seed: 9}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, {:insufficient_funds, "card_declined"}}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    task_error = Repo.one!(ActivityTask).error |> decode_term()
    assert %DeclinedError{reason: :insufficient_funds, code: "card_declined"} = task_error

    [%{type: :activity_scheduled}, %{type: :activity_failed, error: event_error}] =
      Postgres.load(Continuum.Runtime.Instance.default(), run_id)

    assert %DeclinedError{reason: :insufficient_funds, code: "card_declined"} = event_error
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
      Postgres.complete_activity_task!(
        Continuum.Runtime.Instance.default(),
        stale_task,
        {:ok, 10},
        lease_token
      )
    end

    assert ["activity_scheduled"] = event_types(run_id)
    assert Repo.one!(ActivityTask).state == "leased"
  end

  test "activity completion rejects stale run authority before appending an event" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [state: "leased", lease_owner: "worker-a", lease_expires_at: future_time()]
    )

    claimed_task =
      task.mfa
      |> decode_term()
      |> Map.merge(%{
        id: task.id,
        run_id: task.run_id,
        seq: task.seq,
        attempt: task.attempt,
        lease_owner: "worker-a"
      })

    current_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

    assert_raise RuntimeError, ~r/lease_mismatch/, fn ->
      Postgres.complete_activity_task!(
        Continuum.Runtime.Instance.default(),
        claimed_task,
        {:ok, 10},
        current_token + 1
      )
    end

    assert ["activity_scheduled"] = event_types(run_id)
    assert Repo.one!(ActivityTask).state == "leased"
  end

  test "claim_one claims an available task at perform time" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "oban-worker",
               30
             )

    assert claimed.executor == :oban
    assert claimed.id == task.id
    assert claimed.run_id == run_id
    assert claimed.lease_owner == "oban-worker"
    assert is_integer(claimed.run_lease_token)

    task = Repo.one!(ActivityTask)
    assert task.state == "leased"
    assert task.lease_owner == "oban-worker"

    assert :not_available =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "second-worker",
               30
             )
  end

  test "claim_one rejects stale attempts before running an activity" do
    {:ok, _run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [attempt: task.attempt + 1]
    )

    assert :stale =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "stale-worker",
               30
             )

    assert Repo.one!(ActivityTask).state == "available"
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

  defp decode_term(binary) when is_binary(binary), do: :erlang.binary_to_term(binary)

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

  defp insert_activity_result(module, key, result) do
    Repo.insert!(%ActivityResult{
      activity_module: Atom.to_string(module),
      idempotency_key: key,
      run_id: Ecto.UUID.generate(),
      seq: 1,
      result: :erlang.term_to_binary(result),
      completed_at: DateTime.utc_now()
    })
  end
end
