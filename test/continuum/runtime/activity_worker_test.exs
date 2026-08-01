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

  defmodule LocalIdentityActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(_seed), do: self()
  end

  defmodule LocalIdentityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(LocalIdentityActivity.run(input.seed))
    end
  end

  defmodule UnsafeActivityError do
    defexception [:owner, message: "unsafe activity failure"]
  end

  defmodule UnsafeErrorActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(:raise_pid), do: raise(%UnsafeActivityError{owner: self()})
    def run(:throw_pid), do: throw(self())
    def run(:exit_reference), do: exit(make_ref())
    def run(:oversized), do: throw(String.duplicate("x", 70_000))
  end

  defmodule UnsafeErrorFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: activity(UnsafeErrorActivity.run(input.mode))
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

  defmodule ResilientActivity do
    use Continuum.Activity, retry: [max_attempts: 3]

    def run(n), do: {:ok, n * 2}
  end

  defmodule ResilientFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, value} = activity(ResilientActivity.run(input.seed))
      {:ok, value + 1}
    end
  end

  defmodule SlowActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(sleep_ms) do
      Process.sleep(sleep_ms)
      {:ok, :slept}
    end
  end

  defmodule SlowActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(SlowActivity.run(input.sleep_ms))
    end
  end

  defmodule ProgressActivity do
    use Continuum.Activity, retry: [max_attempts: 1], context: true

    def run(context, value) do
      :ok = Continuum.Activity.Context.heartbeat(context, %{phase: "upload", percent: 50})
      {:ok, value * 2}
    end
  end

  defmodule ProgressFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: activity(ProgressActivity.run(input.value))
  end

  defmodule CancelAwareActivity do
    use Continuum.Activity, retry: [max_attempts: 1], context: true

    def run(context, probe) do
      Continuum.Test.ImpureProbe.notify_with_self(probe, :cancel_aware_started)

      receive do
        :check_cancellation ->
          cancelled? = Continuum.Activity.Context.cancelled?(context)
          Continuum.Test.ImpureProbe.notify(probe, {:activity_cancelled, cancelled?})
          {:ok, :stopped}
      end
    end
  end

  defmodule CancelAwareFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: activity(CancelAwareActivity.run(input.probe))
  end

  defmodule LeaseRaceActivity do
    use Continuum.Activity, retry: [max_attempts: 2]

    def run(probe) do
      Continuum.Test.ImpureProbe.notify_with_self(probe, :lease_race_activity_started)

      receive do
        :finish_lease_race_activity -> {:ok, :finished}
      end
    end
  end

  defmodule LeaseRaceFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(LeaseRaceActivity.run(input.test_pid))
    end
  end

  defmodule ScriptedLeaseRepo do
    def start_link(responses) do
      Agent.start_link(fn -> %{calls: 0, responses: responses} end, name: __MODULE__)
    end

    def calls, do: Agent.get(__MODULE__, & &1.calls)

    def query(_sql, _params) do
      Agent.get_and_update(__MODULE__, fn state ->
        {response, rest} =
          case state.responses do
            [r | rest] -> {r, rest}
            [] -> {{:ok, %{num_rows: 1}}, []}
          end

        {response, %{state | calls: state.calls + 1, responses: rest}}
      end)
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

    assert decoded.retry == [
             max_attempts: 1,
             backoff: :constant,
             base_ms: 1_000,
             max_backoff_ms: 60_000,
             max_retry_horizon_ms: 86_400_000
           ]

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 11}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert Repo.one!(ActivityTask).state == "completed"
  end

  test "turns a node-local activity result into a non-retryable durable failure" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(LocalIdentityFlow, %{seed: 1}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "invalid-result", batch_size: 1)

    assert {:ok,
            %{
              state: :completed,
              result: {:error, %Continuum.DurableTermError{kind: :PID} = error}
            }} = Continuum.await(run_id, 1_000, journal: Postgres)

    assert Exception.message(error) =~ "activity_result"
    assert Repo.one!(ActivityTask).state == "discarded"
    assert event_types(run_id) == ["activity_scheduled", "activity_failed"]
  end

  test "sanitizes non-durable and oversized activity errors before persistence" do
    for {mode, kind} <- [
          raise_pid: :error,
          throw_pid: :throw,
          exit_reference: :exit,
          oversized: :throw
        ] do
      {:ok, run_id} =
        Continuum.Runtime.Engine.start_run(UnsafeErrorFlow, %{mode: mode}, journal: Postgres)

      assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
      assert {:ok, 1} = Dispatcher.dispatch_once(owner: "unsafe-error", batch_size: 1)

      assert {:ok,
              %{
                state: :completed,
                result: {:error, %Continuum.ActivityError{kind: ^kind} = error}
              }} = Continuum.await(run_id, 1_000, journal: Postgres)

      assert :ok = Continuum.DurableTerm.validate(error, :activity_error)
      assert byte_size(:erlang.term_to_binary(error)) < 65_536
      assert Repo.one!(ActivityTask).state == "discarded"

      Repo.delete_all(ActivityTask)
    end
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

  test "catch-up recovers a committed activity completion when the immediate wake is lost" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "lost-completion-wake",
               30
             )

    lease_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

    assert :ok =
             Postgres.complete_activity_task!(
               Continuum.Runtime.Instance.default(),
               claimed,
               {:ok, 10},
               lease_token
             )

    assert_wake_pending(run_id)
    assert :ok = Continuum.Runtime.SignalRouter.catch_up_once()

    assert {:ok, %{state: :completed, result: {:ok, 11}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "catch-up recovers a committed activity failure when the immediate wake is lost" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DeclinedFlow, %{seed: 9}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "lost-failure-wake",
               30
             )

    lease_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))
    error = %DeclinedError{reason: :insufficient_funds, code: "card_declined"}

    assert :ok =
             Postgres.fail_activity_task!(
               Continuum.Runtime.Instance.default(),
               claimed,
               error,
               lease_token
             )

    assert_wake_pending(run_id)
    assert :ok = Continuum.Runtime.SignalRouter.catch_up_once()

    assert {:ok, %{state: :completed, result: {:ok, {:insufficient_funds, "card_declined"}}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
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

    assert_raise Continuum.Runtime.JournalError, ~r/activity_task_lease_mismatch/, fn ->
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

  test "activity completion rejects a stale attempt from the same task owner" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [
        state: "leased",
        attempt: task.attempt + 1,
        lease_owner: "worker-a",
        lease_expires_at: future_time()
      ]
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

    assert_raise Continuum.Runtime.JournalError, ~r/activity_task_attempt_mismatch/, fn ->
      Postgres.complete_activity_task!(
        Continuum.Runtime.Instance.default(),
        stale_task,
        {:ok, 10},
        lease_token
      )
    end

    task = Repo.one!(ActivityTask)
    assert task.state == "leased"
    assert task.attempt == 2
    assert ["activity_scheduled"] = event_types(run_id)
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

    assert_raise Continuum.Runtime.JournalError, ~r/lease_mismatch/, fn ->
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

  test "dispatch_once requeues a stranded leased task with an expired lease" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ResilientFlow, %{seed: 4}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    # Simulate a worker that died after claiming: the task is 'leased' with
    # an expired lease, which the claim queries alone can never pick up.
    assert {:ok, _claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "crashed-worker",
               -5
             )

    assert Repo.one!(ActivityTask).state == "leased"

    handler_id = "dispatcher-requeue-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :activity_dispatcher, :requeued],
        fn event, measurements, metadata, test_pid ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)

    assert_received {:telemetry, [:continuum, :activity_dispatcher, :requeued], %{count: 1}, _}

    assert {:ok, %{state: :completed, result: {:ok, 9}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    completed = Repo.one!(ActivityTask)
    assert completed.state == "completed"
    # The crashed execution consumed an attempt.
    assert completed.attempt == 2
  end

  test "a crash requeue past max_attempts fails the task without re-executing it" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(NilKeyFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    # NilKeyActivity allows a single attempt; the crashed execution was it.
    assert {:ok, _claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "crashed-worker",
               -5
             )

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "activity-test", batch_size: 1)

    assert {:error, %{state: :failed}} = Continuum.await(run_id, 1_000, journal: Postgres)

    assert Agent.get(NilKeyActivity, & &1) == 0
    assert event_types(run_id) == ["activity_scheduled", "activity_failed"]

    failed = Repo.one!(ActivityTask)
    assert failed.state == "discarded"
    assert decode_term(failed.error) == :attempts_exhausted
  end

  test "execute renews the task lease on a short horizon, not timeout + margin" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 6}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    # Claim with an already-expired TTL: without the execution-time lease
    # renewal, the completion write would reject with
    # :activity_task_lease_expired and the task would wedge.
    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "slow-worker",
               -5
             )

    assert :ok = Continuum.Runtime.ActivityWorker.execute(claimed)

    assert {:ok, %{state: :completed, result: {:ok, 13}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    task = Repo.one!(ActivityTask)
    assert task.state == "completed"

    # The lease horizon stays short (one heartbeat TTL, default 30s) — a
    # crashed worker's task is rescuable within ~TTL instead of waiting out
    # the activity timeout + margin.
    assert DateTime.compare(task.lease_expires_at, DateTime.utc_now()) == :gt

    assert DateTime.compare(
             task.lease_expires_at,
             DateTime.add(DateTime.utc_now(), 35, :second)
           ) == :lt
  end

  test "the heartbeat keeps a long-running activity's lease alive" do
    previous_ttl = Application.get_env(:continuum, :task_lease_ttl_seconds)
    previous_renew = Application.get_env(:continuum, :task_lease_renew_ms)
    Application.put_env(:continuum, :task_lease_ttl_seconds, 1)
    Application.put_env(:continuum, :task_lease_renew_ms, 100)

    on_exit(fn ->
      restore_env(:task_lease_ttl_seconds, previous_ttl)
      restore_env(:task_lease_renew_ms, previous_renew)
    end)

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(
        SlowActivityFlow,
        %{sleep_ms: 1_500},
        journal: Postgres
      )

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "heartbeat-worker",
               30
             )

    # The activity outlives the 1s lease TTL; without renewals the completion
    # write would reject with :activity_task_lease_expired.
    assert :ok = Continuum.Runtime.ActivityWorker.execute(claimed)

    assert {:ok, %{state: :completed, result: {:ok, :slept}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert Repo.one!(ActivityTask).state == "completed"
  end

  test "context activities persist bounded progress for health and Observer" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ProgressFlow, %{value: 21}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "progress-worker",
               30
             )

    context = Continuum.Activity.Context.from_task(claimed)

    assert_raise Continuum.DurableTermError, fn ->
      Continuum.Activity.Context.heartbeat(context, %{owner: self()})
    end

    assert_raise ArgumentError, ~r/exceed/, fn ->
      Continuum.Activity.Context.heartbeat(context, String.duplicate("x", 17_000))
    end

    assert :ok = Continuum.Runtime.ActivityWorker.execute(claimed)

    assert {:ok, %{state: :completed, result: {:ok, 42}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    persisted = Repo.one!(ActivityTask)
    assert %DateTime{} = persisted.last_heartbeat_at
    assert decode_term(persisted.heartbeat_details) == %{phase: "upload", percent: 50}

    assert {:ok, [%{heartbeat_details: %{phase: "upload", percent: 50}}]} =
             Continuum.Observer.list_activity_tasks(run_id)

    assert {:ok, health} = Continuum.Health.report()

    assert Enum.any?(health.activities.heartbeats, fn heartbeat ->
             heartbeat.task_id == task.id and heartbeat.details.percent == 50
           end)
  end

  test "context activities retain synchronous test parity" do
    {:ok, run_id} = Continuum.Test.start_synchronous(ProgressFlow, %{value: 4})

    assert {:ok, %{state: :completed, result: {:ok, 8}}} = Continuum.await(run_id)
  end

  test "context activities observe workflow cancellation cooperatively" do
    probe = Continuum.Test.ImpureProbe.register()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(CancelAwareFlow, %{probe: probe}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "cancel-aware-worker",
               30
             )

    worker = Task.async(fn -> Continuum.Runtime.ActivityWorker.execute(claimed) end)
    assert_receive {:cancel_aware_started, activity_pid}

    assert :ok = Continuum.cancel(run_id, journal: Postgres)
    send(activity_pid, :check_cancellation)

    assert_receive {:activity_cancelled, true}
    assert :ok = Task.await(worker)

    assert {:error, %{state: :cancelled}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "fenced-out completion releases the task and the run still makes progress" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{seed: 8}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "fenced-worker",
               30
             )

    # The engine dies and the run lease rotates while the activity is in
    # flight: the captured token is now stale.
    [{engine_pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run_id)
    engine_ref = Process.monitor(engine_pid)
    Process.exit(engine_pid, :kill)
    assert_receive {:DOWN, ^engine_ref, :process, ^engine_pid, :killed}

    Repo.update_all(from(r in Run, where: r.id == ^run_id), inc: [lease_token: 1])

    handler_id = "activity-fenced-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :activity, :fenced],
        fn event, measurements, metadata, test_pid ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    log =
      capture_log(fn ->
        assert :ok = Continuum.Runtime.ActivityWorker.execute(claimed)
      end)

    assert log =~ "fenced out"
    assert_received {:telemetry, [:continuum, :activity, :fenced], %{}, %{action: :requeued}}

    # Fencing held: nothing was journaled under the stale token...
    assert event_types(run_id) == ["activity_scheduled"]

    # ...and the task is claimable again instead of stranded in 'leased'.
    task = Repo.one!(ActivityTask)
    assert task.state == "available"
    assert is_nil(task.lease_owner)

    assert {:ok, reclaimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "second-worker",
               30
             )

    capture_log(fn ->
      assert :ok = Continuum.Runtime.ActivityWorker.execute(reclaimed)
    end)

    assert event_types(run_id) == ["activity_scheduled", "activity_completed"]
    assert Repo.one!(ActivityTask).state == "completed"
  end

  test "execute skips a task whose lease was taken over" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(NilKeyFlow, %{seed: 3}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "original-worker",
               30
             )

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [lease_owner: "usurper-worker"]
    )

    log =
      capture_log(fn ->
        assert :ok = Continuum.Runtime.ActivityWorker.execute(claimed)
      end)

    assert log =~ "lease no longer held"
    assert Agent.get(NilKeyActivity, & &1) == 0
    assert event_types(run_id) == ["activity_scheduled"]

    task = Repo.one!(ActivityTask)
    assert task.state == "leased"
    assert task.lease_owner == "usurper-worker"
  end

  test "task lease heartbeat retries after a transient renewal error" do
    {:ok, _agent} = ScriptedLeaseRepo.start_link([{:error, :closed}])
    previous = Application.get_env(:continuum, :task_lease_renew_ms)
    Application.put_env(:continuum, :task_lease_renew_ms, 50)
    on_exit(fn -> restore_env(:task_lease_renew_ms, previous) end)

    task = %{
      id: Ecto.UUID.generate(),
      run_id: Ecto.UUID.generate(),
      attempt: 1,
      lease_owner: "heartbeat-retry",
      instance: %{repo: ScriptedLeaseRepo}
    }

    log =
      capture_log(fn ->
        pid = Continuum.Runtime.ActivityWorker.start_task_lease_heartbeat(task)
        assert_eventually(fn -> ScriptedLeaseRepo.calls() >= 3 end, 100)
        send(pid, :stop)
      end)

    assert log =~ "will retry"
  end

  test "task lease heartbeat stops once renewal reports the lease lost" do
    {:ok, _agent} = ScriptedLeaseRepo.start_link([{:ok, %{num_rows: 0}}])
    previous = Application.get_env(:continuum, :task_lease_renew_ms)
    Application.put_env(:continuum, :task_lease_renew_ms, 50)
    on_exit(fn -> restore_env(:task_lease_renew_ms, previous) end)

    task = %{
      id: Ecto.UUID.generate(),
      run_id: Ecto.UUID.generate(),
      attempt: 1,
      lease_owner: "heartbeat-lost",
      instance: %{repo: ScriptedLeaseRepo}
    }

    pid = Continuum.Runtime.ActivityWorker.start_task_lease_heartbeat(task)
    assert_eventually(fn -> not Process.alive?(pid) end, 100)
    assert ScriptedLeaseRepo.calls() == 1
  end

  test "a stale worker cannot release a newer attempt owned by the same dispatcher" do
    probe = Continuum.Test.ImpureProbe.register()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(
        LeaseRaceFlow,
        %{test_pid: probe},
        journal: Postgres
      )

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    task = Repo.one!(ActivityTask)

    assert {:ok, claimed} =
             Dispatcher.claim_one(
               Continuum.Runtime.Instance.default(),
               task.id,
               task.attempt,
               "stable-dispatcher-owner",
               30
             )

    worker = Task.async(fn -> Continuum.Runtime.ActivityWorker.execute(claimed) end)
    assert_receive {:lease_race_activity_started, activity_pid}

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [attempt: task.attempt + 1]
    )

    Repo.update_all(from(r in Run, where: r.id == ^run_id), inc: [lease_token: 1])
    send(activity_pid, :finish_lease_race_activity)

    capture_log(fn -> assert :ok = Task.await(worker) end)

    newer_claim = Repo.one!(ActivityTask)
    assert newer_claim.state == "leased"
    assert newer_claim.lease_owner == "stable-dispatcher-owner"
    assert newer_claim.attempt == task.attempt + 1
    assert event_types(run_id) == ["activity_scheduled"]
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

  defp restore_env(key, nil), do: Application.delete_env(:continuum, key)
  defp restore_env(key, value), do: Application.put_env(:continuum, key, value)

  defp future_time do
    DateTime.utc_now()
    |> DateTime.add(60, :second)
    |> DateTime.truncate(:microsecond)
  end

  defp assert_wake_pending(run_id) do
    next_wakeup_at =
      Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.next_wakeup_at))

    assert %DateTime{} = next_wakeup_at
    assert DateTime.compare(next_wakeup_at, db_now()) in [:lt, :eq]
  end

  defp db_now do
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()")
    now
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
