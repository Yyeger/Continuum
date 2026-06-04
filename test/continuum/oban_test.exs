defmodule Continuum.ObanTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{ActivityWorker.Dispatcher, Instance, Journal.Postgres}
  alias Continuum.Schema.{ActivityResult, ActivityTask, Event, Run}

  defmodule DoubleActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(n), do: {:ok, n * 2}
    def idempotency_key([n]), do: "double:#{n}"
  end

  defmodule DoubleFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, value} = activity(DoubleActivity.run(input.seed))
      {:ok, value + 1}
    end
  end

  defmodule FlakyActivity do
    use Continuum.Activity, retry: [max_attempts: 2, backoff: :constant, base_ms: 1]

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

  defmodule SlowActivity do
    use Continuum.Activity, retry: [max_attempts: 1], timeout: 1

    def run(pid) do
      send(pid, :slow_started)
      Process.sleep(25)
      {:ok, :too_late}
    end
  end

  defmodule TimeoutFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(SlowActivity.run(input.pid))
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

  defmodule SagaActivities do
    use Continuum.Activity, retry: [max_attempts: 1]

    def reserve(pid), do: send(pid, :reserved) && {:ok, :reservation}
    def fail(_pid), do: raise("boom")
    def release(pid), do: send(pid, :released) && {:ok, :released}
  end

  defmodule SagaFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, _reservation} =
        activity(SagaActivities.reserve(input.pid),
          compensate: {SagaActivities, :release, [input.pid]}
        )

      case activity(SagaActivities.fail(input.pid), compensate: :none) do
        {:error, _reason} ->
          compensate_all()
          {:ok, :compensated}

        {:ok, value} ->
          {:ok, value}
      end
    end
  end

  defmodule BlockingActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(pid) do
      send(pid, {:blocking_started, self()})

      receive do
        :finish -> {:ok, :done}
      after
        1_000 -> {:error, :blocked}
      end
    end
  end

  defmodule BlockingFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(BlockingActivity.run(input.pid))
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

    :ok
  end

  test "enqueue inserts a one-shot Oban job with stable task identifiers" do
    oban_name = unique_name("oban")

    start_supervised!(
      {Oban, name: oban_name, repo: Repo, queues: false, plugins: false, testing: :manual}
    )

    instance_name = unique_name("continuum")

    instance =
      Instance.new(
        name: instance_name,
        repo: Repo,
        activity_executor: {:oban, name: oban_name, queue: :continuum_activities}
      )

    task_id = Ecto.UUID.generate()

    assert {:ok, job} = Continuum.Oban.enqueue(instance, %{id: task_id, attempt: 3})

    assert job.worker == "Continuum.Oban.Worker"
    assert job.queue == "continuum_activities"
    assert job.max_attempts == 1
    assert arg(job.args, "task_id") == task_id
    assert arg(job.args, "attempt") == 3

    assert job.args |> arg("instance") |> Continuum.Oban.decode_instance() == instance_name
  end

  test "activity completes through Oban worker and replays like builtin execution" do
    instance = start_oban_continuum!()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DoubleFlow, %{seed: 5},
        journal: Postgres,
        instance: instance.name
      )

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)

    assert Repo.one!(ActivityTask).state == "available"
    assert :ok = perform_next_oban_job()

    assert {:ok, %{state: :completed, result: {:ok, 11}}} =
             Continuum.await(run_id, 1_000, journal: Postgres, instance: instance.name)

    assert event_types(run_id) == ["activity_scheduled", "activity_completed"]
  end

  test "failed Oban activity retries through Continuum, not Oban" do
    instance = start_oban_continuum!()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(RetryFlow, %{seed: 9},
        journal: Postgres,
        instance: instance.name
      )

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)
    assert :ok = perform_next_oban_job()

    assert_eventually(fn -> Repo.one!(ActivityTask).attempt == 2 end)
    assert Repo.one!(ActivityTask).state == "available"
    assert event_types(run_id) == ["activity_scheduled"]

    make_task_due()

    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)
    assert :ok = perform_next_oban_job()

    assert {:ok, %{state: :completed, result: {:ok, 9}}} =
             Continuum.await(run_id, 1_000, journal: Postgres, instance: instance.name)

    assert Agent.get(FlakyActivity, & &1) == 2
  end

  test "Oban worker preserves Continuum timeout semantics" do
    instance = start_oban_continuum!()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimeoutFlow, %{pid: self()},
        journal: Postgres,
        instance: instance.name
      )

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)
    assert :ok = perform_next_oban_job()
    assert_received :slow_started

    assert {:ok, %{state: :completed, result: {:error, :timeout}}} =
             Continuum.await(run_id, 1_000, journal: Postgres, instance: instance.name)

    assert ["activity_scheduled", "activity_failed"] = event_types(run_id)
  end

  test "queued Oban job claims at perform time after queue delay" do
    instance = start_oban_continuum!()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DoubleFlow, %{seed: 7},
        journal: Postgres,
        instance: instance.name
      )

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)

    Process.sleep(35)
    assert Repo.one!(ActivityTask).state == "available"
    assert :ok = perform_next_oban_job()

    assert {:ok, %{state: :completed, result: {:ok, 15}}} =
             Continuum.await(run_id, 1_000, journal: Postgres, instance: instance.name)
  end

  test "duplicate Oban job no-ops after the task is completed" do
    instance = start_oban_continuum!()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DoubleFlow, %{seed: 8},
        journal: Postgres,
        instance: instance.name
      )

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)

    job = next_oban_job()
    assert :ok = Continuum.Oban.Worker.perform(job)
    mark_job_completed(job)
    assert :ok = Continuum.Oban.Worker.perform(job)

    assert {:ok, %{state: :completed, result: {:ok, 17}}} =
             Continuum.await(run_id, 1_000, journal: Postgres, instance: instance.name)

    assert event_count(run_id, "activity_completed") == 1
  end

  test "Oban worker completion is fenced by the captured run lease token" do
    instance = start_oban_continuum!()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(BlockingFlow, %{pid: self()},
        journal: Postgres,
        instance: instance.name
      )

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)

    job = next_oban_job()

    task =
      Task.async(fn ->
        try do
          {:ok, Continuum.Oban.Worker.perform(job)}
        rescue
          error -> {:error, error}
        end
      end)

    assert_receive {:blocking_started, activity_pid}

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      inc: [lease_token: 1]
    )

    send(activity_pid, :finish)

    assert {:error, %RuntimeError{message: message}} = Task.await(task, 1_000)
    assert message =~ "lease_mismatch"

    assert event_types(run_id) == ["activity_scheduled"]
  end

  test "idempotency hit via Oban skips the MFA" do
    instance = start_oban_continuum!()
    committed_result = {:ok, {:cached, 5}}

    Repo.insert!(%ActivityResult{
      activity_module: Atom.to_string(IdempotentActivity),
      idempotency_key: "idempotent:5",
      run_id: Ecto.UUID.generate(),
      seq: 1,
      result: :erlang.term_to_binary(committed_result),
      completed_at: DateTime.utc_now()
    })

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(IdempotentFlow, %{seed: 5},
        journal: Postgres,
        instance: instance.name
      )

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)
    assert :ok = perform_next_oban_job()

    assert {:ok, %{state: :completed, result: ^committed_result}} =
             Continuum.await(run_id, 1_000, journal: Postgres, instance: instance.name)

    assert Agent.get(IdempotentActivity, & &1) == 0
  end

  test "compensation runs through Oban worker" do
    instance = start_oban_continuum!()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SagaFlow, %{pid: self()},
        journal: Postgres,
        instance: instance.name
      )

    assert_eventually(fn -> event_count(run_id, "activity_scheduled") == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)
    assert :ok = perform_next_oban_job()
    assert_receive :reserved

    assert_eventually(fn -> event_count(run_id, "activity_scheduled") == 2 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)
    assert :ok = perform_next_oban_job()

    assert_eventually(fn -> event_count(run_id, "compensation_scheduled") == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(instance: instance.name, batch_size: 1)
    assert :ok = perform_next_oban_job()
    assert_receive :released

    assert {:ok, %{state: :completed, result: {:ok, :compensated}}} =
             Continuum.await(run_id, 1_000, journal: Postgres, instance: instance.name)

    assert "compensation_completed" in event_types(run_id)
  end

  defp arg(args, key) do
    Map.get(args, key, Map.get(args, String.to_atom(key)))
  end

  defp start_oban_continuum! do
    oban_name = unique_name("oban")

    start_supervised!(
      {Oban, name: oban_name, repo: Repo, queues: false, plugins: false, testing: :manual}
    )

    instance_name = unique_name("continuum")

    {:ok, _supervisor} =
      Continuum.children(
        name: instance_name,
        repo: Repo,
        activity_executor: {:oban, name: oban_name, queue: :continuum_activities},
        heartbeater: false,
        dispatcher: false,
        activity_dispatcher: false,
        recovery: false,
        snapshotter: false,
        timer_wheel: false,
        signal_router: false
      )
      |> Supervisor.start_link(strategy: :one_for_one)

    Instance.lookup(instance_name)
  end

  defp perform_next_oban_job do
    job = next_oban_job()

    result = Continuum.Oban.Worker.perform(job)
    mark_job_completed(job)
    result
  end

  defp next_oban_job do
    Repo.one!(
      from(j in Oban.Job,
        where: j.worker == "Continuum.Oban.Worker" and j.state == "available",
        order_by: [asc: j.id],
        limit: 1
      )
    )
  end

  defp mark_job_completed(job) do
    now = DateTime.utc_now()

    Repo.update_all(
      from(j in Oban.Job, where: j.id == ^job.id),
      set: [state: "completed", completed_at: now]
    )
  end

  defp make_task_due do
    Repo.update_all(
      ActivityTask,
      set: [available_at: DateTime.add(DateTime.utc_now(), -1, :second)]
    )
  end

  defp assert_eventually(fun, attempts \\ 30)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp event_types(run_id) do
    Repo.all(
      from(e in Event,
        where: e.run_id == ^run_id,
        order_by: [asc: e.seq],
        select: e.event_type
      )
    )
  end

  defp event_count(run_id, type) do
    Repo.aggregate(from(e in Event, where: e.run_id == ^run_id and e.event_type == ^type), :count)
  end

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end
end
