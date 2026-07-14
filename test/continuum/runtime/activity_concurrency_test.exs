defmodule Continuum.Runtime.ActivityConcurrencyTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.ActivityWorker.Dispatcher
  alias Continuum.Runtime.{Instance, Lease}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}

  defmodule Workflow do
    def __continuum_workflow__ do
      %{
        module: __MODULE__,
        entrypoint: __MODULE__,
        version: 1,
        version_hash: :crypto.hash(:sha256, "activity-concurrency")
      }
    end
  end

  defmodule BlockingActivity do
    def run(probe) do
      Continuum.Test.ImpureProbe.notify_with_self(probe, :activity_started)

      receive do
        :release -> {:ok, :done}
      end
    end
  end

  setup do
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)

    name = String.to_atom("activity_capacity_#{System.unique_integer([:positive])}")

    children =
      Continuum.children(
        name: name,
        repo: Repo,
        activity_max_concurrency: 1,
        heartbeater: false,
        recovery: false,
        dispatcher: false,
        activity_dispatcher: false,
        snapshotter: false,
        timer_wheel: false,
        signal_router: false,
        version_registry: false
      )

    {:ok, _supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

    {:ok, instance: Instance.lookup(name)}
  end

  test "claims no more work than available capacity and reports saturation", %{instance: instance} do
    handler_id = "activity-capacity-#{System.unique_integer()}"
    parent = self()

    :telemetry.attach_many(
      handler_id,
      [
        [:continuum, :activity_dispatcher, :polled],
        [:continuum, :activity_dispatcher, :saturated],
        [:continuum, :activity_dispatcher, :claim_rejected]
      ],
      fn event, measurements, metadata, pid ->
        send(pid, {:telemetry, event, measurements, metadata})
      end,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    probe = Continuum.Test.ImpureProbe.register()
    Enum.each(1..3, &schedule_blocking_task(instance, &1, probe))

    two_seconds_ago = DateTime.add(DateTime.utc_now(), -2, :second)
    Repo.update_all(ActivityTask, set: [scheduled_at: two_seconds_ago])

    assert {:ok, 1} =
             Dispatcher.dispatch_once(instance: instance.name, owner: "capacity", batch_size: 10)

    assert_receive {:activity_started, activity_pid}, 1_000
    assert DynamicSupervisor.count_children(instance.activity_supervisor).active == 1
    assert Repo.aggregate(from(t in ActivityTask, where: t.state == "leased"), :count) == 1
    assert Repo.aggregate(from(t in ActivityTask, where: t.state == "available"), :count) == 2

    assert {:ok, 0} =
             Dispatcher.dispatch_once(instance: instance.name, owner: "capacity", batch_size: 10)

    assert_receive {:telemetry, [:continuum, :activity_dispatcher, :saturated], measurements,
                    metadata},
                   1_000

    assert measurements.active == 1
    assert measurements.pending == 2
    assert measurements.oldest_queue_age_ms >= 1_000
    assert metadata.max_concurrency == 1
    assert metadata.reason == :capacity

    assert_receive {:telemetry, [:continuum, :activity_dispatcher, :claim_rejected], %{count: 2},
                    %{reason: :capacity}},
                   1_000

    send(activity_pid, :release)
  end

  test "saturated poll delays include bounded jitter" do
    delays = Enum.map(1..50, fn _ -> Dispatcher.next_poll_delay(1_000, 250, true) end)

    assert Enum.all?(delays, &(&1 in 1_000..1_250))
    assert Dispatcher.next_poll_delay(1_000, 250, false) == 1_000
    assert Dispatcher.next_poll_delay(1_000, 0, true) == 1_000
  end

  defp schedule_blocking_task(instance, index, probe) do
    run_id = Ecto.UUID.generate()
    task_id = Ecto.UUID.generate()
    owner = "capacity-run-#{index}"

    :ok = Postgres.start_run(instance, run_id, Workflow, %{index: index})
    {:ok, lease} = Lease.acquire(run_id, owner: owner, repo: Repo)

    event = %{
      type: :activity_scheduled,
      task_id: task_id,
      mfa: {BlockingActivity, :run, [probe]},
      opts: [],
      command_id: {:capacity, index},
      seq: 0
    }

    task = %{
      id: task_id,
      seq: 0,
      mfa: {BlockingActivity, :run, [probe]},
      opts: [],
      retry: [max_attempts: 1],
      timeout_ms: 5_000,
      idempotency_key: nil,
      command_id: {:capacity, index}
    }

    :ok = Postgres.schedule_activity!(instance, run_id, event, task, lease.token)
  end
end
