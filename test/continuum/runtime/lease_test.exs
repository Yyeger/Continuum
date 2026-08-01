defmodule Continuum.Runtime.LeaseTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease
  alias Continuum.Schema.{ActivityTask, Event, Run, Signal, Timer}

  defmodule SomeWorkflow do
    @moduledoc false

    def __continuum_workflow__ do
      %{
        module: __MODULE__,
        entrypoint: __MODULE__,
        version: 1,
        version_hash: :crypto.hash(:sha256, "lease-test")
      }
    end
  end

  defmodule FencedActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(value), do: {:ok, value}
  end

  defmodule FencedActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(FencedActivity.run(input.value))
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

  describe "acquire/2" do
    test "claims an unleased run and returns a fencing token" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      assert {:ok, %Lease{owner: "node-a", token: token}} =
               Lease.acquire(run_id, owner: "node-a")

      run = Repo.one!(from(r in Run, where: r.id == ^run_id))

      assert is_integer(token)
      assert run.lease_owner == "node-a"
      assert run.lease_token == token
      assert %DateTime{} = run.lease_acquired_at
      assert %DateTime{} = run.lease_heartbeat_at
      assert run.lease_expires_at != nil
    end

    test "returns a pending cancel request when claiming a run" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      Repo.update_all(from(r in Run, where: r.id == ^run_id), set: [cancel_requested_at: now])

      assert {:ok, %Lease{cancel_requested_at: cancel_requested_at}} =
               Lease.acquire(run_id, owner: "node-a")

      assert cancel_requested_at == DateTime.to_naive(now)
    end

    test "does not claim a run with an active lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{}} = Lease.acquire(run_id, owner: "node-a")

      assert {:error, :not_acquired} = Lease.acquire(run_id, owner: "node-b")
    end

    test "claims a run whose lease expired and increments the fencing token" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{token: token_a}} = Lease.acquire(run_id, owner: "node-a")

      expire_lease(run_id)

      assert {:ok, %Lease{owner: "node-b", token: token_b}} =
               Lease.acquire(run_id, owner: "node-b")

      assert token_b > token_a
    end
  end

  describe "renew/4" do
    test "renews only when owner and token still match" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{owner: owner, token: token}} = Lease.acquire(run_id, owner: "node-a")

      past = DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)
      Repo.update_all(from(r in Run, where: r.id == ^run_id), set: [lease_heartbeat_at: past])

      assert :ok = Lease.renew(run_id, owner, token)
      assert DateTime.compare(Repo.get!(Run, run_id).lease_heartbeat_at, past) == :gt
      assert {:error, :lost} = Lease.renew(run_id, "node-b", token)
      assert {:error, :lost} = Lease.renew(run_id, owner, token + 1)
    end

    test "reports lost after another owner steals an expired lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{owner: owner, token: token}} = Lease.acquire(run_id, owner: "node-a")

      expire_lease(run_id)
      assert {:ok, %Lease{owner: "node-b"}} = Lease.acquire(run_id, owner: "node-b")

      assert {:error, :lost} = Lease.renew(run_id, owner, token)
    end
  end

  describe "release/4" do
    test "atomically releases only the matching fenced lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{owner: owner, token: token}} = Lease.acquire(run_id, owner: "node-a")

      assert {:error, :lost} = Lease.release(run_id, "node-b", token)
      assert {:error, :lost} = Lease.release(run_id, owner, token + 1)
      assert :ok = Lease.release(run_id, owner, token)

      run = Repo.one!(from(r in Run, where: r.id == ^run_id))
      assert run.lease_owner == nil
      assert run.lease_token == nil
      assert run.lease_expires_at == nil

      assert {:ok, %Lease{owner: "node-b", token: next_token}} =
               Lease.acquire(run_id, owner: "node-b")

      assert next_token > token
    end
  end

  describe "journal fencing" do
    test "rejects stale journal writes after another owner steals the lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{token: stale_token}} = Lease.acquire(run_id, owner: "node-a")

      expire_lease(run_id)

      assert {:ok, %Lease{token: current_token}} = Lease.acquire(run_id, owner: "node-b")
      assert current_token > stale_token

      assert_raise Continuum.Runtime.JournalError, ~r/lease_mismatch/, fn ->
        Postgres.append!(
          Continuum.Runtime.Instance.default(),
          run_id,
          %{type: :side_effect, kind: :user, payload: :stale_write, seq: nil},
          stale_token
        )
      end

      assert Postgres.load(Continuum.Runtime.Instance.default(), run_id) == []
    end

    test "rejects stale cancel_run! after another owner steals the lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{token: stale_token}} = Lease.acquire(run_id, owner: "node-a")

      expire_lease(run_id)

      assert {:ok, %Lease{token: current_token}} = Lease.acquire(run_id, owner: "node-b")
      assert current_token > stale_token

      assert_raise Continuum.Runtime.JournalError, ~r/lease_mismatch/, fn ->
        Postgres.cancel_run!(Continuum.Runtime.Instance.default(), run_id, stale_token)
      end

      run = Repo.one!(from(r in Run, where: r.id == ^run_id))
      assert run.state == "running"
      assert run.lease_token == current_token
    end

    test "rejects stale complete_activity_task! after another owner steals the run lease" do
      {:ok, run_id} =
        Continuum.Runtime.Engine.start_run(FencedActivityFlow, %{value: 10}, journal: Postgres)

      assert_eventually(fn ->
        Repo.aggregate(ActivityTask, :count) == 1
      end)

      task = Repo.one!(ActivityTask)
      run = Repo.one!(from(r in Run, where: r.id == ^run_id))

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

      expire_lease(run_id)

      assert {:ok, %Lease{token: current_token}} = Lease.acquire(run_id, owner: "node-b")
      assert current_token > run.lease_token

      assert_raise Continuum.Runtime.JournalError, ~r/lease_mismatch/, fn ->
        Postgres.complete_activity_task!(
          Continuum.Runtime.Instance.default(),
          claimed_task,
          {:ok, 10},
          run.lease_token
        )
      end

      assert ["activity_scheduled"] = event_types(run_id)
      assert Repo.one!(ActivityTask).state == "leased"
    end
  end

  defp expire_lease(run_id) do
    expired_at =
      DateTime.utc_now()
      |> DateTime.add(-60, :second)
      |> DateTime.truncate(:microsecond)

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [lease_expires_at: expired_at]
    )
  end

  defp future_time do
    DateTime.utc_now()
    |> DateTime.add(60, :second)
    |> DateTime.truncate(:microsecond)
  end

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

defmodule Continuum.Runtime.LeaseHeartbeaterTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.{Instance, Lease}
  alias Continuum.Runtime.Lease.Heartbeater
  alias Continuum.Schema.Run

  defmodule SuspendedPgFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      await(signal(:continue))
    end
  end

  defmodule BlockingPgFlow do
    def __continuum_workflow__ do
      %{
        module: __MODULE__,
        entrypoint: __MODULE__,
        version: 1,
        version_hash: :crypto.hash(:sha256, "blocking-lease-drain-test")
      }
    end

    def run(%{test_pid: probe}) do
      Continuum.Test.ImpureProbe.notify_with_self(probe, :blocking_flow_started)
      Process.sleep(:infinity)
    end
  end

  defmodule CompletingPgFlow do
    def __continuum_workflow__ do
      %{
        module: __MODULE__,
        entrypoint: __MODULE__,
        version: 1,
        version_hash: :crypto.hash(:sha256, "completing-lease-drain-test")
      }
    end

    def run(%{test_pid: probe}) do
      Continuum.Test.ImpureProbe.notify_with_self(probe, :completing_flow_started)

      receive do
        :finish -> :done
      end
    end
  end

  defmodule SlowReleaseRepo do
    def query(_sql, _params) do
      Process.sleep(5_000)
      {:ok, %{num_rows: 1}}
    end
  end

  test "supervisor shutdown drains engines before the heartbeater exits" do
    {instance, supervisor} = start_runtime!(drain_timeout_ms: 500)

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SuspendedPgFlow, %{},
        instance: instance,
        journal: Postgres
      )

    assert_eventually(fn -> Repo.get!(Run, run_id).state == "suspended" end)
    [{engine_pid, _}] = Registry.lookup(instance.registry, run_id)
    ref = Process.monitor(engine_pid)

    assert :ok = Supervisor.stop(supervisor, :shutdown, 2_000)
    assert_receive {:DOWN, ^ref, :process, ^engine_pid, :normal}, 1_000

    run = Repo.get!(Run, run_id)
    assert run.state == "suspended"
    assert run.lease_owner == nil
    assert run.lease_token == nil
    assert run.lease_expires_at == nil

    assert {:ok, %Lease{owner: "replacement"}} =
             Lease.acquire(run_id, owner: "replacement", repo: Repo)
  end

  test "drain forcibly fences a workflow step that exceeds the deadline" do
    {instance, supervisor} = start_runtime!(drain_timeout_ms: 20)
    on_exit(fn -> if Process.alive?(supervisor), do: Supervisor.stop(supervisor) end)
    probe = Continuum.Test.ImpureProbe.register()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(BlockingPgFlow, %{test_pid: probe},
        instance: instance,
        journal: Postgres
      )

    assert_receive {:blocking_flow_started, engine_pid}, 1_000
    ref = Process.monitor(engine_pid)

    assert {:ok,
            %{
              tracked_count: 1,
              graceful_count: 0,
              forced_count: 1,
              unreleased_count: 0
            }} = Heartbeater.drain(instance, 20)

    assert_receive {:DOWN, ^ref, :process, ^engine_pid, :killed}, 1_000

    run = Repo.get!(Run, run_id)
    assert run.state == "running"
    assert run.lease_owner == nil
    assert run.lease_token == nil
    assert :sys.get_state(instance.dispatcher).enabled? == false
  end

  test "public readiness flips before claims pause and drain is idempotent per instance" do
    {instance, supervisor} = start_runtime!(drain_timeout_ms: 500)
    on_exit(fn -> if Process.alive?(supervisor), do: Supervisor.stop(supervisor) end)

    assert %{
             state: :ready,
             ready?: true,
             drained?: false,
             active_run_count: 0,
             pending_claim_count: 0
           } = Continuum.readiness(instance: instance.name)

    assert %Continuum.Readiness{} = Continuum.readiness(instance: instance.name)

    assert Continuum.ready?(instance: instance.name)
    refute Continuum.drained?(instance: instance.name)

    :ok = :sys.suspend(instance.dispatcher)

    drain_task =
      Task.async(fn -> Continuum.drain(instance: instance.name, timeout: 500) end)

    assert_eventually(fn ->
      Continuum.readiness(instance: instance.name).state == :draining
    end)

    refute Continuum.ready?(instance: instance.name)

    run_count = Repo.aggregate(Run, :count)

    assert {:error, :not_ready} =
             Continuum.start(SuspendedPgFlow, %{}, instance: instance.name, journal: Postgres)

    assert Repo.aggregate(Run, :count) == run_count

    repeated_drain_task =
      Task.async(fn -> Continuum.drain(instance: instance.name, timeout: 500) end)

    :ok = :sys.resume(instance.dispatcher)

    assert {:ok, summary} = Task.await(drain_task, 2_000)
    assert {:ok, ^summary} = Task.await(repeated_drain_task, 2_000)
    assert summary.tracked_count == 0
    assert summary.unreleased_count == 0
    assert summary.abandoned_claim_count == 0

    assert Continuum.drained?(instance: instance.name)
    refute Continuum.ready?(instance: instance.name)

    assert %{
             state: :drained,
             ready?: false,
             drained?: true,
             last_drain: ^summary
           } = Continuum.readiness(instance: instance.name)

    assert {:ok, ^summary} = Continuum.drain(instance: instance.name)
  end

  test "drain completes promptly when an in-flight workflow finishes normally" do
    {instance, supervisor} = start_runtime!(drain_timeout_ms: 1_000)
    on_exit(fn -> if Process.alive?(supervisor), do: Supervisor.stop(supervisor) end)
    probe = Continuum.Test.ImpureProbe.register()

    {:ok, run_id} =
      Continuum.start(CompletingPgFlow, %{test_pid: probe},
        instance: instance.name,
        journal: Postgres
      )

    assert_receive {:completing_flow_started, engine_pid}, 1_000

    drain_task =
      Task.async(fn -> Continuum.drain(instance: instance.name, timeout: 1_000) end)

    assert_eventually(fn -> Continuum.readiness(instance: instance.name).state == :draining end)
    send(engine_pid, :finish)

    assert {:ok, %{tracked_count: 1, graceful_count: 1, forced_count: 0}} =
             Task.await(drain_task, 500)

    assert Repo.get!(Run, run_id).state == "completed"
    assert Continuum.drained?(instance: instance.name)
  end

  test "readiness reports a runtime that has not been started" do
    name = :unstarted_readiness_test
    Instance.new(name: name, repo: Repo) |> Instance.register()

    assert %{state: :not_started, ready?: false, drained?: false} =
             Continuum.readiness(instance: name)

    refute Continuum.ready?(instance: name)
    refute Continuum.drained?(instance: name)
    assert {:error, :runtime_not_started} = Continuum.drain(instance: name)
    assert {:error, {:invalid_timeout, -1}} = Continuum.drain(instance: name, timeout: -1)
  end

  test "slow lease release cannot extend the public drain hard deadline" do
    name = :slow_release_drain_test

    instance =
      Instance.new(name: name, repo: SlowReleaseRepo, drain_timeout_ms: 20)
      |> Instance.register()

    start_supervised!({Heartbeater, instance: instance, drain_timeout_ms: 20})

    engine = spawn(fn -> ignore_messages() end)

    assert :ok =
             Heartbeater.track(
               instance,
               %Lease{run_id: Ecto.UUID.generate(), owner: "slow-owner", token: 1},
               engine
             )

    started_at = System.monotonic_time(:millisecond)
    assert {:ok, summary} = Continuum.drain(instance: name, timeout: 20)
    elapsed_ms = System.monotonic_time(:millisecond) - started_at

    assert elapsed_ms < 1_500
    assert summary.forced_count == 1
    assert summary.unreleased_count == 1
    assert Continuum.readiness(instance: name).state == :degraded
    refute Continuum.drained?(instance: name)
  end

  test "named in-memory engines participate in drain lifecycle" do
    {instance, supervisor} = start_in_memory_runtime!()
    on_exit(fn -> if Process.alive?(supervisor), do: Supervisor.stop(supervisor) end)
    probe = Continuum.Test.ImpureProbe.register()

    assert {:ok, _run_id} =
             Continuum.start(BlockingPgFlow, %{test_pid: probe},
               instance: instance.name,
               journal: Continuum.Runtime.Journal.InMemory
             )

    assert_receive {:blocking_flow_started, engine_pid}, 1_000
    monitor = Process.monitor(engine_pid)

    assert {:ok, %{tracked_count: 1, forced_count: 1, unreleased_count: 0}} =
             Continuum.drain(instance: instance.name, timeout: 20)

    assert_receive {:DOWN, ^monitor, :process, ^engine_pid, :killed}, 1_000
    assert Continuum.drained?(instance: instance.name)

    assert {:error, :not_ready} =
             Continuum.start(SuspendedPgFlow, %{},
               instance: instance.name,
               journal: Continuum.Runtime.Journal.InMemory
             )
  end

  test "engine stops itself when the heartbeater detects a stolen lease" do
    test_pid = self()

    handler_id =
      :telemetry.attach(
        "lease-heartbeater-lost-test",
        [:continuum, :lease, :lost],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SuspendedPgFlow, %{}, journal: Postgres)

    [{pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run_id)
    ref = Process.monitor(pid)

    original = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert is_integer(original.lease_token)

    expire_lease(run_id)

    assert {:ok, %Lease{owner: "node-b", token: stolen_token}} =
             Lease.acquire(run_id, owner: "node-b")

    assert stolen_token > original.lease_token

    assert :ok = Heartbeater.renew_once(Continuum.Runtime.Instance.default())

    assert_receive {:telemetry, [:continuum, :lease, :lost], %{},
                    %{run_id: ^run_id, owner: _, lease_token: _}},
                   1_000

    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000
    assert_eventually(fn -> Registry.lookup(Continuum.Runtime.Registry, run_id) == [] end)
  end

  defp expire_lease(run_id) do
    expired_at =
      DateTime.utc_now()
      |> DateTime.add(-60, :second)
      |> DateTime.truncate(:microsecond)

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [lease_expires_at: expired_at]
    )
  end

  defp start_runtime!(opts) do
    name = :"lease_drain_test_#{System.unique_integer([:positive])}"

    children =
      Continuum.children(
        name: name,
        repo: Repo,
        heartbeater: opts,
        activity_supervisor: false,
        recovery: false,
        dispatcher: [enabled?: false],
        activity_dispatcher: false,
        snapshotter: false,
        timer_wheel: false,
        signal_router: false,
        version_registry: false
      )

    {:ok, supervisor} =
      Supervisor.start_link(children,
        strategy: :one_for_one,
        name: :"lease_drain_supervisor_#{System.unique_integer([:positive])}"
      )

    Process.unlink(supervisor)
    {Instance.lookup(name), supervisor}
  end

  defp start_in_memory_runtime! do
    name = :"memory_drain_test_#{System.unique_integer([:positive])}"

    children =
      Continuum.children(
        name: name,
        journal: Continuum.Runtime.Journal.InMemory,
        heartbeater: [drain_timeout_ms: 20],
        activity_supervisor: false,
        recovery: false,
        dispatcher: false,
        activity_dispatcher: false,
        snapshotter: false,
        timer_wheel: false,
        signal_router: false,
        version_registry: false
      )

    {:ok, supervisor} =
      Supervisor.start_link(children,
        strategy: :one_for_one,
        name: :"memory_drain_supervisor_#{System.unique_integer([:positive])}"
      )

    Process.unlink(supervisor)
    {Instance.lookup(name), supervisor}
  end

  defp ignore_messages do
    receive do
      _message -> ignore_messages()
    end
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
