defmodule Continuum.Runtime.DispatcherTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Dispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease
  alias Continuum.Schema.Event
  alias Continuum.Schema.Run

  defmodule DispatchFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      value = Continuum.side_effect(fn -> input.seed * 3 end)
      {:ok, value}
    end
  end

  defmodule RunningCrashFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      value =
        Continuum.side_effect(fn ->
          send(input.test_pid, {:producer_started, self()})

          receive do
            :continue -> input.seed * 5
          end
        end)

      {:ok, value}
    end
  end

  setup do
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "dispatch_once leases an unowned run and starts an engine" do
    run_id = Ecto.UUID.generate()
    trace_context = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"

    :ok =
      Postgres.start_run(
        Continuum.Runtime.Instance.default(),
        run_id,
        DispatchFlow,
        %{seed: 7},
        trace_context: trace_context
      )

    :ok = Postgres.suspend!(Continuum.Runtime.Instance.default(), run_id, nil)

    handler_id = "dispatcher-trace-context-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :run, :started],
        fn event, measurements, metadata, test_pid ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "dispatcher-test", batch_size: 1)

    assert_receive {:telemetry, [:continuum, :run, :started], %{},
                    %{resumed?: true, trace_context: ^trace_context}},
                   1_000

    assert {:ok, %{state: :completed, result: {:ok, 21}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    run = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert run.lease_owner == "dispatcher-test"
    assert is_integer(run.lease_token)
  end

  test "dispatch_once skips rows scheduled for the future" do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, DispatchFlow, %{seed: 7})

    :ok = Postgres.suspend!(Continuum.Runtime.Instance.default(), run_id, nil)

    future =
      DateTime.utc_now()
      |> DateTime.add(60, :second)
      |> DateTime.truncate(:microsecond)

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [next_wakeup_at: future]
    )

    assert {:ok, 0} = Dispatcher.dispatch_once(owner: "dispatcher-test", batch_size: 1)
    assert {:error, :timeout} = Continuum.await(run_id, 25, journal: Postgres)
  end

  test "dispatch_once steals an expired lease and resumes the run" do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, DispatchFlow, %{seed: 4})

    assert {:ok, %Lease{token: stale_token}} = Lease.acquire(run_id, owner: "old-owner")
    :ok = Postgres.suspend!(Continuum.Runtime.Instance.default(), run_id, stale_token)

    expire_lease(run_id)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "dispatcher-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 12}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    run = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert run.lease_owner == "dispatcher-test"
    assert run.lease_token > stale_token
  end

  test "dispatch_once resumes an expired running row after the engine dies" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(RunningCrashFlow, %{seed: 8, test_pid: self()},
        journal: Postgres
      )

    assert_receive {:producer_started, first_engine_pid}, 1_000
    [{^first_engine_pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run_id)

    first_run = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert first_run.state == "running"
    assert is_integer(first_run.lease_token)

    ref = Process.monitor(first_engine_pid)
    Process.exit(first_engine_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^first_engine_pid, :killed}, 1_000
    assert_eventually(fn -> Registry.lookup(Continuum.Runtime.Registry, run_id) == [] end)

    still_running = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert still_running.state == "running"
    assert still_running.lease_token == first_run.lease_token

    expire_lease(run_id)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "dispatcher-test", batch_size: 1)
    assert_receive {:producer_started, resumed_engine_pid}, 1_000
    assert resumed_engine_pid != first_engine_pid

    send(resumed_engine_pid, :continue)

    assert {:ok, %{state: :completed, result: {:ok, 40}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    resumed_run = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert resumed_run.lease_owner == "dispatcher-test"
    assert resumed_run.lease_token > first_run.lease_token
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
