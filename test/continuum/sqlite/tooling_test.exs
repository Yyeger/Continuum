defmodule Continuum.SQLite.ToolingTest do
  @moduledoc """
  The test helpers, queries, ingress, operator actions, retention, health,
  and replay diagnostics on a SQLite instance whose pollers are driven by
  `Continuum.Test` rather than running in the background.
  """

  use Continuum.Test.SQLiteCase

  alias Continuum.Runtime.{Dispatcher, Instance}
  alias Continuum.Runtime.Journal.SQLite
  alias Continuum.Schema.{ActivityTask, Event, Run, Timer}
  alias Continuum.Test

  defmodule Step do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(value), do: {:ok, value + 1}
  end

  defmodule Charge do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(value) do
      if Agent.get(__MODULE__, & &1), do: raise("dependency unavailable"), else: {:ok, value}
    end
  end

  defmodule LongFlow do
    use Continuum.Workflow, version: 1

    def run(%{seed: seed}) do
      {:ok, first} = activity(Step.run(seed))
      timer(days(30))
      {:ok, second} = activity(Step.run(first))
      {:ok, second}
    end
  end

  defmodule WaitFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      payload = await(signal(:go, timeout: hours(1)))
      timer(hours(1))
      {:ok, payload}
    end
  end

  defmodule ChargeFlow do
    use Continuum.Workflow, version: 1

    def run(%{value: value}) do
      {:ok, charged} = activity(Charge.run(value))
      {:ok, charged + 1}
    end
  end

  defmodule EchoFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      first = Continuum.side_effect(fn -> input end)
      Continuum.side_effect(fn -> first end)
    end
  end

  defmodule LoopFlow do
    use Continuum.Workflow, version: 1, retention: {:milliseconds, 1}

    def run(%{left: 0}), do: {:ok, :done}
    def run(%{left: left}), do: continue_as_new(%{left: left - 1})
  end

  setup do
    name = :"sqlite_tooling_#{System.unique_integer([:positive])}"

    children =
      Continuum.children(
        name: name,
        repo: SQLiteRepo,
        heartbeater: false,
        recovery: false,
        dispatcher: false,
        activity_dispatcher: false,
        timer_wheel: false,
        signal_router: [listen?: false],
        version_registry: false
      )

    start_supervised!(%{
      id: name,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
      type: :supervisor
    })

    start_supervised!(%{
      id: Charge,
      start: {Agent, :start_link, [fn -> true end, [name: Charge]]}
    })

    %{name: name, opts: [instance: name]}
  end

  describe "Continuum.Test" do
    test "drives a run through a crash, an expired lease, and elapsed timers", %{opts: opts} do
      {:ok, run_id} = Continuum.start(LongFlow, %{seed: 40}, opts)

      # The run also rests while its first activity is pending; crash it only
      # once it waits on the timer, so elapse_timers! has a timer to move.
      :ok = drive_until_timer(run_id, opts)
      :ok = Test.crash!(run_id, opts)
      :ok = Test.expire_lease!(run_id, opts)
      :ok = Test.elapse_timers!(run_id, opts)

      assert {:ok, %{state: :completed, result: {:ok, 42}}} = Test.drive(run_id, opts)

      assert run_id |> Test.history([journal: :sqlite] ++ opts) |> Enum.map(& &1.type) == [
               :activity_scheduled,
               :activity_completed,
               :timer_started,
               :timer_fired,
               :activity_scheduled,
               :activity_completed
             ]
    end

    test "injects signals and fires timers", %{opts: opts} do
      {:ok, run_id} = Continuum.start(WaitFlow, %{}, opts)
      :ok = Test.drive_until_state(run_id, [:suspended], opts)

      assert :ok = Test.inject_signal(run_id, :go, :approved, [journal: :sqlite] ++ opts)
      :ok = Test.drive_until_state(run_id, [:suspended], opts)
      eventually(fn -> SQLiteRepo.aggregate(Timer, :count) == 2 end)

      assert :ok = Test.fire_timer(run_id, [journal: :sqlite] ++ opts)
      assert {:ok, %{state: :completed, result: {:ok, :approved}}} = Test.drive(run_id, opts)
    end
  end

  describe "queries" do
    setup %{name: name} do
      instance = Instance.lookup(name)

      start = fn input, opts ->
        run_id = Ecto.UUID.generate()
        :ok = SQLite.start_run(instance, run_id, EchoFlow, input, opts)
        run_id
      end

      eu =
        start.(1,
          attributes: %{region: "eu", tier: 4, customer: %{profile: %{tier: 4}}}
        )

      us = start.(2, attributes: %{region: "us", tier: "4", "odd.key": "x"})
      other = start.(3, attributes: %{region: "eu"}, namespace: "other")

      %{eu: eu, us: us, other: other}
    end

    test "filter attributes by type, nesting, and negation", %{name: name, eu: eu, us: us} do
      assert ids(name, where: [{:eq, [:attributes, "region"], "eu"}]) == [eu]
      assert ids(name, where: [{:eq, [:attributes, :tier], 4}]) == [eu]
      assert ids(name, where: [{:eq, [:attributes, :tier], "4"}]) == [us]
      assert ids(name, where: [{:eq, [:attributes, :customer, :profile, :tier], 4}]) == [eu]
      assert ids(name, where: [{:eq, [:attributes, :customer], %{profile: %{tier: 4}}}]) == [eu]
      assert ids(name, where: [{:neq, [:attributes, :region], "eu"}]) == [us]
      assert ids(name, where: [{:eq, [:attributes, "odd.key"], "x"}]) == [us]
      assert ids(name, where: [{:neq, [:attributes, :missing], "x"}]) == []

      assert {:error, {:unsupported_attribute_value, [1]}} =
               Continuum.list_runs(name, where: [{:eq, [:attributes, :tier], [1]}])
    end

    test "search, namespaces, ordering, and pagination", %{name: name, eu: eu, us: us} do
      assert Enum.sort(ids(name, workflow: "echoflow")) == Enum.sort([eu, us])
      assert ids(name, search: String.slice(eu, 0, 13)) == [eu]
      assert [_other] = ids(name, namespace: "other")

      {:ok, first} = Continuum.list_runs(name, order_by: {:asc, :started_at}, per_page: 1)

      {:ok, second} =
        Continuum.list_runs(name, order_by: {:asc, :started_at}, cursor: first.next_cursor)

      assert Enum.map(first.entries ++ second.entries, & &1.run_id) == [eu, us]
      assert second.next_cursor == nil
    end

    test "set_attributes merges shallowly and honors the namespace", %{opts: opts, eu: eu} do
      assert :ok = Continuum.set_attributes(eu, %{tier: 5, note: nil}, opts)

      assert {:ok, %{attributes: attributes}} = Continuum.get_run(eu, opts)
      assert attributes["tier"] == 5
      assert attributes["region"] == "eu"
      assert Map.has_key?(attributes, "note")

      assert {:error, :not_found} =
               Continuum.set_attributes(eu, %{tier: 6}, [namespace: "other"] ++ opts)

      assert {:error, :not_found} = Continuum.set_attributes(Ecto.UUID.generate(), %{a: 1}, opts)
    end
  end

  test "Multi ingress commits with business writes and dispatches after commit", %{
    name: name,
    opts: opts
  } do
    SQLiteRepo.query!("CREATE TABLE IF NOT EXISTS multi_business (id TEXT PRIMARY KEY)")
    SQLiteRepo.query!("DELETE FROM multi_business")

    multi = fn key ->
      Ecto.Multi.new()
      |> Ecto.Multi.run(:business, fn repo, _changes ->
        repo.query!("INSERT OR IGNORE INTO multi_business VALUES ('order-1')")
        {:ok, "order-1"}
      end)
      |> Continuum.Multi.enqueue(:workflow, EchoFlow, fn %{business: id} -> id end,
        instance: name,
        idempotency_key: key
      )
    end

    assert {:ok, %{workflow: %{run_id: run_id, status: :enqueued}}} =
             SQLiteRepo.transaction(multi.("order-1"), mode: :immediate)

    assert %Run{lease_owner: nil} = SQLiteRepo.get!(Run, run_id)

    assert {:ok, %{workflow: %{run_id: ^run_id, status: :existing}}} =
             SQLiteRepo.transaction(multi.("order-1"), mode: :immediate)

    failing = Ecto.Multi.run(multi.("order-2"), :fail, fn _repo, _ -> {:error, :boom} end)
    assert {:error, :fail, :boom, _changes} = SQLiteRepo.transaction(failing, mode: :immediate)
    assert SQLiteRepo.aggregate(Run, :count) == 1

    assert {:ok, 1} = Dispatcher.dispatch_once(opts)
    assert {:ok, %{state: :completed, result: "order-1"}} = Test.drive(run_id, opts)
  end

  test "operators dead-letter and manually retry a failed activity", %{opts: opts} do
    {:ok, run_id} = Continuum.start(ChargeFlow, %{value: 41}, opts)
    assert {:error, %{state: :failed}} = Test.drive(run_id, opts)

    task = SQLiteRepo.one!(from(t in ActivityTask, where: t.run_id == ^run_id))
    audit = [operator: "oncall", reason: "dependency recovered", execute: true] ++ opts

    assert {:ok, %{status: :executed}} = Continuum.ActivityOperations.dead_letter(task.id, audit)
    assert SQLiteRepo.get!(ActivityTask, task.id).state == "dead_lettered"

    Agent.update(Charge, fn _ -> false end)

    assert {:ok, %{status: :executed, successor_task_id: successor}} =
             Continuum.ActivityOperations.retry(task.id, audit)

    assert %ActivityTask{parent_task_id: parent} = SQLiteRepo.get!(ActivityTask, successor)
    assert parent == task.id
    assert {:ok, %{state: :completed, result: {:ok, 42}}} = Test.drive(run_id, opts)
  end

  test "retention pruning removes expired terminal chains", %{opts: opts} do
    {:ok, run_id} = Continuum.start(LoopFlow, %{left: 2}, opts)
    assert {:ok, %{state: :completed}} = Test.drive(run_id, opts)

    SQLiteRepo.update_all(from(r in Run, where: r.correlation_id == ^run_id),
      set: [completed_at: ~U[2020-01-01 00:00:00.000000Z]]
    )

    assert {:ok, %{chain_count: 1, run_count: 3} = plan} =
             Continuum.Pruner.plan(repo: SQLiteRepo, older_than_days: 1)

    assert hd(plan.run_ids) == run_id
    assert plan.dependent_counts.events > 0
    assert {:ok, %{deleted_run_count: 3}} = Continuum.Pruner.execute(plan, repo: SQLiteRepo)
    assert SQLiteRepo.aggregate(Run, :count) == 0
    assert SQLiteRepo.aggregate(Event, :count) == 0
  end

  test "archiving continued chains deletes expired non-tail runs" do
    previous_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(previous_shell) end)

    root = insert_chain_run!(nil)
    tail = insert_chain_run!(root)

    Mix.Task.rerun("continuum.archive_continued_chains", [
      "--repo",
      "Continuum.Test.SQLiteRepo",
      "--older-than",
      "1d",
      "--execute"
    ])

    assert_received {:mix_shell, :info, ["Deleted 1 continuum_runs rows"]}
    assert SQLiteRepo.get(Run, root) == nil
    assert SQLiteRepo.get(Run, tail)
  end

  test "health reports SQLite runtime state and applies fenced repairs", %{name: name} do
    run_id = Ecto.UUID.generate()
    past = DateTime.add(DateTime.utc_now(), -120, :second)

    SQLiteRepo.insert!(%Run{
      id: run_id,
      workflow: inspect(EchoFlow),
      version_hash: <<1>>,
      state: "suspended",
      input: :erlang.term_to_binary(nil),
      lease_owner: "dead-node",
      lease_token: 7,
      lease_acquired_at: past,
      lease_heartbeat_at: past,
      lease_expires_at: past
    })

    SQLiteRepo.insert!(%Timer{
      id: Ecto.UUID.generate(),
      run_id: run_id,
      fires_at: past,
      fired: false
    })

    assert {:ok, report} = Continuum.Health.report(instance: name, lost_wake_after_ms: 0)
    assert report.status == :degraded
    assert report.partitions == %{report.partitions | status: :ok, applicable?: false}
    assert report.timers.overdue_count == 1
    assert %DateTime{} = hd(report.timers.overdue).fires_at
    assert report.leases.expired_count == 1
    assert [%{run_id: ^run_id, expired: true}] = report.leases.entries
    assert [%{state: "suspended", count: 1}] = report.runs.groups

    repair = [instance: name, lease_owner: "dead-node", lease_token: 7]

    assert {:ok, %{status: :planned}} =
             Continuum.Health.repair(:release_expired_lease, run_id, repair)

    assert {:ok, %{status: :executed}} =
             Continuum.Health.repair(:release_expired_lease, run_id, [execute: true] ++ repair)

    assert %Run{lease_owner: nil, next_wakeup_at: %DateTime{}} = SQLiteRepo.get!(Run, run_id)
  end

  test "Observer replay reports are bounded and read-only", %{opts: opts} do
    {:ok, run_id} = Continuum.start(EchoFlow, %{secret: "private"}, opts)
    assert {:ok, %{state: :completed}} = Test.drive(run_id, opts)
    before = SQLiteRepo.get!(Run, run_id)

    assert {:ok, report} =
             Continuum.Observer.replay_report(run_id, [redactor: fn _ -> :redacted end] ++ opts)

    assert report.outcome == :completed
    assert report.agrees_with_stored_result?
    assert report.event_count == 2

    assert {:error, :history_event_limit} =
             Continuum.Observer.replay_report(run_id, [max_events: 1] ++ opts)

    assert {:error, :run_payload_too_large} =
             Continuum.Observer.replay_report(run_id, [max_payload_bytes: 1] ++ opts)

    assert SQLiteRepo.get!(Run, run_id) == before
  end

  test "event partitions are refused on SQLite", %{name: name} do
    assert {:error, :partitions_unsupported_on_sqlite} =
             Continuum.Partitions.plan(repo: SQLiteRepo)

    assert {:error, :partitions_unsupported_on_sqlite} =
             Continuum.Partitions.ensure(instance: name)

    assert {:error, {:partitions_unsupported_on_sqlite, _child}} =
             start_supervised({Continuum.Runtime.PartitionMaintainer, instance: name})
  end

  defp drive_until_timer(run_id, opts) do
    eventually(fn ->
      _ = Test.drive(run_id, Keyword.put(opts, :timeout, 20))
      SQLiteRepo.exists?(from(t in Timer, where: t.run_id == ^run_id))
    end)

    Test.drive_until_state(run_id, [:suspended], opts)
  end

  defp ids(name, opts) do
    {:ok, page} = Continuum.list_runs(name, opts)
    Enum.map(page.entries, & &1.run_id)
  end

  defp insert_chain_run!(continued_from) do
    id = Ecto.UUID.generate()
    long_ago = ~U[2020-01-01 00:00:00.000000Z]

    SQLiteRepo.insert!(%Run{
      id: id,
      workflow: inspect(LoopFlow),
      version_hash: <<1>>,
      state: if(continued_from, do: "running", else: "completed"),
      input: :erlang.term_to_binary(nil),
      correlation_id: continued_from || id,
      continued_from_run_id: continued_from,
      completed_at: if(continued_from, do: nil, else: long_ago),
      retention_until: if(continued_from, do: nil, else: long_ago)
    })

    id
  end

  defp eventually(fun, attempts \\ 200) do
    cond do
      fun.() -> :ok
      attempts == 0 -> flunk("condition was not met in time")
      true -> Process.sleep(10) && eventually(fun, attempts - 1)
    end
  end
end
