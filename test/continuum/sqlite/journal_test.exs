defmodule Continuum.SQLite.JournalTest do
  @moduledoc """
  The durable journal on SQLite: the same CRUD, fencing, and transactional
  scheduling guarantees the PostgreSQL journal tests cover, exercised through
  `Continuum.Runtime.Journal.SQLite`.
  """

  use Continuum.Test.SQLiteCase

  alias Continuum.Runtime.{Instance, JournalError}
  alias Continuum.Runtime.Journal.SQLite
  alias Continuum.Schema.{ActivityTask, Event, Run, Signal, Snapshot, Timer}

  defmodule Flow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, input}
  end

  defmodule RetainedFlow do
    use Continuum.Workflow, retention: {:milliseconds, 5_000}

    def run(input), do: input
  end

  setup do
    name = :"sqlite_journal_#{System.unique_integer([:positive])}"
    instance = Instance.register(Instance.new(name: name, repo: SQLiteRepo))
    start_supervised!({Registry, keys: :unique, name: instance.registry})
    start_supervised!({Phoenix.PubSub, name: instance.pubsub})
    %{instance: instance}
  end

  test "named instances with a SQLite repo select the SQLite journal", %{instance: instance} do
    assert Instance.journal(instance) == SQLite
  end

  describe "runs" do
    test "start, read, complete, and fail", %{instance: instance} do
      completed = Ecto.UUID.generate()
      failed = Ecto.UUID.generate()

      assert :ok =
               SQLite.start_run(instance, completed, Flow, %{foo: :bar},
                 attributes: %{region: "eu"},
                 trace_context: "00-trace"
               )

      assert %{
               state: :running,
               input: %{foo: :bar},
               attributes: %{"region" => "eu"},
               namespace: "default",
               trace_context: "00-trace"
             } = SQLite.get_run(instance, completed)

      assert :ok = SQLite.complete!(instance, completed, {:ok, 99}, nil)
      assert %{state: :completed, result: {:ok, 99}} = SQLite.get_run(instance, completed)

      :ok = SQLite.start_run(instance, failed, Flow, %{})
      stacktrace = [{Flow, :run, 1, [file: ~c"flow.ex", line: 1]}]
      assert :ok = SQLite.fail!(instance, failed, {:exit, :boom, stacktrace}, nil)

      assert %{
               state: :failed,
               error: %Continuum.RunFailure{kind: :exit, reason: :boom},
               error_stacktrace: ^stacktrace
             } = SQLite.get_run(instance, failed)

      assert SQLite.get_run(instance, Ecto.UUID.generate()) == nil
    end

    test "a reused run id is rejected without replacing the run", %{instance: instance} do
      run_id = Ecto.UUID.generate()

      assert :ok = SQLite.start_run(instance, run_id, Flow, %{attempt: 1})
      assert {:error, %Ecto.Changeset{}} = SQLite.start_run(instance, run_id, Flow, %{attempt: 2})
      assert %{input: %{attempt: 1}} = SQLite.get_run(instance, run_id)
    end

    test "idempotency keys return the run that reserved them", %{instance: instance} do
      first = Ecto.UUID.generate()

      assert :ok = SQLite.start_run(instance, first, Flow, %{}, idempotency_key: "order-1")

      assert {:error, {:already_started, ^first}} =
               SQLite.start_run(instance, Ecto.UUID.generate(), Flow, %{},
                 idempotency_key: "order-1"
               )

      assert :ok =
               SQLite.start_run(instance, Ecto.UUID.generate(), Flow, %{},
                 idempotency_key: "order-1",
                 namespace: "other"
               )
    end

    test "leased starts allocate a fresh fencing token", %{instance: instance} do
      first = Ecto.UUID.generate()
      second = Ecto.UUID.generate()

      assert {:ok, %{token: token1, owner: "node-a"}} =
               SQLite.start_run(instance, first, Flow, %{}, lease: [owner: "node-a"])

      assert {:ok, %{token: token2}} =
               SQLite.start_run(instance, second, Flow, %{}, lease: [owner: "node-a"])

      assert token2 > token1

      run = SQLiteRepo.get!(Run, first)
      assert run.lease_owner == "node-a"
      assert run.lease_token == token1
      assert DateTime.diff(run.lease_expires_at, run.lease_acquired_at, :second) == 30
    end

    test "terminal transitions set terminal-relative retention", %{instance: instance} do
      for terminal <- [:complete, :fail, :cancel] do
        run_id = Ecto.UUID.generate()
        :ok = SQLite.start_run(instance, run_id, RetainedFlow, %{})

        :ok =
          case terminal do
            :complete -> SQLite.complete!(instance, run_id, :ok, nil)
            :fail -> SQLite.fail!(instance, run_id, :boom, nil)
            :cancel -> SQLite.cancel_run!(instance, run_id, nil)
          end

        run = SQLiteRepo.get!(Run, run_id)
        assert DateTime.diff(run.retention_until, run.completed_at, :millisecond) in 4_000..6_000
      end
    end
  end

  describe "history and fencing" do
    test "events round-trip in sequence order", %{instance: instance} do
      run_id = start!(instance)
      payload = {:ok, %{id: 1, items: [1, 2, 3], nested: %{a: :b}}}

      :ok = SQLite.append!(instance, run_id, side_effect(payload, 1), nil)
      :ok = SQLite.append!(instance, run_id, side_effect(:first, 0), nil)

      assert [%{seq: 0, payload: :first, kind: :user}, %{seq: 1, payload: ^payload}] =
               SQLite.load(instance, run_id)

      raw = SQLiteRepo.one!(from(e in Event, where: e.seq == 1, select: e.payload))
      assert :erlang.binary_to_term(raw).payload == payload
    end

    test "appends and terminal writes are fenced by the lease token", %{instance: instance} do
      run_id = start!(instance)
      lease!(run_id, 42)

      assert :ok = SQLite.append!(instance, run_id, side_effect(1, 0), 42)

      assert_raise JournalError, ~r/lease_mismatch/, fn ->
        SQLite.append!(instance, run_id, side_effect(2, 1), 99)
      end

      assert_raise JournalError, ~r/lease_mismatch/, fn ->
        SQLite.append!(instance, run_id, side_effect(2, 1), nil)
      end

      assert_raise JournalError, ~r/cas_failed/, fn ->
        SQLite.complete!(instance, run_id, :ok, 99)
      end

      :ok = SQLite.complete!(instance, run_id, :ok, 42)

      assert_raise JournalError, ~r/cas_failed/, fn ->
        SQLite.fail!(instance, run_id, :late, 42)
      end

      assert SQLiteRepo.get!(Run, run_id).state == "completed"
    end

    test "implicit sequence numbers serialize concurrent appends", %{instance: instance} do
      run_id = start!(instance)

      1..20
      |> Task.async_stream(
        fn n -> SQLite.append!(instance, run_id, side_effect(n, nil), nil) end,
        max_concurrency: 8,
        timeout: 15_000
      )
      |> Enum.each(&assert(&1 == {:ok, :ok}))

      assert Enum.map(SQLite.load(instance, run_id), & &1.seq) == Enum.to_list(0..19)
    end

    test "snapshots bound the history handed to a resuming engine", %{instance: instance} do
      run_id = start!(instance)
      Enum.each(0..3, &SQLite.append!(instance, run_id, side_effect(&1, &1), nil))

      snapshot = %Continuum.Snapshot{
        run_id: run_id,
        through_seq: 1,
        version_hash: <<1, 2, 3>>,
        taken_at: ~U[2026-10-18 12:00:00.000000Z]
      }

      assert :ok = SQLite.take_snapshot!(instance, snapshot)
      assert :ok = SQLite.take_snapshot!(instance, snapshot)
      assert SQLiteRepo.aggregate(Snapshot, :count) == 1

      assert {%Continuum.Snapshot{through_seq: 1}, [%{seq: 2}, %{seq: 3}]} =
               SQLite.load_with_snapshot(instance, run_id, nil)
    end
  end

  describe "activities" do
    test "scheduling writes the event and task in one fenced transaction", %{instance: instance} do
      run_id = start!(instance)
      lease!(run_id, 7)
      task = activity_task(run_id, 0)

      assert_raise JournalError, fn ->
        SQLite.schedule_activity!(instance, run_id, scheduled_event(task), task, 8)
      end

      assert SQLiteRepo.aggregate(ActivityTask, :count) == 0
      assert SQLite.load(instance, run_id) == []

      :ok = SQLite.schedule_activity!(instance, run_id, scheduled_event(task), task, 7)

      assert %ActivityTask{state: "available", attempt: 1} =
               SQLiteRepo.get!(ActivityTask, task.id)

      assert [%{type: :activity_scheduled}] = SQLite.load(instance, run_id)
    end

    test "completion commits the result once per idempotency key", %{instance: instance} do
      run_id = start!(instance)
      lease!(run_id, 7)
      first = activity_task(run_id, 0)
      second = activity_task(run_id, 2)

      for task <- [first, second] do
        :ok = SQLite.schedule_activity!(instance, run_id, scheduled_event(task), task, 7)
        lease_task!(task)
      end

      idempotency = [module: __MODULE__, key: "charge-1"]

      :ok =
        SQLite.complete_activity_task!(instance, leased(first, instance), :first_result, 7,
          idempotency: idempotency
        )

      :ok =
        SQLite.complete_activity_task!(instance, leased(second, instance), :second_result, 7,
          idempotency: idempotency
        )

      assert {:ok, :first_result} = SQLite.get_activity_result(instance, __MODULE__, "charge-1")

      assert [_scheduled, %{type: :activity_completed, payload: :first_result}, _second, done] =
               SQLite.load(instance, run_id)

      assert done.payload == :first_result
      assert SQLiteRepo.get!(Run, run_id).next_wakeup_at
    end

    test "completion rejects an expired task lease", %{instance: instance} do
      run_id = start!(instance)
      lease!(run_id, 7)
      task = activity_task(run_id, 0)
      :ok = SQLite.schedule_activity!(instance, run_id, scheduled_event(task), task, 7)
      lease_task!(task, DateTime.add(DateTime.utc_now(), -1, :second))

      assert_raise JournalError, ~r/activity_task_lease_expired/, fn ->
        SQLite.complete_activity_task!(instance, leased(task), :late, 7)
      end
    end

    test "retries move the task back to available after the backoff", %{instance: instance} do
      run_id = start!(instance)
      lease!(run_id, 7)
      task = activity_task(run_id, 0)
      :ok = SQLite.schedule_activity!(instance, run_id, scheduled_event(task), task, 7)
      lease_task!(task)

      before = DateTime.utc_now()
      :ok = SQLite.retry_activity_task!(instance, leased(task), :transient, 2_000, 7)

      retried = SQLiteRepo.get!(ActivityTask, task.id)
      assert retried.state == "available"
      assert retried.attempt == 2
      assert retried.lease_owner == nil
      assert DateTime.diff(retried.available_at, before, :millisecond) in 1_900..3_000
    end
  end

  describe "timers" do
    test "arming notifies the local timer wheel after commit", %{instance: instance} do
      Process.register(self(), instance.timer_wheel)
      run_id = start!(instance)
      lease!(run_id, 3)
      timer_id = Ecto.UUID.generate()
      fires_at = ~U[2030-01-01 00:00:00.000000Z]

      event = %{type: :timer_started, timer_id: timer_id, ms: 1_000, command_id: nil, seq: 0}

      :ok =
        SQLite.schedule_timer!(instance, run_id, event, %{id: timer_id, fires_at: fires_at}, 3)

      assert_receive {:notification, _pid, _ref, "continuum_timer_armed", payload}
      assert payload == "#{run_id}|#{DateTime.to_iso8601(fires_at)}"
      assert SQLiteRepo.get!(Run, run_id).next_wakeup_at == fires_at

      :ok = SQLite.fire_timer!(instance, run_id, timer_id, 3)
      :ok = SQLite.fire_timer!(instance, run_id, timer_id, 3)

      assert %Timer{fired: true} = SQLiteRepo.get!(Timer, timer_id)
      assert [_started, %{type: :timer_fired, seq: 1}] = SQLite.load(instance, run_id)
    end

    test "a rolled-back timer never notifies the wheel", %{instance: instance} do
      Process.register(self(), instance.timer_wheel)
      run_id = start!(instance)
      lease!(run_id, 3)
      timer_id = Ecto.UUID.generate()
      event = %{type: :timer_started, timer_id: timer_id, ms: 1, command_id: nil, seq: 0}

      assert_raise JournalError, fn ->
        SQLite.schedule_timer!(
          instance,
          run_id,
          event,
          %{id: timer_id, fires_at: DateTime.utc_now()},
          4
        )
      end

      refute_received {:notification, _, _, _, _}
    end
  end

  describe "signals" do
    test "delivery is buffered, deduplicated, and consumed exactly once", %{instance: instance} do
      run_id = start!(instance)
      lease!(run_id, 5)

      assert {:ok, ^run_id, :delivered} =
               SQLite.deliver_signal!(instance, run_id, :approve, %{by: "ops"},
                 delivery_id: "d-1"
               )

      assert {:ok, ^run_id, :duplicate} =
               SQLite.deliver_signal!(instance, run_id, :approve, %{by: "ops"},
                 delivery_id: "d-1"
               )

      assert SQLiteRepo.aggregate(Signal, :count) == 1

      assert {:ok, %{by: "ops"}, %{type: :signal_received, seq: 0}} =
               SQLite.consume_pending_signal!(instance, run_id, :approve, nil, 0, 5)

      assert SQLite.consume_pending_signal!(instance, run_id, :approve, nil, 1, 5) == :none
      assert SQLiteRepo.one!(from(s in Signal, select: s.delivered))
    end

    test "terminal and unknown runs refuse delivery", %{instance: instance} do
      run_id = start!(instance)
      :ok = SQLite.complete!(instance, run_id, :ok, nil)

      assert {:error, :run_terminal} = SQLite.deliver_signal!(instance, run_id, :late, 1, [])

      assert {:error, :not_found} =
               SQLite.deliver_signal!(instance, Ecto.UUID.generate(), :late, 1, [])
    end
  end

  describe "composition" do
    test "children are started, awaited, and cancelled with their parent", %{instance: instance} do
      parent = start!(instance)
      lease!(parent, 9)
      child = Ecto.UUID.generate()

      started = %{type: :child_started, child_run_id: child, command_id: nil, seq: 0}

      :ok =
        SQLite.start_child!(
          instance,
          parent,
          %{
            child_run_id: child,
            workflow: Flow,
            input: %{n: 1},
            parent_command_id: nil,
            trace_context: nil,
            started_event: started
          },
          9
        )

      assert %Run{parent_run_id: ^parent, state: "running"} = SQLiteRepo.get!(Run, child)
      assert SQLite.await_child_terminal!(instance, parent, child, nil, 1, 9) == :pending

      grandchild = Ecto.UUID.generate()

      SQLiteRepo.insert!(%Run{
        id: grandchild,
        workflow: inspect(Flow),
        version_hash: <<0>>,
        state: "suspended",
        input: :erlang.term_to_binary(nil),
        parent_run_id: child
      })

      :ok = SQLite.cancel_run!(instance, parent, 9)

      assert %{state: :cancelled} = SQLite.get_run(instance, parent)
      assert %{state: :cancelled, error: :parent_cancelled} = SQLite.get_run(instance, child)
      assert %{state: :cancelled} = SQLite.get_run(instance, grandchild)
    end

    test "child completion is journaled on the parent", %{instance: instance} do
      parent = start!(instance)
      lease!(parent, 9)
      child = Ecto.UUID.generate()

      :ok =
        SQLite.start_child!(
          instance,
          parent,
          %{
            child_run_id: child,
            workflow: Flow,
            input: %{},
            parent_command_id: nil,
            trace_context: nil,
            started_event: %{type: :child_started, child_run_id: child, command_id: nil, seq: 0}
          },
          9
        )

      :ok = SQLite.complete!(instance, child, {:ok, :child_done}, nil)
      assert SQLiteRepo.get!(Run, parent).next_wakeup_at

      assert {:completed, {:ok, :child_done}, %{type: :child_completed, seq: 1}} =
               SQLite.await_child_terminal!(instance, parent, child, nil, 1, 9)
    end

    test "continue_as_new moves the chain and its pending signals", %{instance: instance} do
      root = start!(instance)
      lease!(root, 11)
      {:ok, ^root, :delivered} = SQLite.deliver_signal!(instance, root, :next, 1, [])
      successor = Ecto.UUID.generate()

      event = %{
        type: :run_continued_as_new,
        next_run_id: successor,
        next_input_hash: <<0>>,
        command_id: nil,
        seq: 0
      }

      assert root == SQLite.continue_as_new!(instance, root, successor, %{n: 2}, event, 11)

      assert %{state: :completed, result: {:continued, ^successor}} =
               SQLite.get_run(instance, root)

      assert %Run{continued_from_run_id: ^root, correlation_id: ^root} =
               SQLiteRepo.get!(Run, successor)

      assert SQLite.resolve_chain_tip(instance, root) == successor
      assert SQLiteRepo.one!(from(s in Signal, select: s.run_id)) == successor

      assert {:ok, ^successor, :delivered} = SQLite.deliver_signal!(instance, root, :again, 2, [])
    end
  end

  defp start!(instance) do
    run_id = Ecto.UUID.generate()
    :ok = SQLite.start_run(instance, run_id, Flow, %{})
    run_id
  end

  defp lease!(run_id, token) do
    SQLiteRepo.update_all(from(r in Run, where: r.id == ^run_id),
      set: [lease_token: token, lease_owner: "node-1"]
    )
  end

  defp side_effect(payload, seq),
    do: %{type: :side_effect, kind: :user, payload: payload, seq: seq}

  defp activity_task(run_id, seq) do
    %{
      id: Ecto.UUID.generate(),
      run_id: run_id,
      seq: seq,
      mfa: {__MODULE__, :work, []},
      command_id: nil
    }
  end

  defp scheduled_event(task) do
    %{
      type: :activity_scheduled,
      task_id: task.id,
      mfa: task.mfa,
      opts: [],
      command_id: nil,
      seq: task.seq
    }
  end

  defp lease_task!(task, expires_at \\ DateTime.add(DateTime.utc_now(), 30, :second)) do
    SQLiteRepo.update_all(from(t in ActivityTask, where: t.id == ^task.id),
      set: [state: "leased", lease_owner: "worker-1", lease_expires_at: expires_at]
    )
  end

  defp leased(task), do: Map.merge(task, %{lease_owner: "worker-1", attempt: 1})

  defp leased(task, instance), do: task |> leased() |> Map.put(:instance, instance)
end
