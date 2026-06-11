defmodule Continuum.TestSupport.GoldenJournalFixtures do
  @moduledoc false

  import Ecto.Query

  alias Continuum.Runtime.{ActivityWorker, Dispatcher, Instance, Journal}

  alias Continuum.Schema.{
    ActivityResult,
    ActivityTask,
    Event,
    Run,
    Signal,
    Snapshot,
    Timer,
    WorkflowVersion
  }

  alias Continuum.Test.Repo

  @continuum_version Mix.Project.config()[:version]

  @fixture_names ~w(
    activity_signal_activity
    signal_timeout
    timer_wakeup
    activity_retry_success
    activity_terminal_failure
    side_effect
    deterministic_primitives
    saga_compensation_success
    saga_compensation_failure
    parallel_compensation
    child_fanout
    parent_cancellation_cascade
    continue_as_new_chain
    continued_child_parent_await
    patched_old_history
    patched_new_history
    snapshot_prefix
    namespace_search_attributes
    unknown_workflow_version
  )

  def fixture_names, do: @fixture_names

  def fixture_path(name) do
    Path.join(["test", "fixtures", "journals", "#{name}.journal"])
  end

  def load!(name) do
    name
    |> fixture_path()
    |> File.read!()
    |> :erlang.binary_to_term()
  end

  def generate_all!(dir \\ Path.join(["test", "fixtures", "journals"])) do
    ensure_repo_started()
    checkout_sandbox()
    File.mkdir_p!(dir)

    Enum.each(@fixture_names, fn name ->
      fixture = apply(__MODULE__, String.to_existing_atom("build_#{name}"), [])
      File.write!(Path.join(dir, "#{name}.journal"), :erlang.term_to_binary(fixture))
    end)

    :ok
  end

  defmodule ActivitySignalFlow do
    use Continuum.Workflow, version: 1
    alias Continuum.TestSupport.GoldenJournalFixtures.Activities

    def run(input) do
      {:ok, first} = activity(Activities.echo({:reserved, input.order_id}))
      decision = await(signal(:approval))
      {:ok, second} = activity(Activities.echo({:charged, decision}))
      {:ok, {first, second}}
    end
  end

  defmodule SignalTimeoutFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      case await(signal(:approval, timeout: input.timeout_ms)) do
        :timeout -> {:ok, :timed_out}
        payload -> {:ok, payload}
      end
    end
  end

  defmodule TimerFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      timer(input.ms)
      {:ok, :timer_fired}
    end
  end

  defmodule RetryFlow do
    use Continuum.Workflow, version: 1
    alias Continuum.TestSupport.GoldenJournalFixtures.RetryActivity

    def run(input) do
      {:ok, value} =
        activity(RetryActivity.run(input.seed),
          retry: [max_attempts: 2, backoff: :constant, base_ms: 1]
        )

      {:ok, value}
    end
  end

  defmodule TerminalFailureError do
    defexception [:reason, message: "terminal activity failure"]
  end

  defmodule TerminalFailureFlow do
    use Continuum.Workflow, version: 1

    alias Continuum.TestSupport.GoldenJournalFixtures.{
      TerminalFailureActivity,
      TerminalFailureError
    }

    def run(input) do
      case activity(TerminalFailureActivity.run(input.seed)) do
        {:error, %TerminalFailureError{reason: reason}} -> {:ok, {:failed, reason}}
        other -> {:unexpected, other}
      end
    end
  end

  defmodule SideEffectFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      value = Continuum.side_effect(fn -> input.seed * 2 end)
      {:ok, value + 1}
    end
  end

  defmodule DeterministicPrimitivesFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      {:ok, {Continuum.now(), Continuum.today(), Continuum.uuid4(), Continuum.random()}}
    end
  end

  defmodule SagaSuccessFlow do
    use Continuum.Workflow, version: 1
    alias Continuum.TestSupport.GoldenJournalFixtures.Activities

    def run(input) do
      {:ok, charge} =
        activity(Activities.echo({:charged, input.order_id}),
          compensate: {Activities, :refund, [input.order_id]}
        )

      compensate(charge)
      {:ok, :refunded}
    end
  end

  defmodule SagaFailureFlow do
    use Continuum.Workflow, version: 1
    alias Continuum.TestSupport.GoldenJournalFixtures.Activities

    def run(input) do
      {:ok, charge} =
        activity(Activities.echo({:charged, input.order_id}),
          compensate: {Activities, :fail_refund, [input.order_id]}
        )

      result = compensate(charge)
      {:ok, {:compensation, result}}
    end
  end

  defmodule ParallelCompensationFlow do
    use Continuum.Workflow, version: 1
    alias Continuum.TestSupport.GoldenJournalFixtures.Activities

    def run(input) do
      {:ok, _charge} =
        activity(Activities.echo({:charged, input.order_id}),
          compensate: {Activities, :refund, [input.order_id]}
        )

      {:ok, _reserve} =
        activity(Activities.echo({:reserved, input.order_id}),
          compensate: {Activities, :release, [input.order_id]}
        )

      compensate_all(mode: :parallel)
      {:ok, :parallel_compensated}
    end
  end

  defmodule LeafFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, {:leaf, input.id}}
  end

  defmodule FanoutParentFlow do
    use Continuum.Workflow, version: 1
    alias Continuum.TestSupport.GoldenJournalFixtures.LeafFlow

    def run(input) do
      result =
        input.ids
        |> Enum.map(fn id -> start_child(LeafFlow, %{id: id}, id: "leaf-#{id}") end)
        |> Enum.map(&await_child/1)

      {:ok, result}
    end
  end

  defmodule SlowLeafFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: {:ok, await(signal(:never))}
  end

  defmodule CancelParentFlow do
    use Continuum.Workflow, version: 1
    alias Continuum.TestSupport.GoldenJournalFixtures.SlowLeafFlow

    def run(_input) do
      {:ok, _} = await(child(SlowLeafFlow.run(%{})))
      {:ok, :unreachable}
    end
  end

  defmodule CycleActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(n), do: {:ok, n}
  end

  defmodule CycleFlow do
    use Continuum.Workflow, version: 1
    alias Continuum.TestSupport.GoldenJournalFixtures.CycleActivity

    def run(%{n: n, max: max}) do
      {:ok, _} = activity(CycleActivity.run(n))

      if n >= max do
        {:ok, {:done, n}}
      else
        continue_as_new(%{n: n + 1, max: max})
      end
    end
  end

  defmodule ParentOfCycleFlow do
    use Continuum.Workflow, version: 1
    alias Continuum.TestSupport.GoldenJournalFixtures.CycleFlow

    def run(input) do
      result = await(child(CycleFlow.run(%{n: 1, max: input.max})))
      {:ok, {:parent_saw, result}}
    end
  end

  defmodule PatchedOldFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      if Continuum.patched?(:golden_patch) do
        {:ok, :new_branch}
      else
        value = Continuum.side_effect(fn -> :old_branch end)
        {:ok, value}
      end
    end
  end

  defmodule PatchedNewFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      if Continuum.patched?(:golden_patch) do
        {:ok, :new_branch}
      else
        {:ok, :old_branch}
      end
    end
  end

  defmodule SnapshotFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      result =
        Enum.reduce(input.steps, 0, fn step, acc ->
          Continuum.side_effect(fn -> acc + step end)
        end)

      {:ok, result}
    end
  end

  defmodule Activities do
    use Continuum.Activity, retry: [max_attempts: 1]

    def echo(value), do: {:ok, value}
    def refund(order_id), do: {:ok, {:refunded, order_id}}
    def release(order_id), do: {:ok, {:released, order_id}}
    def fail_refund(order_id), do: raise("refund failed for #{order_id}")
  end

  defmodule RetryActivity do
    use Continuum.Activity, retry: [max_attempts: 2, backoff: :constant, base_ms: 1]

    def run(seed) do
      attempt = Agent.get_and_update(__MODULE__, fn current -> {current + 1, current + 1} end)

      if attempt == 1 do
        raise "retry me"
      else
        {:ok, seed * 10}
      end
    end
  end

  defmodule TerminalFailureActivity do
    use Continuum.Activity, retry: [max_attempts: 1]
    alias Continuum.TestSupport.GoldenJournalFixtures.TerminalFailureError

    def run(_seed), do: raise(%TerminalFailureError{reason: :declined})
  end

  def build_activity_signal_activity do
    run_postgres(
      "activity_signal_activity",
      ActivitySignalFlow,
      %{order_id: "ord-100"},
      fn run_id ->
        wait_for_activity_tasks(1)
        dispatch_activity()

        pump_runs_until(fn ->
          event_types(Journal.Postgres.load(Instance.default(), run_id)) == [
            :activity_scheduled,
            :activity_completed,
            :signal_awaited
          ]
        end)

        :ok = Continuum.signal(run_id, :approval, :approved, journal: Journal.Postgres)
        wait_for_activity_tasks(2)
        dispatch_activity()
        await_completed(run_id, {:ok, {{:reserved, "ord-100"}, {:charged, :approved}}})
      end,
      {:ok, {{:reserved, "ord-100"}, {:charged, :approved}}}
    )
  end

  def build_signal_timeout do
    run_postgres(
      "signal_timeout",
      SignalTimeoutFlow,
      %{timeout_ms: 10},
      fn run_id ->
        pump_runs_until(fn ->
          event_types(Journal.Postgres.load(Instance.default(), run_id)) == [:signal_awaited]
        end)

        fire_postgres_timer!(run_id)
        await_completed(run_id, {:ok, :timed_out})
      end,
      {:ok, :timed_out}
    )
  end

  def build_timer_wakeup do
    run_in_memory(
      "timer_wakeup",
      TimerFlow,
      %{ms: 60_000},
      fn run_id ->
        wait_for_event_types(run_id, [:timer_started])
        :ok = Continuum.Test.fire_timer(run_id)
      end,
      {:ok, :timer_fired},
      [:timer_started, :timer_fired]
    )
  end

  def build_activity_retry_success do
    start_agent!(RetryActivity, fn -> 0 end)

    run_postgres(
      "activity_retry_success",
      RetryFlow,
      %{seed: 7},
      fn run_id ->
        wait_for_activity_tasks(1)
        dispatch_activity()
        wait_for_retry_attempt(2)
        make_tasks_due()
        dispatch_activity()
        await_completed(run_id, {:ok, 70})
      end,
      {:ok, 70}
    )
  end

  def build_activity_terminal_failure do
    run_postgres(
      "activity_terminal_failure",
      TerminalFailureFlow,
      %{seed: 7},
      fn run_id ->
        wait_for_activity_tasks(1)
        dispatch_activity()
        await_completed(run_id, {:ok, {:failed, :declined}})
      end,
      {:ok, {:failed, :declined}}
    )
  end

  def build_side_effect do
    run_in_memory(
      "side_effect",
      SideEffectFlow,
      %{seed: 20},
      &await_noop/1,
      {:ok, 41},
      [:side_effect]
    )
  end

  def build_deterministic_primitives do
    Continuum.Test.reset_in_memory!()
    {:ok, run_id} = Continuum.Test.start_synchronous(DeterministicPrimitivesFlow, %{})
    {:ok, %{state: :completed, result: result}} = Continuum.await(run_id, 1_000)
    history = Continuum.Test.history(run_id)

    fixture(
      "deterministic_primitives",
      DeterministicPrimitivesFlow,
      %{},
      result,
      history,
      [:side_effect, :side_effect, :side_effect, :side_effect]
    )
  end

  def build_saga_compensation_success do
    run_in_memory(
      "saga_compensation_success",
      SagaSuccessFlow,
      %{order_id: "ord-200"},
      &await_noop/1,
      {:ok, :refunded},
      [:activity_completed, :compensation_completed]
    )
  end

  def build_saga_compensation_failure do
    run_in_memory(
      "saga_compensation_failure",
      SagaFailureFlow,
      %{order_id: "ord-201"},
      &await_noop/1,
      {:ok, {:compensation, {:error, %RuntimeError{message: "refund failed for ord-201"}}}},
      [:activity_completed, :compensation_failed]
    )
  end

  def build_parallel_compensation do
    run_postgres(
      "parallel_compensation",
      ParallelCompensationFlow,
      %{order_id: "ord-300"},
      fn run_id ->
        wait_for_activity_tasks(1)
        dispatch_activity()
        wait_for_activity_tasks(2)
        dispatch_activity()
        wait_for_activity_tasks(4)
        dispatch_activity()
        dispatch_activity()
        await_completed(run_id, {:ok, :parallel_compensated})
      end,
      {:ok, :parallel_compensated}
    )
    |> Map.put(:replay_journal, Journal.Postgres)
  end

  def build_child_fanout do
    run_postgres(
      "child_fanout",
      FanoutParentFlow,
      %{ids: [1, 2, 3]},
      fn run_id ->
        pump_runs_until(fn -> run_state(run_id) == "completed" end)
        await_completed(run_id, {:ok, [{:ok, {:leaf, 1}}, {:ok, {:leaf, 2}}, {:ok, {:leaf, 3}}]})
      end,
      {:ok, [{:ok, {:leaf, 1}}, {:ok, {:leaf, 2}}, {:ok, {:leaf, 3}}]}
    )
  end

  def build_parent_cancellation_cascade do
    clean_postgres!()
    {:ok, run_id} = Continuum.start(CancelParentFlow, %{}, journal: Journal.Postgres)

    pump_runs_until(fn -> children_of(run_id) != [] end)
    {:ok, 1} = Dispatcher.dispatch_once(owner: "golden-cancel", batch_size: 10)
    pump_runs_until(fn -> match?([%{state: "suspended"}], children_of(run_id)) end)

    :ok = Continuum.cancel(run_id, journal: Journal.Postgres)
    pump_runs_until(fn -> run_state(run_id) == "failed" end)

    history = Journal.Postgres.load(Instance.default(), run_id)

    fixture("parent_cancellation_cascade", CancelParentFlow, %{}, :parent_cancelled, history, [
      :child_started
    ])
    |> Map.put(:replay, :metadata_only)
    |> Map.put(:run_metadata, %{terminal_state: :failed, child_terminal_state: :failed})
  end

  def build_continue_as_new_chain do
    clean_postgres!()
    {:ok, root} = Continuum.start(CycleFlow, %{n: 1, max: 3}, journal: Journal.Postgres)
    pump_runs_until(fn -> chain_done?(root) end)
    [first | _] = chain_runs(root)
    history = Journal.Postgres.load(Instance.default(), first.id)

    fixture(
      "continue_as_new_chain",
      CycleFlow,
      %{n: 1, max: 3},
      {:continued, :next_run_id},
      history,
      [
        :activity_scheduled,
        :activity_completed,
        :run_continued_as_new
      ]
    )
    |> Map.put(:replay, :continued)
    |> Map.put(:run_metadata, %{correlation_id: root, chain_length: 3})
  end

  def build_continued_child_parent_await do
    run_postgres(
      "continued_child_parent_await",
      ParentOfCycleFlow,
      %{max: 3},
      fn run_id ->
        pump_runs_until(fn -> run_state(run_id) == "completed" end)
        await_completed(run_id, {:ok, {:parent_saw, {:ok, {:done, 3}}}})
      end,
      {:ok, {:parent_saw, {:ok, {:done, 3}}}}
    )
  end

  def build_patched_old_history do
    history = [
      %{
        type: :side_effect,
        kind: :user,
        payload: :old_branch,
        seq: 0
      }
    ]

    fixture("patched_old_history", PatchedOldFlow, %{}, {:ok, :old_branch}, history, [
      :side_effect
    ])
  end

  def build_patched_new_history do
    run_in_memory(
      "patched_new_history",
      PatchedNewFlow,
      %{},
      &await_noop/1,
      {:ok, :new_branch},
      [:patched]
    )
  end

  def build_snapshot_prefix do
    Continuum.Test.reset_in_memory!()
    input = %{steps: [1, 2, 3]}
    {:ok, run_id} = Continuum.Test.start_synchronous(SnapshotFlow, input)
    {:ok, %{state: :completed, result: {:ok, 6}}} = Continuum.await(run_id, 1_000)

    [first | rest] = Continuum.Test.history(run_id)

    {:ok, snapshot} =
      Continuum.Snapshot.compact(
        "golden-snapshot-prefix",
        SnapshotFlow.__continuum_workflow__().version_hash,
        [first]
      )

    fixture("snapshot_prefix", SnapshotFlow, input, {:ok, 6}, rest, [:side_effect, :side_effect])
    |> Map.put(:snapshot, snapshot)
    |> Map.put(:snapshot_through_seq, snapshot.through_seq)
  end

  def build_namespace_search_attributes do
    clean_postgres!()

    {:ok, run_id} =
      Continuum.start(SideEffectFlow, %{seed: 5},
        journal: Journal.Postgres,
        namespace: "golden",
        attributes: %{tenant: "acme", priority: 3}
      )

    await_completed(run_id, {:ok, 11})
    run = Repo.get!(Run, run_id)
    history = Journal.Postgres.load(Instance.default(), run_id)

    fixture("namespace_search_attributes", SideEffectFlow, %{seed: 5}, {:ok, 11}, history, [
      :side_effect
    ])
    |> Map.put(:run_metadata, %{namespace: run.namespace, attributes: run.attributes})
  end

  def build_unknown_workflow_version do
    clean_postgres!()
    run_id = Ecto.UUID.generate()
    version_hash = "missing-golden-version"

    %Run{}
    |> Ecto.Changeset.change(%{
      id: run_id,
      workflow: inspect(__MODULE__.MissingLogicalFlow),
      version_hash: version_hash,
      state: "suspended",
      input: :erlang.term_to_binary(%{}),
      next_wakeup_at:
        DateTime.utc_now() |> DateTime.add(-1, :second) |> DateTime.truncate(:microsecond)
    })
    |> Repo.insert!()

    {:ok, 1} = Dispatcher.dispatch_once(owner: "golden-unknown-version", batch_size: 1)

    # The node lacking the version releases the lease and leaves the run
    # suspended (it is no longer marked stuck globally).
    pump_runs_until(fn ->
      run = Repo.get!(Run, run_id)
      run.state == "suspended" and is_nil(run.lease_owner)
    end)

    fixture(
      "unknown_workflow_version",
      __MODULE__.MissingLogicalFlow,
      %{},
      :suspended,
      [],
      []
    )
    |> Map.put(:replay, :metadata_only)
    |> Map.put(:run_metadata, %{
      version_hash: version_hash,
      terminal_state: :suspended
    })
  end

  defp run_in_memory(name, workflow, input, driver, expected_result, expected_event_types) do
    Continuum.Test.reset_in_memory!()
    {:ok, run_id} = Continuum.Test.start_synchronous(workflow, input)
    driver.(run_id)
    {:ok, %{state: :completed, result: ^expected_result}} = Continuum.await(run_id, 1_000)
    history = Continuum.Test.history(run_id)
    fixture(name, workflow, input, expected_result, history, expected_event_types)
  end

  defp run_postgres(name, workflow, input, driver, expected_result) do
    clean_postgres!()
    {:ok, run_id} = Continuum.start(workflow, input, journal: Journal.Postgres)
    driver.(run_id)
    history = Journal.Postgres.load(Instance.default(), run_id)
    fixture(name, workflow, input, expected_result, history, event_types(history))
  end

  defp fixture(name, workflow, input, expected_result, history, expected_event_types) do
    %{
      schema_version: 1,
      continuum_version: @continuum_version,
      name: name,
      workflow_module: workflow,
      input: input,
      expected_result: expected_result,
      expected_event_types: expected_event_types,
      expected_command_ids: command_ids(history),
      history: history,
      replay: :ok
    }
  end

  defp await_noop(_run_id), do: :ok

  defp wait_for_event_types(run_id, expected, attempts \\ 200) do
    wait_until(fn -> event_types(Continuum.Test.history(run_id)) == expected end, attempts)
  end

  defp wait_for_activity_tasks(count),
    do: wait_until(fn -> Repo.aggregate(ActivityTask, :count) >= count end)

  defp wait_for_retry_attempt(attempt) do
    wait_until(fn ->
      Repo.one(from(t in ActivityTask, select: {t.state, t.attempt})) == {"available", attempt}
    end)
  end

  defp dispatch_activity do
    {:ok, 1} = ActivityWorker.Dispatcher.dispatch_once(owner: "golden-activity", batch_size: 1)
  end

  defp make_tasks_due do
    Repo.update_all(ActivityTask,
      set: [available_at: DateTime.add(DateTime.utc_now(), -1, :second)]
    )
  end

  defp await_completed(run_id, expected_result, opts \\ []) do
    await_opts = Keyword.merge([journal: Journal.Postgres], opts)

    {:ok, %{state: :completed, result: ^expected_result}} =
      Continuum.await(run_id, 1_000, await_opts)
  end

  defp event_types(history), do: Enum.map(history, & &1.type)

  defp command_ids(history) do
    history
    |> Enum.filter(&Map.has_key?(&1, :command_id))
    |> Enum.map(& &1.command_id)
  end

  defp clean_postgres! do
    Repo.delete_all(WorkflowVersion)
    Repo.delete_all(Snapshot)
    Repo.delete_all(ActivityResult)
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Timer)
    Repo.delete_all(Signal)
    Repo.delete_all(Event)
    Repo.delete_all(Run)

    if Code.ensure_loaded?(Oban.Job) do
      Repo.delete_all(Oban.Job)
    end

    :ok
  end

  defp pump_runs_until(fun, attempts \\ 400)

  defp pump_runs_until(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Dispatcher.dispatch_once(owner: "golden-run", batch_size: 10)
      ActivityWorker.Dispatcher.dispatch_once(owner: "golden-act", batch_size: 10)
      Process.sleep(5)
      pump_runs_until(fun, attempts - 1)
    end
  end

  defp pump_runs_until(_fun, 0), do: raise("golden fixture pump timed out")

  defp wait_until(fun, attempts \\ 200)

  defp wait_until(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  defp wait_until(_fun, 0), do: raise("golden fixture condition timed out")

  defp children_of(parent_id) do
    Repo.all(
      from(r in Run,
        where: r.parent_run_id == ^parent_id,
        order_by: [asc: r.started_at],
        select: %{id: r.id, state: r.state, error: r.error}
      )
    )
  end

  defp chain_runs(root) do
    Repo.all(
      from(r in Run,
        where: r.id == ^root or r.correlation_id == ^root,
        order_by: [asc: r.started_at],
        select: %{
          id: r.id,
          state: r.state,
          result: r.result,
          correlation_id: r.correlation_id,
          continued_from_run_id: r.continued_from_run_id
        }
      )
    )
  end

  defp chain_done?(root) do
    Enum.any?(chain_runs(root), &(&1.state == "completed" and not continued?(&1.result)))
  end

  defp continued?(nil), do: false
  defp continued?(binary), do: match?({:continued, _}, :erlang.binary_to_term(binary))

  defp run_state(run_id), do: Repo.one(from(r in Run, where: r.id == ^run_id, select: r.state))

  defp pending_timer_id(run_id) do
    Repo.one!(from(t in Timer, where: t.run_id == ^run_id and t.fired == false, select: t.id))
  end

  defp fire_postgres_timer!(run_id) do
    instance = Instance.default()
    lease_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))
    :ok = Journal.Postgres.fire_timer!(instance, run_id, pending_timer_id(run_id), lease_token)
    Continuum.Runtime.Engine.wake(instance, run_id)
    :ok
  end

  defp start_agent!(name, fun) do
    case Agent.start_link(fun, name: name) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> Agent.update(name, fn _ -> fun.() end)
    end
  end

  defp checkout_sandbox do
    case Ecto.Adapters.SQL.Sandbox.checkout(Repo) do
      :ok -> Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
      {:already, :owner} -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp ensure_repo_started do
    case Repo.start_link() do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end
end
