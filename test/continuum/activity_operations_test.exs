defmodule Continuum.ActivityOperationsTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.ActivityWorker.Dispatcher, as: ActivityDispatcher
  alias Continuum.Runtime.Dispatcher, as: RunDispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Run}

  defmodule RecoverableActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(value) do
      if Agent.get(__MODULE__, & &1), do: raise("dependency unavailable"), else: {:ok, value}
    end
  end

  defmodule RecoverableFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, value} = activity(RecoverableActivity.run(input.value))
      {:ok, value + 1}
    end
  end

  setup do
    start_supervised!(%{
      id: RecoverableActivity,
      start: {Agent, :start_link, [fn -> true end, [name: RecoverableActivity]]}
    })

    :ok
  end

  test "records attempts and resumes a failed run through an audited retry lineage" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(RecoverableFlow, %{value: 41}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = ActivityDispatcher.dispatch_once(owner: "f3-initial", batch_size: 1)
    assert {:error, %{state: :failed}} = Continuum.await(run_id, 1_000, journal: Postgres)

    original = Repo.one!(ActivityTask)
    assert original.state == "discarded"
    assert event_types(run_id) == ["activity_scheduled", "activity_failed"]

    assert {:ok, inspection} = Continuum.ActivityOperations.get(original.id, repo: Repo)
    assert inspection.lineage_id == original.id
    assert [%{task_id: task_id, attempt: 1, outcome: :discarded}] = inspection.attempts
    assert task_id == original.id

    audit_opts = [repo: Repo, operator: "oncall@example", reason: "dependency recovered"]

    assert {:ok, %{status: :planned}} =
             Continuum.ActivityOperations.classify(original.id, :retryable, audit_opts)

    assert {:ok, %{status: :executed}} =
             Continuum.ActivityOperations.classify(
               original.id,
               :retryable,
               Keyword.put(audit_opts, :execute, true)
             )

    assert {:ok, %{status: :executed}} =
             Continuum.ActivityOperations.dead_letter(
               original.id,
               Keyword.put(audit_opts, :execute, true)
             )

    assert Repo.get!(ActivityTask, original.id).state == "dead_lettered"
    Agent.update(RecoverableActivity, fn _ -> false end)

    retry_opts =
      audit_opts ++
        [
          policy: [
            retry: [
              max_attempts: 2,
              backoff: :constant,
              base_ms: 0,
              max_backoff_ms: 0,
              max_retry_horizon_ms: 90_000
            ],
            timeout: 30_000
          ]
        ]

    assert {:ok, %{status: :planned, retry_policy: %{timeout_ms: 30_000}}} =
             Continuum.ActivityOperations.retry(original.id, retry_opts)

    assert {:ok, %{status: :executed, successor_task_id: successor_id}} =
             Continuum.ActivityOperations.retry(
               original.id,
               Keyword.put(retry_opts, :execute, true)
             )

    assert event_types(run_id) == [
             "activity_scheduled",
             "activity_failed",
             "activity_retry_scheduled"
           ]

    assert %ActivityTask{parent_task_id: parent_id, lineage_id: lineage_id, state: "available"} =
             Repo.get!(ActivityTask, successor_id)

    assert parent_id == original.id
    assert lineage_id == original.id
    assert Repo.get!(Run, run_id).state == "suspended"

    assert {:ok, 1} = RunDispatcher.dispatch_once(owner: "f3-run-retry", batch_size: 1)

    assert_eventually(fn ->
      case Repo.get!(Run, run_id) do
        %Run{lease_token: token, state: state}
        when is_integer(token) and state in ["running", "suspended"] ->
          true

        _ ->
          false
      end
    end)

    case ActivityDispatcher.dispatch_once(owner: "f3-retry", batch_size: 1) do
      {:ok, count} when count in [0, 1] -> :ok
    end

    assert {:ok, %{state: :completed, result: {:ok, 42}}} =
             Continuum.await(run_id, 2_000, journal: Postgres)

    assert event_types(run_id) == [
             "activity_scheduled",
             "activity_failed",
             "activity_retry_scheduled",
             "activity_completed"
           ]

    assert {:ok, inspection} = Continuum.ActivityOperations.get(original.id, repo: Repo)
    assert inspection.classification == :retryable
    assert Enum.map(inspection.tasks, & &1.state) == [:dead_lettered, :completed]
    assert Enum.map(inspection.attempts, & &1.outcome) == [:discarded, :completed]

    assert Enum.map(inspection.operations, & &1.action) == [
             :classify,
             :dead_letter,
             :manual_retry
           ]

    assert Enum.all?(inspection.operations, &(&1.operator == "oncall@example"))
    assert Enum.all?(inspection.operations, &(&1.reason == "dependency recovered"))
  end

  test "rejects unaudited or replay-ambiguous retries" do
    assert {:error, :operator_and_reason_required} =
             Continuum.ActivityOperations.retry(Ecto.UUID.generate(), repo: Repo)

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(RecoverableFlow, %{value: 1}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    task = Repo.one!(ActivityTask)

    assert {:error, :not_discarded} =
             Continuum.ActivityOperations.retry(task.id,
               repo: Repo,
               operator: "ops",
               reason: "too early"
             )

    assert Repo.get!(Run, run_id).state in ["running", "suspended"]
  end

  defp event_types(run_id) do
    Repo.all(
      from(e in Continuum.Schema.Event,
        where: e.run_id == ^run_id,
        order_by: e.seq,
        select: e.event_type
      )
    )
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(fun, 0), do: assert(fun.())

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
