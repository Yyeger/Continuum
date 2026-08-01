defmodule Continuum.PrunerTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityResult, Event, Run, RunIngressKey, Signal}

  defmodule PruneFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: input
  end

  setup do
    previous_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)

    Repo.delete_all(ActivityResult)
    Repo.delete_all(RunIngressKey)
    Repo.delete_all(Signal)
    Repo.delete_all(Event)
    Repo.delete_all(Run)

    on_exit(fn -> Mix.shell(previous_shell) end)
    :ok
  end

  test "dry-run planning is bounded and does not mutate histories" do
    first = insert_expired_run()
    _second = insert_expired_run()

    assert {:ok, %{chain_count: 1, run_count: 1}} =
             Continuum.Pruner.plan(repo: Repo, batch_size: 1)

    Mix.Task.rerun("continuum.runs.prune", [
      "--repo",
      "Continuum.Test.Repo",
      "--batch-size",
      "1"
    ])

    assert_received {:mix_shell, :info, [summary]}
    assert summary =~ "Would prune 1 logical chains / 1 runs"
    assert Repo.get(Run, first)
  end

  test "default pruning removes history but preserves idempotency records" do
    run_id = insert_expired_run(idempotency_key: "request-keep")
    insert_history(run_id)
    insert_idempotency(run_id, "request-keep")

    assert {:ok, plan} = Continuum.Pruner.plan(repo: Repo)
    assert plan.run_ids == [run_id]
    assert plan.dependent_counts.events == 1
    assert plan.dependent_counts.activity_results == 1
    assert plan.dependent_counts.ingress_keys == 1

    assert {:ok, %{deleted_run_count: 1, deleted_idempotency_count: 0}} =
             Continuum.Pruner.execute(plan, repo: Repo)

    refute Repo.get(Run, run_id)
    assert Repo.aggregate(Event, :count) == 0
    assert Repo.aggregate(Signal, :count) == 0
    assert Repo.aggregate(ActivityResult, :count) == 1
    assert Repo.aggregate(RunIngressKey, :count) == 1

    assert {:ok, ^run_id, :existing} =
             Continuum.start_unique(PruneFlow, %{retry: true},
               journal: Postgres,
               idempotency_key: "request-keep"
             )
  end

  test "explicit delete policy removes independent idempotency records" do
    run_id = insert_expired_run(idempotency_key: "request-delete")
    insert_idempotency(run_id, "request-delete")

    assert {:ok, plan} =
             Continuum.Pruner.plan(repo: Repo, idempotency_policy: :delete)

    assert {:ok, %{deleted_run_count: 1, deleted_idempotency_count: 2}} =
             Continuum.Pruner.execute(plan, repo: Repo)

    assert Repo.aggregate(ActivityResult, :count) == 0
    assert Repo.aggregate(RunIngressKey, :count) == 0

    assert :ok =
             Postgres.start_run(Instance.default(), Ecto.UUID.generate(), PruneFlow, %{},
               idempotency_key: "request-delete"
             )
  end

  test "active and unexpired runs are skipped while child chains prune before parents" do
    _active = insert_expired_run(state: "suspended", completed_at: nil)

    _unexpired =
      insert_expired_run(retention_until: DateTime.utc_now() |> DateTime.add(1, :day))

    parent = insert_expired_run()
    child = insert_expired_run(parent_run_id: parent)

    assert {:ok, %{chain_count: 1, run_ids: [^child]} = child_plan} =
             Continuum.Pruner.plan(repo: Repo)

    assert {:ok, %{deleted_run_count: 1}} = Continuum.Pruner.execute(child_plan, repo: Repo)

    assert {:ok, %{chain_count: 1, run_ids: [^parent]}} =
             Continuum.Pruner.plan(repo: Repo)
  end

  test "all incarnations in a continued chain prune atomically" do
    root = Ecto.UUID.generate()
    first = insert_expired_run(id: root, correlation_id: root)

    second =
      insert_expired_run(
        correlation_id: root,
        continued_from_run_id: root
      )

    assert {:ok, %{chains: [%{logical_id: ^root, run_ids: run_ids}]} = plan} =
             Continuum.Pruner.plan(repo: Repo)

    assert run_ids == [first, second]
    assert {:ok, %{deleted_run_count: 2}} = Continuum.Pruner.execute(plan, repo: Repo)
    assert Repo.aggregate(Run, :count) == 0
  end

  defp insert_expired_run(opts \\ []) do
    run_id = Keyword.get(opts, :id, Ecto.UUID.generate())
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    metadata = PruneFlow.__continuum_workflow__()

    %Run{}
    |> Ecto.Changeset.change(%{
      id: run_id,
      workflow: inspect(PruneFlow),
      version_hash: metadata.version_hash,
      namespace: "default",
      idempotency_key: Keyword.get(opts, :idempotency_key),
      state: Keyword.get(opts, :state, "completed"),
      input: :erlang.term_to_binary(%{}),
      result: :erlang.term_to_binary(:ok),
      started_at: DateTime.add(now, -100, :day),
      completed_at: Keyword.get(opts, :completed_at, DateTime.add(now, -90, :day)),
      retention_until: Keyword.get(opts, :retention_until, DateTime.add(now, -60, :day)),
      correlation_id: Keyword.get(opts, :correlation_id, run_id),
      continued_from_run_id: Keyword.get(opts, :continued_from_run_id),
      parent_run_id: Keyword.get(opts, :parent_run_id)
    })
    |> Repo.insert!()

    run_id
  end

  defp insert_history(run_id) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %Event{}
    |> Ecto.Changeset.change(%{
      run_id: run_id,
      seq: 0,
      event_type: "side_effect",
      payload: :erlang.term_to_binary(%{type: :side_effect}),
      inserted_at: now
    })
    |> Repo.insert!()

    %Signal{}
    |> Ecto.Changeset.change(%{
      run_id: run_id,
      correlation_id: run_id,
      name: "done",
      payload: :erlang.term_to_binary(:ok),
      delivered: true,
      inserted_at: now
    })
    |> Repo.insert!()
  end

  defp insert_idempotency(run_id, key) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %ActivityResult{}
    |> Ecto.Changeset.change(%{
      activity_module: inspect(__MODULE__),
      idempotency_key: key,
      run_id: run_id,
      seq: 0,
      result: :erlang.term_to_binary(:ok),
      completed_at: now
    })
    |> Repo.insert!()

    %RunIngressKey{}
    |> Ecto.Changeset.change(%{
      namespace: "default",
      workflow: inspect(PruneFlow),
      idempotency_key: key,
      run_id: run_id,
      created_at: now
    })
    |> Repo.insert!()
  end
end
