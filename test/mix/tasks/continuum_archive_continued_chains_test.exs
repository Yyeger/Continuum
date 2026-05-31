defmodule Mix.Tasks.Continuum.ArchiveContinuedChainsTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Schema.{ActivityResult, ActivityTask, Event, Run, Signal, Snapshot, Timer}

  setup do
    previous_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)

    Repo.delete_all(ActivityResult)
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Timer)
    Repo.delete_all(Signal)
    Repo.delete_all(Snapshot)
    Repo.delete_all(Event)
    Repo.delete_all(Run)

    on_exit(fn -> Mix.shell(previous_shell) end)
  end

  test "dry-run reports old non-tail cycles without deleting them" do
    [run1, run2, run3] = insert_chain(3)
    insert_dependent_rows(run1)
    insert_dependent_rows(run2)
    insert_dependent_rows(run3)

    Mix.Task.rerun("continuum.archive_continued_chains", [
      "--repo",
      "Continuum.Test.Repo",
      "--older-than",
      "30d"
    ])

    assert_received {:mix_shell, :info, ["Would delete 2 continued-chain runs"]}
    assert_received {:mix_shell, :info, ["Would delete continuum_events rows: 2"]}
    assert Repo.aggregate(Run, :count) == 3
    assert Repo.aggregate(Event, :count) == 3
  end

  test "execute deletes non-tail cycles and dependent rows while keeping the tail" do
    [run1, run2, run3] = insert_chain(3)
    insert_dependent_rows(run1)
    insert_dependent_rows(run2)
    insert_dependent_rows(run3)

    Mix.Task.rerun("continuum.archive_continued_chains", [
      "--repo",
      "Continuum.Test.Repo",
      "--older-than",
      "30d",
      "--execute"
    ])

    assert_received {:mix_shell, :info, ["Deleted 2 continuum_runs rows"]}

    refute Repo.get(Run, run1)
    refute Repo.get(Run, run2)
    assert Repo.get(Run, run3)

    assert only_run_ids(Event) == [run3]
    assert only_run_ids(Snapshot) == [run3]
    assert only_run_ids(Timer) == [run3]
    assert only_run_ids(Signal) == [run3]
    assert only_run_ids(ActivityTask) == [run3]
    assert only_run_ids(ActivityResult) == [run3]
  end

  test "live parent and future retention block deletion" do
    parent_id = insert_run(state: "running")
    [child1, _child2] = insert_chain(2, parent_run_id: parent_id)

    future_root = Ecto.UUID.generate()
    future_tail = Ecto.UUID.generate()

    insert_run(
      id: future_root,
      correlation_id: future_root,
      retention_until: DateTime.utc_now() |> DateTime.add(30, :day)
    )

    insert_run(id: future_tail, correlation_id: future_root, continued_from_run_id: future_root)

    Mix.Task.rerun("continuum.archive_continued_chains", [
      "--repo",
      "Continuum.Test.Repo",
      "--older-than",
      "30d",
      "--execute"
    ])

    assert_received {:mix_shell, :info, ["Deleted 0 continuum_runs rows"]}
    assert Repo.get(Run, child1)
    assert Repo.get(Run, future_root)
  end

  defp insert_chain(count, opts \\ []) do
    root = Ecto.UUID.generate()

    1..count
    |> Enum.reduce([], fn _index, acc ->
      run_id = Ecto.UUID.generate()
      predecessor = List.last(acc)

      insert_run(
        Keyword.merge(opts,
          id: run_id,
          correlation_id: root,
          continued_from_run_id: predecessor
        )
      )

      acc ++ [run_id]
    end)
  end

  defp insert_run(opts) do
    run_id = Keyword.get(opts, :id, Ecto.UUID.generate())
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %Run{}
    |> Ecto.Changeset.change(%{
      id: run_id,
      workflow: inspect(__MODULE__),
      version_hash: "archive-test",
      state: Keyword.get(opts, :state, "completed"),
      input: :erlang.term_to_binary(%{}),
      result: :erlang.term_to_binary({:continued, "next"}),
      completed_at: Keyword.get(opts, :completed_at, DateTime.add(now, -90, :day)),
      retention_until: Keyword.get(opts, :retention_until, DateTime.add(now, -60, :day)),
      parent_run_id: Keyword.get(opts, :parent_run_id),
      correlation_id: Keyword.get(opts, :correlation_id, run_id),
      continued_from_run_id: Keyword.get(opts, :continued_from_run_id)
    })
    |> Repo.insert!()

    run_id
  end

  defp insert_dependent_rows(run_id) do
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

    %Snapshot{}
    |> Ecto.Changeset.change(%{
      run_id: run_id,
      through_seq: 0,
      version_hash: "archive-test",
      format_version: 1,
      payload: :erlang.term_to_binary(%{}),
      taken_at: now
    })
    |> Repo.insert!()

    %Timer{}
    |> Ecto.Changeset.change(%{
      id: Ecto.UUID.generate(),
      run_id: run_id,
      fires_at: now,
      fired: true
    })
    |> Repo.insert!()

    %Signal{}
    |> Ecto.Changeset.change(%{
      run_id: run_id,
      name: "archive",
      payload: :erlang.term_to_binary(:ok),
      delivered: true,
      inserted_at: now
    })
    |> Repo.insert!()

    %ActivityTask{}
    |> Ecto.Changeset.change(%{
      id: Ecto.UUID.generate(),
      run_id: run_id,
      seq: 0,
      mfa: :erlang.term_to_binary({__MODULE__, :activity, []}),
      state: "completed",
      scheduled_at: now,
      available_at: now
    })
    |> Repo.insert!()

    %ActivityResult{}
    |> Ecto.Changeset.change(%{
      activity_module: inspect(__MODULE__),
      idempotency_key: run_id,
      run_id: run_id,
      seq: 0,
      result: :erlang.term_to_binary(:ok),
      completed_at: now
    })
    |> Repo.insert!()
  end

  defp only_run_ids(schema) do
    Repo.all(from(row in schema, select: row.run_id, order_by: row.run_id))
  end
end
