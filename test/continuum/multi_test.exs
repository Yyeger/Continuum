defmodule Continuum.MultiTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{Dispatcher, Instance}
  alias Continuum.Schema.{Run, RunIngressKey}

  defmodule Flow do
    use Continuum.Workflow
    def run(input), do: input
  end

  setup do
    previous = Application.get_env(:continuum, :journal)
    Application.put_env(:continuum, :journal, Continuum.Runtime.Journal.Postgres)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:continuum, :journal, previous),
        else: Application.delete_env(:continuum, :journal)
    end)

    # A stand-in application table lets us verify atomic business writes without
    # adding a production schema solely for tests. Sandbox rollback removes it.
    Repo.query!("CREATE TABLE continuum_multi_business (id text PRIMARY KEY)")
    :ok
  end

  test "business writes and enqueue commit together, with no engine started by Multi" do
    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:business, fn repo, _ ->
        repo.query!("INSERT INTO continuum_multi_business VALUES ('order-1')")
        {:ok, "order-1"}
      end)
      |> Continuum.Multi.enqueue(:workflow, Flow, fn %{business: id} -> %{order: id} end,
        namespace: "orders",
        attributes: %{customer: "42"},
        trace_context: "trace"
      )

    assert {:ok, %{workflow: %{run_id: id, status: :enqueued}}} = Repo.transaction(multi)
    run = Repo.get!(Run, id)
    assert run.namespace == "orders"
    assert run.attributes == %{"customer" => "42"}
    assert run.trace_context == "trace"
    assert run.correlation_id == id
    assert run.version_hash == Flow.__continuum_workflow__().version_hash
    assert is_nil(run.lease_owner)
    assert is_nil(run.lease_token)
    assert Registry.lookup(Instance.default().registry, id) == []

    assert {:ok, 1} = Dispatcher.dispatch_once()
    assert {:ok, %{result: %{order: "order-1"}}} = Continuum.await(id, 1_000)
  end

  test "later failure rolls back business, run, and reserved idempotency key" do
    id = Ecto.UUID.generate()

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:business, fn repo, _ ->
        repo.query!("INSERT INTO continuum_multi_business VALUES ('rollback')")
        {:ok, :written}
      end)
      |> Continuum.Multi.enqueue(:workflow, Flow, :input,
        run_id: id,
        idempotency_key: "rollback"
      )
      |> Ecto.Multi.error(:later, :reject)

    assert {:error, :later, :reject, _} = Repo.transaction(multi)
    assert Repo.query!("SELECT * FROM continuum_multi_business").rows == []
    refute Repo.get(Run, id)
    refute Repo.exists?(from(k in RunIngressKey, where: k.run_id == ^id))
  end

  test "duplicates return a stable root without aborting the surrounding transaction" do
    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.put(:key, "same")
      |> Continuum.Multi.enqueue(:first, Flow, 1, fn %{key: key} -> [idempotency_key: key] end)
      |> Continuum.Multi.enqueue(:second, Flow, 2, idempotency_key: "same")
      |> Ecto.Multi.run(:after_duplicate, fn repo, _ ->
        repo.query!("INSERT INTO continuum_multi_business VALUES ('still-valid')")
        {:ok, :written}
      end)

    assert {:ok, %{first: %{run_id: id, status: :enqueued}, second: existing}} =
             Repo.transaction(multi)

    assert existing == %{run_id: id, status: :existing}
    assert Continuum.DurableTerm.decode!(Repo.get!(Run, id).input) == 1
    assert Repo.aggregate(Run, :count) == 1
    assert Repo.query!("SELECT * FROM continuum_multi_business").rows == [["still-valid"]]
  end

  test "rejects a different repo or a nondurable instance before inserting" do
    for {instance, reason} <- [
          {%{Instance.default() | repo: WrongRepo}, :repo_mismatch},
          {%{Instance.default() | journal: Continuum.Runtime.Journal.InMemory},
           :postgres_journal_required}
        ] do
      multi = Continuum.Multi.enqueue(Ecto.Multi.new(), :workflow, Flow, 1, instance: instance)
      assert {:error, :workflow, ^reason, %{}} = Repo.transaction(multi)
    end

    assert Repo.aggregate(Run, :count) == 0
  end
end

defmodule Continuum.MultiCommitTest do
  use ExUnit.Case, async: false

  import Ecto.Query
  alias Continuum.Runtime.{Dispatcher, Instance}
  alias Continuum.Schema.Run
  alias Continuum.Test.Repo
  alias Ecto.Adapters.SQL.Sandbox

  setup do
    previous = Application.get_env(:continuum, :journal)
    Application.put_env(:continuum, :journal, Continuum.Runtime.Journal.Postgres)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:continuum, :journal, previous),
        else: Application.delete_env(:continuum, :journal)
    end)

    :ok
  end

  test "a separate connection cannot see enqueue before commit, then dispatch survives producer death" do
    id = Ecto.UUID.generate()
    parent = self()

    producer =
      spawn_monitor(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          multi =
            Ecto.Multi.new()
            |> Continuum.Multi.enqueue(:workflow, Continuum.MultiTest.Flow, :committed,
              run_id: id
            )
            |> Ecto.Multi.run(:wait, fn _, _ ->
              send(parent, {:ready, self()})

              receive do
                :commit -> {:ok, :done}
              after
                5_000 -> {:error, :timeout}
              end
            end)

          send(parent, {:committed, Repo.transaction(multi)})
        end)
      end)

    {pid, ref} = producer
    assert_receive {:ready, ^pid}, 1_000

    Sandbox.unboxed_run(Repo, fn ->
      refute Repo.get(Run, id)
      assert Registry.lookup(Instance.default().registry, id) == []
    end)

    send(pid, :commit)
    assert_receive {:committed, {:ok, _}}, 1_000
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    Sandbox.unboxed_run(Repo, fn ->
      # Share this real (non-sandboxed) connection with the resumed engine.
      Sandbox.mode(Repo, {:shared, self()})

      try do
        assert Repo.get!(Run, id).state == "running"
        assert {:ok, _} = Dispatcher.dispatch_once()
        assert {:ok, %{result: :committed}} = Continuum.await(id, 1_000)
      after
        Repo.delete_all(from(r in Run, where: r.id == ^id))
        Sandbox.mode(Repo, :manual)
      end
    end)
  end
end
