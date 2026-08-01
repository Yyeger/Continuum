defmodule Continuum.NamespacePreconditionTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.{InMemory, Postgres}
  alias Continuum.Schema.{Event, Run, Signal}

  defmodule ApprovalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      approved = await(signal(:approved))
      {:ok, approved}
    end
  end

  setup do
    Repo.delete_all(Signal)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    Continuum.Test.reset_in_memory!()
    :ok
  end

  test "guards every Postgres run-id operation without mutation" do
    {:ok, run_id} =
      Continuum.start(ApprovalFlow, %{}, journal: Postgres, namespace: "tenant-a")

    assert_eventually(fn -> Repo.get!(Run, run_id).state == "suspended" end)

    assert {:error, :not_found} = Continuum.get_run(run_id, namespace: "tenant-b")

    assert {:error, :not_found} =
             Continuum.set_attributes(run_id, %{crossed: true}, namespace: "tenant-b")

    assert {:error, :not_found} =
             Continuum.signal(run_id, :approved, %{by: "wrong"},
               journal: Postgres,
               namespace: "tenant-b"
             )

    assert {:error, :not_found} =
             Continuum.cancel(run_id, journal: Postgres, namespace: "tenant-b")

    assert {:error, :not_found} =
             Continuum.await(run_id, 0, journal: Postgres, namespace: "tenant-b")

    persisted = Repo.get!(Run, run_id)
    assert persisted.state == "suspended"
    assert persisted.attributes == %{}
    assert Repo.aggregate(Signal, :count) == 0

    assert :ok =
             Continuum.set_attributes(run_id, %{region: "eu"}, namespace: "tenant-a")

    assert {:ok, %{namespace: "tenant-a", attributes: %{"region" => "eu"}}} =
             Continuum.get_run(run_id, namespace: "tenant-a")

    assert :ok =
             Continuum.signal(run_id, :approved, %{by: "owner"},
               journal: Postgres,
               namespace: "tenant-a"
             )

    assert {:ok, %{state: :completed, result: {:ok, %{by: "owner"}}}} =
             Continuum.await(run_id, 1_000, journal: Postgres, namespace: "tenant-a")
  end

  test "same-namespace cancellation succeeds" do
    {:ok, run_id} =
      Continuum.start(ApprovalFlow, %{}, journal: Postgres, namespace: "tenant-a")

    assert_eventually(fn -> Repo.get!(Run, run_id).state == "suspended" end)

    assert :ok = Continuum.cancel(run_id, journal: Postgres, namespace: "tenant-a")
    assert Repo.get!(Run, run_id).state == "cancelled"
  end

  test "guards in-memory signal and await operations" do
    {:ok, run_id} =
      Continuum.Test.start_synchronous(ApprovalFlow, %{}, namespace: "tenant-a")

    assert {:error, :not_found} =
             Continuum.signal(run_id, :approved, :wrong,
               journal: InMemory,
               namespace: "tenant-b"
             )

    assert {:error, :not_found} =
             Continuum.await(run_id, 0, journal: InMemory, namespace: "tenant-b")

    assert :ok =
             Continuum.signal(run_id, :approved, :right,
               journal: InMemory,
               namespace: "tenant-a"
             )

    assert {:ok, %{state: :completed, result: {:ok, :right}}} =
             Continuum.await(run_id, 1_000, journal: InMemory, namespace: "tenant-a")
  end

  test "rejects invalid namespace preconditions" do
    run_id = Ecto.UUID.generate()

    assert {:error, {:invalid_namespace, nil}} = Continuum.get_run(run_id, namespace: nil)

    assert {:error, {:invalid_namespace, :tenant}} =
             Continuum.await(run_id, 0, namespace: :tenant)
  end

  defp assert_eventually(fun, attempts \\ 100)

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
