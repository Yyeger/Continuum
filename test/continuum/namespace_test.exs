defmodule Continuum.NamespaceTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run}

  defmodule NamespaceFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, input.value}
  end

  setup do
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "start tags rows and query/list isolate namespaces" do
    default_id = start_run!("default", 1)
    tenant_id = start_run!("tenant-a", 2)

    assert Repo.one!(from(r in Run, where: r.id == ^default_id)).namespace == "default"
    assert Repo.one!(from(r in Run, where: r.id == ^tenant_id)).namespace == "tenant-a"

    assert {:ok, %{entries: default_entries}} = Continuum.query()
    assert Enum.map(default_entries, & &1.run_id) == [default_id]

    assert {:ok, %{entries: tenant_entries}} = Continuum.query(namespace: "tenant-a")
    assert Enum.map(tenant_entries, & &1.run_id) == [tenant_id]

    assert {:ok, %{entries: observer_entries}} =
             Continuum.Observer.list_runs(namespace: "tenant-a")

    assert Enum.map(observer_entries, & &1.run_id) == [tenant_id]
  end

  test "public start forwards namespace and attributes to durable runs" do
    {:ok, run_id} =
      Continuum.start(NamespaceFlow, %{value: 7},
        journal: Postgres,
        namespace: "tenant-public",
        attributes: %{region: "eu", tier: "gold"}
      )

    assert {:ok, %{state: :completed, result: {:ok, 7}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    run = Repo.one!(from(r in Run, where: r.id == ^run_id))

    assert run.namespace == "tenant-public"
    assert run.attributes == %{"region" => "eu", "tier" => "gold"}
  end

  test "run-id keyed operations do not require namespace" do
    run_id = start_run!("tenant-a", 5)

    assert {:ok, run} = Continuum.get_run(run_id)
    assert run.namespace == "tenant-a"

    assert :ok = Continuum.cancel(run_id, journal: Postgres)

    assert {:error, %{state: :failed, error: :cancelled}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  defp start_run!(namespace, value) do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(
        Continuum.Runtime.Instance.default(),
        run_id,
        NamespaceFlow,
        %{value: value},
        namespace: namespace
      )

    run_id
  end
end
