defmodule Continuum.QueryTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run}

  defmodule QueryFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      Continuum.side_effect(fn -> {:ok, input.value} end)
    end
  end

  setup do
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "queries by state, timestamps, ordering, and JSONB attributes" do
    older = insert_run!(%{value: 1}, %{region: "eu", customer_tier: 2})
    newer = insert_run!(%{value: 2}, %{region: "us", customer_tier: 3})

    Repo.update_all(
      from(r in Run, where: r.id == ^older),
      set: [started_at: ~U[2026-06-01 00:00:00Z], state: "suspended"]
    )

    Repo.update_all(
      from(r in Run, where: r.id == ^newer),
      set: [started_at: ~U[2026-06-02 00:00:00Z], state: "completed"]
    )

    assert {:ok, page} =
             Continuum.query(
               where: [
                 {:eq, [:attributes, "region"], "eu"},
                 {:gte, :started_at, ~U[2026-06-01 00:00:00Z]}
               ],
               order_by: {:asc, :started_at}
             )

    assert page.total == 1
    assert [%{run_id: ^older, state: :suspended, attributes: %{"region" => "eu"}}] = page.entries

    assert {:ok, page} = Continuum.query(where: [{:in, :state, ["completed", "suspended"]}])
    assert page.total == 2
  end

  test "caps pagination and preserves observer search behavior" do
    run_id = insert_run!(%{value: 9}, %{region: "eu"})

    assert {:ok, page} = Continuum.Query.list(search: run_id, per_page: 1_000)
    assert page.per_page == 100
    assert [%{run_id: ^run_id}] = page.entries

    assert {:ok, observer_page} = Continuum.Observer.list_runs(search: run_id)
    assert [%{run_id: ^run_id}] = observer_page.entries
  end

  test "set_attributes merges metadata without journaling" do
    run_id = insert_run!(%{value: 5}, %{region: "eu"})

    assert :ok = Continuum.set_attributes(run_id, %{customer_tier: 4})

    assert {:ok, run} = Continuum.get_run(run_id)
    assert run.attributes == %{"region" => "eu", "customer_tier" => 4}

    assert {:ok, page} =
             Continuum.query(where: [{:eq, [:attributes, :customer_tier], 4}])

    assert [%{run_id: ^run_id}] = page.entries
    assert Repo.aggregate(Event, :count) == 0
  end

  test "rejects invalid query fields and non JSON attributes" do
    assert {:error, {:invalid_field, :missing}} =
             Continuum.query(where: [{:eq, :missing, "x"}])

    assert {:error, {:invalid_json, _reason}} =
             Continuum.set_attributes(Ecto.UUID.generate(), %{bad: self()})
  end

  defp insert_run!(input, attributes) do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(
        Continuum.Runtime.Instance.default(),
        run_id,
        QueryFlow,
        input,
        attributes: attributes
      )

    run_id
  end
end
