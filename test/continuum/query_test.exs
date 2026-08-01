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

    assert [%{run_id: ^older, state: :suspended, attributes: %{"region" => "eu"}}] = page.entries

    assert {:ok, page} = Continuum.query(where: [{:in, :state, ["completed", "suspended"]}])
    assert Enum.map(page.entries, & &1.run_id) |> Enum.sort() == Enum.sort([older, newer])
  end

  test "caps pagination and preserves observer search behavior" do
    run_id = insert_run!(%{value: 9}, %{region: "eu"})

    assert {:ok, page} = Continuum.Query.list(search: run_id, per_page: 1_000)
    assert page.per_page == 100
    assert [%{run_id: ^run_id}] = page.entries

    assert {:ok, observer_page} = Continuum.Observer.list_runs(search: run_id)
    assert [%{run_id: ^run_id}] = observer_page.entries
  end

  test "uses stable keyset pagination for equal sort values" do
    ids = for value <- 1..3, do: insert_run!(%{value: value}, %{})
    timestamp = ~U[2026-06-01 00:00:00Z]
    Repo.update_all(from(r in Run, where: r.id in ^ids), set: [started_at: timestamp])

    assert {:ok, first} = Continuum.query(order_by: {:asc, :started_at}, per_page: 2)
    assert length(first.entries) == 2
    assert is_binary(first.next_cursor)

    assert {:ok, second} =
             Continuum.query(
               order_by: {:asc, :started_at},
               per_page: 2,
               cursor: first.next_cursor
             )

    paged_ids = Enum.map(first.entries ++ second.entries, & &1.run_id)
    assert paged_ids == Enum.sort(ids)
    assert second.next_cursor == nil
    assert {:error, :invalid_cursor} = Continuum.query(cursor: "forged")
  end

  test "attribute equality keeps JSON types and uses the GIN containment index" do
    number_id = insert_run!(%{}, %{"customer_tier" => 4})
    string_id = insert_run!(%{}, %{"customer_tier" => "4"})

    assert {:ok, %{entries: [%{run_id: ^number_id}]}} =
             Continuum.query(where: [{:eq, [:attributes, :customer_tier], 4}])

    assert {:ok, %{entries: [%{run_id: ^string_id}]}} =
             Continuum.query(where: [{:eq, [:attributes, :customer_tier], "4"}])

    Repo.query!("SET LOCAL enable_seqscan = off")

    plan =
      Repo.query!(
        "EXPLAIN (COSTS OFF) SELECT id FROM continuum_runs WHERE attributes @> $1::jsonb",
        [Jason.encode!(%{"customer_tier" => 4})]
      ).rows
      |> List.flatten()
      |> Enum.join("\n")

    assert plan =~ "continuum_runs_attributes_gin_idx"
  end

  test "filters nested attributes with type-preserving containment" do
    number_id = insert_run!(%{}, %{customer: %{profile: %{tier: 4}}})
    string_id = insert_run!(%{}, %{customer: %{profile: %{tier: "4"}}})

    assert {:ok, %{entries: [%{run_id: ^number_id}]}} =
             Continuum.query(where: [{:eq, [:attributes, :customer, :profile, :tier], 4}])

    assert {:ok, %{entries: [%{run_id: ^string_id}]}} =
             Continuum.query(where: [{:eq, [:attributes, :customer, :profile, :tier], "4"}])

    assert {:ok, %{entries: [%{run_id: ^string_id}]}} =
             Continuum.query(where: [{:neq, [:attributes, :customer, :profile, :tier], 4}])
  end

  test "can omit, cap, and redact decoded run payloads" do
    run_id = insert_run!(%{secret: "token", large: String.duplicate("x", 100)}, %{})

    assert {:ok, %{entries: [%{input: nil}]}} =
             Continuum.Query.list(search: run_id, include_payloads: false)

    assert {:ok, %{input: %{omitted: :payload_too_large, encoded_bytes: bytes}}} =
             Continuum.get_run(run_id, max_payload_bytes: 16)

    assert bytes > 16

    redactor = fn
      %{secret: _secret} = payload -> Map.put(payload, :secret, "[REDACTED]")
      payload -> payload
    end

    assert {:ok, %{input: %{secret: "[REDACTED]"}}} =
             Continuum.get_run(run_id, redactor: redactor)
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
