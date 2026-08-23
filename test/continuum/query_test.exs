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

  test "state filters compare the state column alone" do
    cancelled = insert_run!(%{value: 1}, %{})
    failed = insert_run!(%{value: 2}, %{})

    # A canonical cancel keeps its error payload for context; a real failure
    # carries an unrelated error term.
    Repo.update_all(
      from(r in Run, where: r.id == ^cancelled),
      set: [state: "cancelled", error: :erlang.term_to_binary(:cancelled)]
    )

    Repo.update_all(
      from(r in Run, where: r.id == ^failed),
      set: [state: "failed", error: :erlang.term_to_binary(:boom)]
    )

    assert {:ok, %{entries: [%{run_id: ^cancelled, state: :cancelled}]}} =
             Continuum.list_runs(where: [{:eq, :state, :cancelled}])

    assert {:ok, %{entries: [%{run_id: ^failed, state: :failed}]}} =
             Continuum.list_runs(where: [{:eq, :state, :failed}])
  end

  test "the legacy cancel promotion moves failed + :cancelled rows into the cancelled filter" do
    legacy = insert_run!(%{value: 1}, %{})

    Repo.update_all(
      from(r in Run, where: r.id == ^legacy),
      set: [state: "failed", error: :erlang.term_to_binary(:cancelled)]
    )

    # Pre-migration shape: the row is a failure as far as the state column goes,
    # and only the decoded payload says otherwise.
    assert {:ok, %{entries: []}} = Continuum.list_runs(where: [{:eq, :state, :cancelled}])

    Repo.query!(
      "UPDATE continuum_runs SET state = 'cancelled' WHERE state = 'failed' AND error = $1",
      [:erlang.term_to_binary(:cancelled)]
    )

    assert {:ok, %{entries: [%{run_id: ^legacy, state: :cancelled}]}} =
             Continuum.list_runs(where: [{:eq, :state, :cancelled}])

    assert {:ok, %{entries: []}} = Continuum.list_runs(where: [{:eq, :state, :failed}])
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
             Continuum.list_runs(
               where: [
                 {:eq, [:attributes, "region"], "eu"},
                 {:gte, :started_at, ~U[2026-06-01 00:00:00Z]}
               ],
               order_by: {:asc, :started_at}
             )

    assert %Continuum.Page{} = page
    assert [%Continuum.Run{}] = page.entries

    assert [%{run_id: ^older, state: :suspended, attributes: %{"region" => "eu"}}] = page.entries

    assert {:ok, page} = Continuum.list_runs(where: [{:in, :state, ["completed", "suspended"]}])
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

    assert {:ok, first} = Continuum.list_runs(order_by: {:asc, :started_at}, per_page: 2)
    assert [_, _] = first.entries
    assert is_binary(first.next_cursor)

    assert {:ok, second} =
             Continuum.list_runs(
               order_by: {:asc, :started_at},
               per_page: 2,
               cursor: first.next_cursor
             )

    paged_ids = Enum.map(first.entries ++ second.entries, & &1.run_id)
    assert paged_ids == Enum.sort(ids)
    assert second.next_cursor == nil
    assert {:error, :invalid_cursor} = Continuum.list_runs(cursor: "forged")
  end

  test "attribute equality keeps JSON types and uses the GIN containment index" do
    number_id = insert_run!(%{}, %{"customer_tier" => 4})
    string_id = insert_run!(%{}, %{"customer_tier" => "4"})

    assert {:ok, %{entries: [%{run_id: ^number_id}]}} =
             Continuum.list_runs(where: [{:eq, [:attributes, :customer_tier], 4}])

    assert {:ok, %{entries: [%{run_id: ^string_id}]}} =
             Continuum.list_runs(where: [{:eq, [:attributes, :customer_tier], "4"}])

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
             Continuum.list_runs(where: [{:eq, [:attributes, :customer, :profile, :tier], 4}])

    assert {:ok, %{entries: [%{run_id: ^string_id}]}} =
             Continuum.list_runs(where: [{:eq, [:attributes, :customer, :profile, :tier], "4"}])

    assert {:ok, %{entries: [%{run_id: ^string_id}]}} =
             Continuum.list_runs(where: [{:neq, [:attributes, :customer, :profile, :tier], 4}])
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
             Continuum.list_runs(where: [{:eq, [:attributes, :customer_tier], 4}])

    assert [%{run_id: ^run_id}] = page.entries
    assert Repo.aggregate(Event, :count) == 0
  end

  test "rejects invalid query fields and non JSON attributes" do
    assert {:error, {:invalid_field, :missing}} =
             Continuum.list_runs(where: [{:eq, :missing, "x"}])

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

  test "the deprecated query/1,2 names still delegate to list_runs" do
    # `query/1,2` are reserved for a per-run named read (v0.10), which could
    # only be told apart from a paginated row search by a first-argument guard
    # once the API freezes. They stay as deprecated delegates for one release.
    # Called through `apply/3` so this test does not emit the deprecation
    # warning it exists to prove is there.
    assert apply(Continuum, :query, [[per_page: 1]]) == Continuum.list_runs(per_page: 1)

    assert apply(Continuum, :query, [Continuum, [per_page: 1]]) ==
             Continuum.list_runs(Continuum, per_page: 1)

    assert {:docs_v1, _, _, _, _, _, docs} = Code.fetch_docs(Continuum)

    for arity <- [1, 2] do
      assert Enum.any?(docs, fn
               {{:function, :query, ^arity}, _line, _sig, _doc, meta} ->
                 meta[:deprecated] =~ "list_runs"

               _entry ->
                 false
             end)
    end
  end
end
