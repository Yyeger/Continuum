defmodule Continuum.SetAttributesTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run}

  defmodule AttrFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, input.id}
  end

  setup do
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "external attribute updates are queryable and not journaled" do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(
        Continuum.Runtime.Instance.default(),
        run_id,
        AttrFlow,
        %{id: 1},
        attributes: %{region: "eu"}
      )

    assert :ok = Continuum.set_attributes(run_id, %{region: "us", plan: "pro"})

    assert {:ok, page} = Continuum.query(where: [{:eq, [:attributes, "plan"], "pro"}])
    assert [%{run_id: ^run_id, attributes: %{"region" => "us", "plan" => "pro"}}] = page.entries
    assert Repo.aggregate(Event, :count) == 0
  end

  test "concurrent disjoint merges both survive (atomic SQL merge)" do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(
        Continuum.Runtime.Instance.default(),
        run_id,
        AttrFlow,
        %{id: 2}
      )

    parent = self()

    [{:ok, :ok}, {:ok, :ok}] =
      [%{a: 1}, %{b: 2}]
      |> Enum.map(fn attrs ->
        Task.async(fn ->
          Ecto.Adapters.SQL.Sandbox.allow(Repo, parent, self())
          Continuum.set_attributes(run_id, attrs)
        end)
      end)
      |> Task.yield_many(5_000)
      |> Enum.map(fn {_task, result} -> result end)

    run = Repo.get!(Run, run_id)
    assert run.attributes == %{"a" => 1, "b" => 2}
  end

  test "set_attributes on a missing run returns not_found" do
    assert {:error, :not_found} = Continuum.set_attributes(Ecto.UUID.generate(), %{a: 1})
  end
end
