defmodule Continuum.Runtime.JournalContractTest do
  use ExUnit.Case, async: false

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.{InMemory, Postgres}
  alias Continuum.Test.Repo

  defmodule ContractFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, input}
  end

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    InMemory.reset()
    :ok
  end

  for adapter <- [InMemory, Postgres] do
    @adapter adapter

    test "#{inspect(adapter)} rejects a reused run ID without replacing the run" do
      instance = Instance.default()
      run_id = Ecto.UUID.generate()

      assert :ok = @adapter.start_run(instance, run_id, ContractFlow, %{attempt: 1})
      assert {:error, _reason} = @adapter.start_run(instance, run_id, ContractFlow, %{attempt: 2})

      assert %{input: %{attempt: 1}} = @adapter.get_run(instance, run_id)
    end
  end
end
