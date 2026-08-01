defmodule Continuum.DocumentationExamplesTest do
  use ExUnit.Case, async: false

  defmodule DocumentedSignalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      case await(signal(:fraud_review)) do
        :approved -> {:ok, :shipped}
        :rejected -> {:error, :rejected}
      end
    end
  end

  test "documented await signal branches receive the payload directly" do
    for {payload, expected} <- [approved: {:ok, :shipped}, rejected: {:error, :rejected}] do
      Continuum.Test.reset_in_memory!()
      {:ok, run_id} = Continuum.Test.start_synchronous(DocumentedSignalFlow, %{})

      assert :ok = Continuum.signal(run_id, :fraud_review, payload)
      assert {:ok, %{state: :completed, result: ^expected}} = Continuum.await(run_id, 1_000)
    end
  end
end
