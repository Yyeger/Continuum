defmodule Continuum.ReplayPropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  defmodule SideEffectChainFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      result =
        Enum.reduce(input.steps, 0, fn step, acc ->
          Continuum.side_effect(fn -> acc + step end)
        end)

      {:ok, result}
    end
  end

  property "side-effect histories replay to the same result" do
    check all(steps <- list_of(integer(-100..100), max_length: 20), max_runs: 50) do
      Continuum.Test.reset_in_memory!()

      {:ok, run_id} = Continuum.Test.start_synchronous(SideEffectChainFlow, %{steps: steps})
      expected = {:ok, Enum.sum(steps)}

      assert {:ok, %{state: :completed, result: ^expected}} = Continuum.await(run_id, 1_000)

      history = Continuum.Test.history(run_id)
      assert length(history) == length(steps)

      assert Continuum.Test.assert_replays(SideEffectChainFlow, %{steps: steps}, history) ==
               expected
    end
  end
end
