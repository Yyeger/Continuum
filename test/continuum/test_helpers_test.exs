defmodule Continuum.TestHelpersTest do
  use ExUnit.Case, async: false

  defmodule GoldenFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      n = Continuum.side_effect(fn -> input.seed * 2 end)
      {:ok, n + 1}
    end
  end

  test "loads history and asserts golden replay" do
    Continuum.Test.reset_in_memory!()

    {:ok, run_id} = Continuum.Test.start_in_memory(GoldenFlow, %{seed: 10})
    assert {:ok, %{state: :completed, result: {:ok, 21}}} = Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)
    assert Continuum.Test.assert_replays(GoldenFlow, %{seed: 999}, history) == {:ok, 21}

    assert Continuum.Test.assert_replays(GoldenFlow, %{seed: 999}, history, {:ok, 21}) ==
             {:ok, 21}
  end

  test "dumps and reloads golden history files" do
    Continuum.Test.reset_in_memory!()

    {:ok, run_id} = Continuum.Test.start_in_memory(GoldenFlow, %{seed: 4})
    assert {:ok, %{state: :completed, result: {:ok, 9}}} = Continuum.await(run_id, 1_000)

    path = Path.join(System.tmp_dir!(), "continuum-golden-#{System.unique_integer()}.journal")
    :ok = Continuum.Test.dump_history!(run_id, path)

    assert Continuum.Test.load_history!(path) == Continuum.Test.history(run_id)
    File.rm(path)
  end

  test "patched? is a v0.1 false stub" do
    refute Continuum.patched?(:future_change)
  end
end
