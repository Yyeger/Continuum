defmodule Continuum.TestHelpersTest do
  use ExUnit.Case, async: false

  defmodule GoldenFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      n = Continuum.side_effect(fn -> input.seed * 2 end)
      {:ok, n + 1}
    end
  end

  defmodule TimerFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      timer(input.ms)
      {:ok, :fired}
    end
  end

  defmodule SignalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      {:ok, await(signal(:decision))}
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

  test "fires in-memory timers injected through test helpers" do
    Continuum.Test.reset_in_memory!()

    {:ok, run_id} = Continuum.Test.start_in_memory(TimerFlow, %{ms: 60_000})

    assert_eventually(fn ->
      match?([%{type: :timer_started}], Continuum.Test.history(run_id))
    end)

    assert {:error, :timeout} = Continuum.await(run_id, 25)
    assert :ok = Continuum.Test.fire_timer(run_id)

    assert {:ok, %{state: :completed, result: {:ok, :fired}}} =
             Continuum.await(run_id, 1_000)
  end

  test "injects in-memory signals without using an engine signal cast" do
    Continuum.Test.reset_in_memory!()

    refute function_exported?(Continuum.Runtime.Engine, :deliver_signal, 3)

    {:ok, run_id} = Continuum.Test.start_in_memory(SignalFlow, %{})
    assert {:error, :timeout} = Continuum.await(run_id, 25)
    assert :ok = Continuum.Test.inject_signal(run_id, :decision, :go)

    assert {:ok, %{state: :completed, result: {:ok, :go}}} =
             Continuum.await(run_id, 1_000)
  end

  defp assert_eventually(fun, attempts \\ 20)

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
