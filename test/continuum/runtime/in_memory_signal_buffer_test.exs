defmodule Continuum.Runtime.InMemorySignalBufferTest do
  @moduledoc """
  Audit finding 2.3: in-memory signal delivery buffers per run (mirroring the
  `continuum_signals` mailbox) instead of appending `signal_received` at the
  journal tail. Early and out-of-order signals wait for their matching await;
  before the fix they produced a permanent `ReplayDriftError`.
  """

  use ExUnit.Case, async: false

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.InMemory

  defmodule TwoSignalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      a = await(signal(:a))
      b = await(signal(:b))
      {:ok, {a, b}}
    end
  end

  defmodule TimerThenSignalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      timer(minutes(5))
      payload = await(signal(:go))
      {:ok, payload}
    end
  end

  defmodule SameNameTwiceFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      first = await(signal(:tick))
      second = await(signal(:tick))
      {:ok, [first, second]}
    end
  end

  test "out-of-order signals are buffered until their matching await" do
    {:ok, run_id} = Continuum.Test.start_synchronous(TwoSignalFlow, %{})

    # The run is parked on :a; :b arrives first and must wait in the mailbox.
    :ok = Continuum.signal(run_id, :b, :second)
    :ok = Continuum.signal(run_id, :a, :first)

    assert {:ok, %{state: :completed, result: {:ok, {:first, :second}}}} =
             Continuum.await(run_id, 1_000)
  end

  test "a signal arriving while the run is parked on a timer waits for the await" do
    {:ok, run_id} = Continuum.Test.start_synchronous(TimerThenSignalFlow, %{})
    wait_until(fn -> Enum.any?(Continuum.Test.history(run_id), &(&1.type == :timer_started)) end)

    :ok = Continuum.signal(run_id, :go, :payload)
    :ok = Continuum.Test.fire_timer(run_id)

    assert {:ok, %{state: :completed, result: {:ok, :payload}}} = Continuum.await(run_id, 1_000)
  end

  test "signals with the same name are consumed in delivery order" do
    {:ok, run_id} = Continuum.Test.start_synchronous(SameNameTwiceFlow, %{})

    :ok = Continuum.signal(run_id, :tick, 1)
    :ok = Continuum.signal(run_id, :tick, 2)

    assert {:ok, %{state: :completed, result: {:ok, [1, 2]}}} = Continuum.await(run_id, 1_000)
  end

  test "consumed signals are journaled with the consuming await's command identity" do
    {:ok, run_id} = Continuum.Test.start_synchronous(TwoSignalFlow, %{})

    :ok = Continuum.signal(run_id, :a, :first)
    :ok = Continuum.signal(run_id, :b, :second)
    {:ok, _} = Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)

    assert [
             %{type: :signal_received, name: :a, command_id: a_command},
             %{type: :signal_received, name: :b, command_id: b_command}
           ] = history

    assert a_command != nil
    assert b_command != nil
    assert a_command != b_command
  end

  test "signaling a nonexistent run returns {:error, :not_found}" do
    assert {:error, :not_found} = Continuum.signal("no-such-run", :a, :payload)
  end

  defp wait_until(fun, timeout_ms \\ 1_000) do
    do_wait_until(fun, System.monotonic_time(:millisecond) + timeout_ms)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition never became true")

      true ->
        Process.sleep(5)
        do_wait_until(fun, deadline)
    end
  end

  test "the consumed buffer entry is removed from the mailbox" do
    {:ok, run_id} = Continuum.Test.start_synchronous(TwoSignalFlow, %{})

    :ok = Continuum.signal(run_id, :a, :first)
    :ok = Continuum.signal(run_id, :b, :second)
    {:ok, _} = Continuum.await(run_id, 1_000)

    instance = Instance.default()
    assert :none = InMemory.consume_buffered_signal!(instance, run_id, :a)
    assert :none = InMemory.consume_buffered_signal!(instance, run_id, :b)
  end
end
