defmodule Continuum.Runtime.SuspendLeakTest do
  @moduledoc """
  Audit finding 3.2: a user `catch` arm that swallows Continuum's suspend
  throw (thrown *after* the pending effect was journaled) must not let the
  workflow keep executing — the next effect (or the engine, on a normal
  return) raises `Continuum.SuspendLeakError` and the run fails loudly
  instead of corrupting its history.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  test "a swallowed suspend fails the run at the next effect" do
    capture_io(:standard_error, fn ->
      defmodule SwallowThenEffectFlow do
        use Continuum.Workflow, version: 1

        def run(_input) do
          decision =
            try do
              await(signal(:approval))
            catch
              _, _ -> :swallowed
            end

          value = Continuum.side_effect(fn -> decision end)
          {:ok, value}
        end
      end

      send(self(), {:flow, SwallowThenEffectFlow})
    end)

    assert_received {:flow, flow}
    {:ok, run_id} = Continuum.Test.start_synchronous(flow, %{})

    assert {:error, %{state: :failed, error: failure}} = Continuum.await(run_id, 1_000)
    assert %Continuum.SuspendLeakError{} = error = leak_error(failure)
    assert Exception.message(error) =~ "suspend signal was swallowed"
    assert Exception.message(error) =~ "rescue"
  end

  test "a swallowed suspend fails the run when the workflow returns normally" do
    capture_io(:standard_error, fn ->
      defmodule SwallowThenReturnFlow do
        use Continuum.Workflow, version: 1

        def run(_input) do
          try do
            await(signal(:approval))
          catch
            _, _ -> :swallowed
          end

          {:ok, :done}
        end
      end

      send(self(), {:flow, SwallowThenReturnFlow})
    end)

    assert_received {:flow, flow}
    {:ok, run_id} = Continuum.Test.start_synchronous(flow, %{})

    assert {:error, %{state: :failed, error: failure}} = Continuum.await(run_id, 1_000)
    assert %Continuum.SuspendLeakError{} = leak_error(failure)
  end

  # `await` resolves the failure from either the engine's `run_finished`
  # broadcast ({:error, exception}) or the journal row ({kind, reason,
  # stacktrace}), depending on which side wins the subscribe race.
  defp leak_error({:error, %Continuum.SuspendLeakError{} = error}), do: error
  defp leak_error({:error, %Continuum.SuspendLeakError{} = error, _stacktrace}), do: error
  defp leak_error(other), do: other

  test "a catch arm that re-throws the control tuple suspends normally" do
    capture_io(:standard_error, fn ->
      defmodule RethrowFlow do
        use Continuum.Workflow, version: 1

        def run(_input) do
          decision =
            try do
              await(signal(:approval))
            catch
              :throw, {:continuum_suspend, _} = signal -> throw(signal)
              _, _ -> :swallowed
            end

          {:ok, decision}
        end
      end

      send(self(), {:flow, RethrowFlow})
    end)

    assert_received {:flow, flow}
    {:ok, run_id} = Continuum.Test.start_synchronous(flow, %{})

    :ok = Continuum.signal(run_id, :approval, :approved)

    assert {:ok, %{state: :completed, result: {:ok, :approved}}} = Continuum.await(run_id, 1_000)
  end

  test "compiling a workflow with a catch arm warns with a rescue-only hint" do
    warning =
      capture_io(:standard_error, fn ->
        defmodule CatchWarnFlow do
          use Continuum.Workflow, version: 1

          def run(_input) do
            try do
              :ok
            catch
              _, _ -> :caught
            end
          end
        end
      end)

    assert warning =~ "catch` arm"
    assert warning =~ "SuspendLeakError"
    assert warning =~ "rescue"
  end

  test "try/rescue without catch does not warn" do
    warning =
      capture_io(:standard_error, fn ->
        defmodule RescueOnlyFlow do
          use Continuum.Workflow, version: 1

          def run(_input) do
            try do
              {:ok, :fine}
            rescue
              _e -> {:error, :rescued}
            end
          end
        end
      end)

    refute warning =~ "catch` arm"
  end
end
