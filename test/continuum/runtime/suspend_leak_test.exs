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

    assert %Continuum.RunFailure{
             kind: :error,
             reason: %Continuum.SuspendLeakError{} = error
           } = failure

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

    assert %Continuum.RunFailure{
             kind: :error,
             reason: %Continuum.SuspendLeakError{}
           } = failure
  end

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

  test "the implicit-try catch spelling warns too" do
    warning =
      capture_io(:standard_error, fn ->
        defmodule ImplicitCatchWarnFlow do
          use Continuum.Workflow, version: 1

          def run(_input) do
            :ok
          catch
            _, _ -> :caught
          end
        end
      end)

    assert warning =~ "catch` arm"
    assert warning =~ "SuspendLeakError"
    assert warning =~ "rescue"
  end

  test "the implicit-try catch spelling warns on a guarded clause" do
    warning =
      capture_io(:standard_error, fn ->
        defmodule GuardedImplicitCatchWarnFlow do
          use Continuum.Workflow, version: 1

          def run(input) when is_map(input) do
            :ok
          catch
            _, _ -> :caught
          end

          def run(_input), do: :ok
        end
      end)

    assert warning =~ "catch` arm"
  end

  test "the implicit spelling warns exactly as often as the explicit one" do
    explicit =
      capture_io(:standard_error, fn ->
        defmodule ExplicitCountFlow do
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

    implicit =
      capture_io(:standard_error, fn ->
        defmodule ImplicitCountFlow do
          use Continuum.Workflow, version: 1

          def run(_input) do
            :ok
          catch
            _, _ -> :caught
          end
        end
      end)

    assert count_catch_warnings(implicit) == count_catch_warnings(explicit)
  end

  test "one scan of an implicit-try body reports one catch arm" do
    # End-to-end warnings are emitted once per compiled definition, and the
    # generated `V_<hash>` entrypoint recompiles every clause, so counts double
    # there. Scan a body directly to pin that a single body is not itself
    # double-reported by the implicit and explicit detectors both matching.
    warning =
      capture_io(:standard_error, fn ->
        Continuum.AstCheck.check_catch_warnings(
          def_body("""
          def run(_input) do
            :ok
          catch
            _, _ -> :caught
          end
          """),
          __ENV__,
          :run,
          1
        )
      end)

    assert count_catch_warnings(warning) == 1
  end

  test "one scan of an implicit body wrapping an explicit try reports both" do
    warning =
      capture_io(:standard_error, fn ->
        Continuum.AstCheck.check_catch_warnings(
          def_body("""
          def run(_input) do
            try do
              :ok
            catch
              _, _ -> :inner
            end
          catch
            _, _ -> :outer
          end
          """),
          __ENV__,
          :run,
          1
        )
      end)

    assert count_catch_warnings(warning) == 2
  end

  test "one scan of an implicit rescue body reports nothing" do
    warning =
      capture_io(:standard_error, fn ->
        Continuum.AstCheck.check_catch_warnings(
          def_body("""
          def run(_input) do
            :ok
          rescue
            _e -> :rescued
          after
            :ok
          end
          """),
          __ENV__,
          :run,
          1
        )
      end)

    assert count_catch_warnings(warning) == 0
  end

  # Mirrors what `@on_definition` hands `check_catch_warnings/4`: the body of a
  # definition, which for the implicit-try spelling is a keyword list.
  defp def_body(source) do
    {:def, _meta, [_head, body]} = Code.string_to_quoted!(source)
    body
  end

  defp count_catch_warnings(output) do
    # The warning body mentions "`catch` arm" more than once, so count on the
    # headline instead.
    length(String.split(output, "workflow code uses a `catch` arm")) - 1
  end

  test "implicit rescue and after without catch do not warn" do
    warning =
      capture_io(:standard_error, fn ->
        defmodule ImplicitRescueOnlyFlow do
          use Continuum.Workflow, version: 1

          def run(_input) do
            {:ok, :fine}
          rescue
            _e -> {:error, :rescued}
          after
            :ok
          end
        end
      end)

    refute warning =~ "catch` arm"
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
              RuntimeError -> {:error, :rescued}
            end
          end
        end
      end)

    refute warning =~ "catch` arm"
  end
end
