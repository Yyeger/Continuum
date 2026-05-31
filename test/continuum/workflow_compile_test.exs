defmodule Continuum.WorkflowCompileTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  describe "compile-time determinism enforcement" do
    test "a clean workflow compiles" do
      defmodule CleanFlow do
        use Continuum.Workflow, version: 1

        def run(_input) do
          :ok
        end
      end

      assert function_exported?(CleanFlow, :__continuum_workflow__, 0)
      meta = CleanFlow.__continuum_workflow__()
      assert meta.module == CleanFlow
      assert meta.version == 1
      assert meta.snapshot_threshold == nil
      assert is_binary(meta.version_hash)
      assert byte_size(meta.version_hash) == 64
      refute function_exported?(CleanFlow, :child_spec, 1)
    end

    test "snapshot_threshold is stored in workflow metadata" do
      defmodule SnapshotThresholdFlow do
        use Continuum.Workflow, version: 1, snapshot_threshold: 500

        def run(_input), do: :ok
      end

      assert SnapshotThresholdFlow.__continuum_workflow__().snapshot_threshold == 500
    end

    test "invalid snapshot_threshold refuses to compile" do
      assert_raise ArgumentError, ~r/expected :snapshot_threshold/, fn ->
        defmodule BadSnapshotThresholdFlow do
          use Continuum.Workflow, version: 1, snapshot_threshold: 0

          def run(_input), do: :ok
        end
      end
    end

    test "a workflow that calls DateTime.utc_now refuses to compile" do
      assert_raise CompileError, ~r/Continuum.now\/0/, fn ->
        defmodule BadFlow1 do
          use Continuum.Workflow, version: 1

          def run(_input) do
            DateTime.utc_now()
          end
        end
      end
    end

    test "a workflow that calls :ets.lookup refuses to compile" do
      assert_raise CompileError, ~r/ETS bypasses the journal/, fn ->
        defmodule BadFlow2 do
          use Continuum.Workflow, version: 1

          def run(_input) do
            :ets.lookup(:foo, :bar)
          end
        end
      end
    end

    test "a workflow that uses :rand.uniform refuses to compile" do
      assert_raise CompileError, ~r/Continuum.random\/0/, fn ->
        defmodule BadFlow3 do
          use Continuum.Workflow, version: 1

          def run(_input) do
            :rand.uniform()
          end
        end
      end
    end

    test "a workflow that calls Continuum.signal refuses to compile" do
      assert_raise CompileError, ~r/signal\/3 is a side effect/, fn ->
        defmodule BadFlow4 do
          use Continuum.Workflow, version: 1

          def run(%{child_id: id}) do
            Continuum.signal(id, :parent_done, :ok)
          end
        end
      end
    end

    test "await child shorthand requires Mod.run(input)" do
      assert_raise ArgumentError, ~r/await child Mod\.run\(input\)/, fn ->
        compile_workflow("BadAwaitChildFun", """
        def run(input) do
          await child OtherFlow.other(input)
        end
        """)
      end

      assert_raise ArgumentError, ~r/await child Mod\.run\(input\)/, fn ->
        compile_workflow("BadAwaitChildArity", """
        def run(input) do
          await child OtherFlow.run(input, :extra)
        end
        """)
      end
    end
  end

  describe "version hash stability" do
    test "AST hash matches across compilations of identical source" do
      hash_a =
        compile_and_get_hash("HashTestA", """
        def run(input) do
          {:ok, input}
        end
        """)

      hash_b =
        compile_and_get_hash("HashTestB", """
        def run(input) do
          {:ok, input}
        end
        """)

      assert hash_a == hash_b
    end

    test "AST hash differs when the body changes" do
      hash_a =
        compile_and_get_hash("HashTestC", """
        def run(input) do
          {:ok, input}
        end
        """)

      hash_b =
        compile_and_get_hash("HashTestD", """
        def run(input) do
          {:error, input}
        end
        """)

      assert hash_a != hash_b
    end
  end

  describe "helper-module trust warnings" do
    test "warns when workflow code calls an unmarked helper module" do
      with_continuum_env([untrusted_call_severity: :warn, trusted_modules: []], fn ->
        output =
          capture_io(:standard_error, fn ->
            compile_helper_workflow("UntrustedHelper", "def classify(input), do: input", """
            def run(input) do
              Helper.classify(input)
            end
            """)
          end)

        assert output =~ "Continuum cannot determine whether"
        assert output =~ ".Helper is deterministic"
        assert output =~ "use Continuum.Pure"
        assert output =~ "activity/2"
        assert output =~ "trusted_modules"
      end)
    end

    test "raises when untrusted helper severity is configured as error" do
      with_continuum_env([untrusted_call_severity: :error, trusted_modules: []], fn ->
        assert_raise CompileError, ~r/cannot determine whether/, fn ->
          compile_helper_workflow("UntrustedHelperError", "def classify(input), do: input", """
          def run(input) do
            Helper.classify(input)
          end
          """)
        end
      end)
    end

    test "does not warn for helpers marked with Continuum.Pure" do
      with_continuum_env([untrusted_call_severity: :warn, trusted_modules: []], fn ->
        output =
          capture_io(:standard_error, fn ->
            compile_helper_workflow(
              "PureHelper",
              "use Continuum.Pure\n  def classify(input), do: input",
              """
              def run(input) do
                Helper.classify(input)
              end
              """
            )
          end)

        refute output =~ "cannot determine whether"
      end)
    end

    test "does not warn for trusted stdlib modules" do
      with_continuum_env([untrusted_call_severity: :warn, trusted_modules: []], fn ->
        output =
          capture_io(:standard_error, fn ->
            compile_workflow("TrustedStdlib", """
            def run(input) do
              Enum.map(input, & &1)
            end
            """)
          end)

        refute output =~ "cannot determine whether"
      end)
    end

    test "does not warn for configured trusted modules" do
      suffix = unique_suffix("TrustedConfig")
      helper = Module.concat([__MODULE__, suffix, Helper])

      with_continuum_env([untrusted_call_severity: :warn, trusted_modules: [helper]], fn ->
        output =
          capture_io(:standard_error, fn ->
            compile_helper_workflow(suffix, "def classify(input), do: input", """
            def run(input) do
              Helper.classify(input)
            end
            """)
          end)

        refute output =~ "cannot determine whether"
      end)
    end

    test "does not warn for activity module calls" do
      with_continuum_env([untrusted_call_severity: :warn, trusted_modules: []], fn ->
        output =
          capture_io(:standard_error, fn ->
            compile_helper_workflow("ActivityCall", "def classify(input), do: input", """
            def run(input) do
              activity Helper.classify(input)
            end
            """)
          end)

        refute output =~ "cannot determine whether"
      end)
    end

    test "does not warn for explicit same-module helper calls" do
      with_continuum_env([untrusted_call_severity: :warn, trusted_modules: []], fn ->
        output =
          capture_io(:standard_error, fn ->
            compile_workflow("SameModuleHelper", """
            def run(input) do
              __MODULE__.classify(input)
            end

            def classify(input), do: input
            """)
          end)

        refute output =~ "cannot determine whether"
      end)
    end
  end

  defp compile_and_get_hash(module_suffix, body) do
    src = """
    defmodule Continuum.WorkflowCompileTest.#{module_suffix} do
      use Continuum.Workflow, version: 1
      #{body}
    end
    """

    [{mod, _}] = Code.compile_string(src)
    mod.__continuum_workflow__().version_hash
  end

  defp compile_helper_workflow(module_suffix, helper_body, workflow_body) do
    suffix =
      if is_atom(module_suffix) do
        module_suffix
      else
        unique_suffix(module_suffix)
      end

    src = """
    defmodule #{inspect(Module.concat([__MODULE__, suffix, Helper]))} do
      #{helper_body}
    end

    defmodule #{inspect(Module.concat([__MODULE__, suffix, Flow]))} do
      use Continuum.Workflow, version: 1
      alias #{inspect(Module.concat([__MODULE__, suffix, Helper]))}, as: Helper

      #{workflow_body}
    end
    """

    Code.compile_string(src)
  end

  defp compile_workflow(module_suffix, workflow_body) do
    suffix = unique_suffix(module_suffix)

    src = """
    defmodule #{inspect(Module.concat([__MODULE__, suffix, Flow]))} do
      use Continuum.Workflow, version: 1

      #{workflow_body}
    end
    """

    Code.compile_string(src)
  end

  defp unique_suffix(prefix), do: :"#{prefix}#{System.unique_integer([:positive])}"

  defp with_continuum_env(config, fun) do
    originals =
      Enum.map(config, fn {key, _value} ->
        {key, Application.fetch_env(:continuum, key)}
      end)

    Enum.each(config, fn {key, value} ->
      Application.put_env(:continuum, key, value)
    end)

    try do
      fun.()
    after
      Enum.each(originals, fn
        {key, {:ok, value}} -> Application.put_env(:continuum, key, value)
        {key, :error} -> Application.delete_env(:continuum, key)
      end)
    end
  end
end
