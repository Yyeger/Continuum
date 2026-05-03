defmodule Continuum.WorkflowCompileTest do
  use ExUnit.Case, async: true

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
      assert is_binary(meta.version_hash)
      assert byte_size(meta.version_hash) == 64
      refute function_exported?(CleanFlow, :child_spec, 1)
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
end
