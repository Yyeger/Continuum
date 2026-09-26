defmodule Continuum.PureVersioningTest do
  use Continuum.Test.DataCase, async: false

  import ExUnit.CaptureIO

  setup do
    previous = Code.compiler_options(ignore_module_conflict: true)
    on_exit(fn -> Code.compiler_options(previous) end)
    :ok
  end

  test "helper edits change workflow identity while old entrypoints retain old behavior" do
    root = unique_root()
    helper = Module.concat(root, Helper)
    flow = Module.concat(root, Flow)
    compile_helper(helper, "def calculate(value), do: value * 2")
    compile_flow(flow, helper)
    original = flow.__continuum_workflow__()
    assert original.entrypoint.run(10) == 20

    compile_helper(helper, "def calculate(value), do: value * 3")
    compile_flow(flow, helper)
    current = flow.__continuum_workflow__()

    assert original.version_hash != current.version_hash
    assert current.entrypoint.run(10) == 30
    assert original.entrypoint.run(10) == 20
  end

  test "transitive helpers and captured calls are pinned" do
    root = unique_root()
    leaf = Module.concat(root, Leaf)
    helper = Module.concat(root, Helper)
    flow = Module.concat(root, Flow)
    compile_helper(leaf, "def calculate(value), do: value + 1")
    wrapper = "def calculate(value), do: Enum.map([value], &#{inspect(leaf)}.calculate/1)"
    compile_helper(helper, wrapper)
    compile_flow(flow, helper)
    original = flow.__continuum_workflow__()

    compile_helper(leaf, "def calculate(value), do: value + 2")
    compile_helper(helper, wrapper)
    compile_flow(flow, helper)
    current = flow.__continuum_workflow__()

    assert original.version_hash != current.version_hash
    assert original.entrypoint.run(10) == [11]
    assert current.entrypoint.run(10) == [12]
  end

  test "same-file forward helpers fail explicitly instead of getting an order-dependent hash" do
    root = unique_root()
    helper = Module.concat(root, Helper)
    flow = Module.concat(root, Flow)

    capture_io(:standard_error, fn ->
      assert_raise CompileError, ~r/define helpers before their callers/, fn ->
        Code.compile_string("""
        defmodule #{inspect(flow)} do
          use Continuum.Workflow
          def run(value), do: #{inspect(helper)}.calculate(value)
        end
        defmodule #{inspect(helper)} do
          use Continuum.Pure
          def calculate(value), do: value * 2
        end
        """)
      end
    end)
  end

  test "helper versions preserve literal module identity and pin remote recursion" do
    helper = Module.concat(unique_root(), Helper)

    compile_helper(helper, """
    def calculate(0), do: __MODULE__
    def calculate(n), do: __MODULE__.calculate(n - 1)
    """)

    old_entrypoint = helper.__continuum_pure_version__().entrypoint
    compile_helper(helper, "def calculate(_value), do: :changed")
    assert old_entrypoint.calculate(2) == helper
  end

  test "a suspended durable run resumes with its original helper after a deployment" do
    root = unique_root()
    helper = Module.concat(root, Helper)
    flow = Module.concat(root, Flow)
    compile_helper(helper, "def calculate(value), do: value * 2")

    source = """
    defmodule #{inspect(flow)} do
      use Continuum.Workflow
      def run(value) do
        await(signal(:resume))
        #{inspect(helper)}.calculate(value)
      end
    end
    """

    Code.compile_string(source)
    journal = Continuum.Runtime.Journal.Postgres
    {:ok, old_run} = Continuum.start(flow, 10, journal: journal)

    # Waiting for the journaled await ensures the old entrypoint owns a
    # suspended run before either module is replaced.
    wait_for_await(old_run)
    compile_helper(helper, "def calculate(value), do: value * 3")
    Code.compile_string(source)

    :ok = Continuum.signal(old_run, :resume, :go, journal: journal)
    assert {:ok, %{result: 20}} = Continuum.await(old_run, 1_000, journal: journal)

    {:ok, new_run} = Continuum.start(flow, 10, journal: journal)
    :ok = Continuum.signal(new_run, :resume, :go, journal: journal)
    assert {:ok, %{result: 30}} = Continuum.await(new_run, 1_000, journal: journal)
  end

  test "Mix recompiles callers when only a transitive helper changes" do
    directory =
      Path.join(System.tmp_dir!(), "continuum-pure-#{System.unique_integer([:positive])}")

    File.mkdir_p!(Path.join(directory, "lib"))
    on_exit(fn -> File.rm_rf!(directory) end)

    File.write!(Path.join(directory, "mix.exs"), """
    defmodule PureCompileFixture.MixProject do
      use Mix.Project
      def project,
        do: [app: :pure_compile_fixture, version: "0.1.0", prune_code_paths: false]
    end
    """)

    leaf_path = Path.join(directory, "lib/leaf.ex")
    File.write!(leaf_path, fixture_leaf(2))

    File.write!(Path.join(directory, "lib/helper.ex"), """
    defmodule PureCompileFixture.Helper do
      use Continuum.Pure
      def calculate(value), do: PureCompileFixture.Leaf.calculate(value)
    end
    """)

    File.write!(Path.join(directory, "lib/flow.ex"), """
    defmodule PureCompileFixture.Flow do
      use Continuum.Workflow
      def run(value), do: PureCompileFixture.Helper.calculate(value)
    end
    """)

    original = compile_fixture(directory)
    Process.sleep(1_100)
    File.write!(leaf_path, fixture_leaf(3))
    current = compile_fixture(directory)
    refute current == original
    assert original =~ ":20"
    assert current =~ ":30"
  end

  defp fixture_leaf(multiplier) do
    """
    defmodule PureCompileFixture.Leaf do
      use Continuum.Pure
      def calculate(value), do: value * #{multiplier}
    end
    """
  end

  defp compile_fixture(directory) do
    paths = Enum.flat_map(:code.get_path(), fn path -> ["-pa", Path.expand(to_string(path))] end)

    expression = """
    metadata = PureCompileFixture.Flow.__continuum_workflow__()
    IO.puts("IDENTITY=" <> metadata.version_hash <> ":" <> to_string(metadata.entrypoint.run(10)))
    """

    {output, status} =
      System.cmd(
        System.find_executable("elixir"),
        paths ++ ["-S", "mix", "run", "--no-start", "--no-deps-check", "-e", expression],
        cd: directory,
        stderr_to_stdout: true,
        env: [{"MIX_ENV", "test"}]
      )

    assert status == 0, output
    [_, identity] = Regex.run(~r/IDENTITY=([^\n]+)/, output)
    identity
  end

  defp wait_for_await(run_id, attempts \\ 100)
  defp wait_for_await(_run_id, 0), do: flunk("workflow did not suspend")

  defp wait_for_await(run_id, attempts) do
    if Repo.get!(Continuum.Schema.Run, run_id).state == "suspended" do
      :ok
    else
      Process.sleep(10)
      wait_for_await(run_id, attempts - 1)
    end
  end

  defp compile_helper(module, body) do
    Code.compile_string("""
    defmodule #{inspect(module)} do
      use Continuum.Pure
      #{body}
    end
    """)
  end

  defp compile_flow(module, helper) do
    Code.compile_string("""
    defmodule #{inspect(module)} do
      use Continuum.Workflow
      def run(value), do: #{inspect(helper)}.calculate(value)
    end
    """)
  end

  defp unique_root, do: Module.concat(__MODULE__, "Case#{System.unique_integer([:positive])}")
end
