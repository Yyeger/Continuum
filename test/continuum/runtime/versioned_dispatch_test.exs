defmodule Continuum.Runtime.VersionedDispatchTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{Dispatcher, Journal.Postgres}
  alias Continuum.Schema.{Event, Run, WorkflowVersion}

  defmodule LogicalFlow do
  end

  defmodule VersionA do
    use Continuum.Workflow, workflow: LogicalFlow, version: 1

    def run(_input), do: {:ok, :version_a}
  end

  defmodule VersionB do
    use Continuum.Workflow, workflow: LogicalFlow, version: 1

    def run(_input), do: {:ok, :version_b}
  end

  setup do
    Repo.delete_all(WorkflowVersion)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "resumes a suspended run through its journaled version hash" do
    run_id = Ecto.UUID.generate()
    version_b_entrypoint = VersionB.__continuum_entrypoint__()

    :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, VersionA, %{})
    :ok = Postgres.suspend!(Continuum.Runtime.Instance.default(), run_id, nil)

    assert {:ok, %{entrypoint: ^version_b_entrypoint}} =
             Continuum.VersionRegistry.ensure_registered(VersionB)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "versioned-dispatch", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, :version_a}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "fresh starts use the entrypoint requested by the caller" do
    assert {:ok, run_id} = Continuum.start(VersionB, %{}, journal: Postgres)

    assert {:ok, %{state: :completed, result: {:ok, :version_b}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "generated entrypoints keep an in-flight run on old code after recompilation" do
    suffix = System.unique_integer([:positive])
    logical = Module.concat([__MODULE__, :"GeneratedLogical#{suffix}"])
    flow = Module.concat([__MODULE__, :"GeneratedFlow#{suffix}"])

    with_module_conflicts_ignored(fn ->
      compile_generated_flow(flow, logical, 1, """
      def run(_input) do
        payload = await(signal(:go))
        {:ok, __MODULE__.label(payload)}
      end

      def label(payload), do: {:version_a, payload}
      """)

      old_hash = flow.__continuum_workflow__().version_hash
      old_entrypoint = flow.__continuum_entrypoint__()

      assert {:ok, run_id} = Continuum.start(flow, %{}, journal: Postgres)
      assert_eventually(fn -> Repo.get!(Run, run_id).state == "suspended" end)

      compile_generated_flow(flow, logical, 2, """
      def run(_input), do: {:ok, __MODULE__.label(:fresh)}
      def label(_payload), do: :version_b
      """)

      new_entrypoint = flow.__continuum_entrypoint__()
      refute new_entrypoint == old_entrypoint

      assert {:ok, %{entrypoint: ^old_entrypoint}} =
               Continuum.VersionRegistry.resolve(logical, old_hash)

      assert {:ok, %{entrypoint: ^new_entrypoint}} =
               Continuum.VersionRegistry.ensure_registered(flow)

      assert :ok = Continuum.Test.inject_signal(run_id, :go, :payload, journal: Postgres)
      assert {:ok, claimed} = Dispatcher.dispatch_once(owner: "generated-dispatch", batch_size: 1)
      assert claimed in [0, 1]

      assert {:ok, %{state: :completed, result: {:ok, {:version_a, :payload}}}} =
               Continuum.await(run_id, 1_000, journal: Postgres)

      assert {:ok, fresh_id} = Continuum.start(flow, %{}, journal: Postgres)

      assert {:ok, %{state: :completed, result: {:ok, :version_b}}} =
               Continuum.await(fresh_id, 1_000, journal: Postgres)
    end)
  end

  defp compile_generated_flow(flow, logical, version, body) do
    Code.compile_string("""
    defmodule #{inspect(flow)} do
      use Continuum.Workflow, workflow: #{inspect(logical)}, version: #{version}

      #{body}
    end
    """)
  end

  defp with_module_conflicts_ignored(fun) do
    previous = Code.compiler_options()
    Code.compiler_options(ignore_module_conflict: true)

    try do
      fun.()
    after
      Code.compiler_options(previous)
    end
  end

  defp assert_eventually(fun, attempts \\ 40)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
