defmodule Continuum.VersionRegistryTest do
  use ExUnit.Case, async: true

  alias Continuum.Runtime.Instance
  alias Continuum.VersionRegistry

  defmodule LogicalFlow do
  end

  defmodule SameA do
    use Continuum.Workflow, workflow: LogicalFlow, version: 1

    def run(input), do: {:ok, input.value}
  end

  defmodule SameB do
    use Continuum.Workflow, workflow: LogicalFlow, version: 1

    def run(input), do: {:ok, input.value}
  end

  defmodule WhitespaceA do
    use Continuum.Workflow, workflow: LogicalFlow, version: 1
    def run(input), do: {:ok, input.value}
  end

  defmodule WhitespaceB do
    use Continuum.Workflow, workflow: LogicalFlow, version: 1

    def run(input) do
      {:ok, input.value}
    end
  end

  defmodule HelperA do
    use Continuum.Workflow, workflow: LogicalFlow, version: 1

    def run(input), do: helper_a(input.value)
    defp helper_a(value), do: {:ok, value}
  end

  defmodule HelperB do
    use Continuum.Workflow, workflow: LogicalFlow, version: 1

    def run(input), do: helper_b(input.value)
    defp helper_b(value), do: {:ok, value}
  end

  test "identical workflow content produces identical hashes" do
    assert SameA.__continuum_workflow__().version_hash ==
             SameB.__continuum_workflow__().version_hash
  end

  test "trivial whitespace and formatting changes do not change the hash" do
    assert WhitespaceA.__continuum_workflow__().version_hash ==
             WhitespaceB.__continuum_workflow__().version_hash
  end

  test "private helper rename changes the content hash" do
    refute HelperA.__continuum_workflow__().version_hash ==
             HelperB.__continuum_workflow__().version_hash
  end

  test "registers and resolves hash-specific entrypoints for one logical workflow" do
    assert {:ok, a} = Continuum.VersionRegistry.ensure_registered(SameA)
    assert {:ok, b} = Continuum.VersionRegistry.ensure_registered(HelperA)

    assert a.workflow == LogicalFlow
    assert a.entrypoint == SameA.__continuum_entrypoint__()
    assert b.workflow == LogicalFlow
    assert b.entrypoint == HelperA.__continuum_entrypoint__()

    same_a_entrypoint = SameA.__continuum_entrypoint__()
    helper_a_entrypoint = HelperA.__continuum_entrypoint__()

    assert {:ok, %{entrypoint: ^same_a_entrypoint}} =
             Continuum.VersionRegistry.resolve(
               LogicalFlow,
               SameA.__continuum_workflow__().version_hash
             )

    assert {:ok, %{entrypoint: ^helper_a_entrypoint}} =
             Continuum.VersionRegistry.resolve(
               inspect(LogicalFlow),
               HelperA.__continuum_workflow__().version_hash
             )
  end

  test "registrar is permanently restarted" do
    assert %{restart: :permanent} =
             VersionRegistry.child_spec(instance: Instance.new(name: unique_instance_name()))
  end

  test "registrar retries transient persistence failures without crashing" do
    test_pid = self()
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    registration_fun = fn _instance, entries ->
      attempt = Agent.get_and_update(attempts, &{&1 + 1, &1 + 1})
      send(test_pid, {:registration_attempt, attempt, entries})
      if attempt == 1, do: {:error, :repo_unavailable}, else: :ok
    end

    instance = Instance.new(name: unique_instance_name(), repo: __MODULE__.FakeRepo)

    pid =
      start_supervised!(
        {VersionRegistry,
         instance: instance,
         workflow_modules: [SameA],
         registration_fun: registration_fun,
         retry_base_ms: 1,
         retry_max_ms: 2}
      )

    assert_receive {:registration_attempt, 1, [%{entrypoint: entrypoint}]}
    assert entrypoint == SameA.__continuum_entrypoint__()
    assert_receive {:registration_attempt, 2, _entries}, 100
    assert Process.alive?(pid)

    assert_eventually(fn ->
      assert %{state: :ready, registered_count: 1, pending_count: 0, last_error: nil} =
               VersionRegistry.status(instance)
    end)
  end

  test "first use is sent through the instance registrar" do
    test_pid = self()

    registration_fun = fn _instance, entries ->
      send(test_pid, {:registered, entries})
      :ok
    end

    instance = Instance.new(name: unique_instance_name(), repo: __MODULE__.FakeRepo)

    start_supervised!(
      {VersionRegistry,
       instance: instance, workflow_modules: [], registration_fun: registration_fun}
    )

    assert_receive {:registered, []}
    assert {:ok, entry} = VersionRegistry.ensure_registered(SameA, instance)
    assert_receive {:registered, [^entry]}
    assert %{state: :ready, registered_count: 1} = VersionRegistry.status(instance)
  end

  test "status reports an instance without a registrar" do
    instance = Instance.new(name: unique_instance_name())
    assert %{state: :not_running, registered_count: 0} = VersionRegistry.status(instance)
  end

  defp assert_eventually(assertion, attempts \\ 20)

  defp assert_eventually(assertion, attempts) when attempts > 0 do
    assertion.()
  rescue
    ExUnit.AssertionError ->
      Process.sleep(5)
      assert_eventually(assertion, attempts - 1)
  end

  defp assert_eventually(assertion, 0), do: assertion.()

  defp unique_instance_name do
    :"version_registry_test_#{System.unique_integer([:positive])}"
  end
end
