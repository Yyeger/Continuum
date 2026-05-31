defmodule Continuum.VersionRegistryTest do
  use ExUnit.Case, async: true

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
end
