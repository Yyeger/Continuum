defmodule Continuum.VersionRegistryDurableTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Instance
  alias Continuum.Schema.WorkflowVersion
  alias Continuum.VersionRegistry

  defmodule DurableFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, input}
  end

  test "first use durably registers a workflow version" do
    instance =
      Instance.new(
        name: :"durable_registry_test_#{System.unique_integer([:positive])}",
        repo: Repo
      )

    start_supervised!({VersionRegistry, instance: instance, workflow_modules: []})

    metadata = DurableFlow.__continuum_workflow__()

    Repo.delete_all(
      from(v in WorkflowVersion,
        where: v.workflow == ^inspect(DurableFlow) and v.version_hash == ^metadata.version_hash
      )
    )

    assert {:ok, entry} = VersionRegistry.ensure_registered(DurableFlow, instance)

    assert %WorkflowVersion{entrypoint: entrypoint} =
             Repo.get_by(WorkflowVersion,
               workflow: inspect(DurableFlow),
               version_hash: metadata.version_hash
             )

    assert entrypoint == inspect(entry.entrypoint)
    assert %{state: :ready, registered_count: 1} = VersionRegistry.status(instance)
  end
end
