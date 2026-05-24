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

    :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, VersionA, %{})
    :ok = Postgres.suspend!(Continuum.Runtime.Instance.default(), run_id, nil)

    assert {:ok, %{entrypoint: VersionB}} =
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
end
