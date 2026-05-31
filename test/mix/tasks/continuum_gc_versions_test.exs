defmodule Mix.Tasks.Continuum.GcVersionsTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Schema.{Run, WorkflowVersion}

  defmodule LogicalFlow do
  end

  defmodule CurrentFlow do
    use Continuum.Workflow, workflow: LogicalFlow, version: 1

    def run(input), do: {:ok, input}
  end

  defmodule UnloadedLogicalFlow do
  end

  setup do
    previous_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)

    Repo.delete_all(WorkflowVersion)
    Repo.delete_all(Run)
    {:ok, current} = Continuum.VersionRegistry.ensure_registered(CurrentFlow)

    on_exit(fn ->
      Mix.shell(previous_shell)
    end)

    %{current: current}
  end

  test "dry-run lists candidates without deleting rows", %{current: current} do
    old_hash = "old-version-hash"
    insert_version(inspect(LogicalFlow), old_hash, "OldFlow")
    insert_version(current.workflow_string, current.version_hash, inspect(CurrentFlow))

    Mix.Task.rerun("continuum.gc_versions", ["--repo", "Continuum.Test.Repo"])

    assert_received {:mix_shell, :info, [summary]}
    assert summary =~ inspect(LogicalFlow)
    assert summary =~ "1 loaded"
    assert summary =~ "1 candidates"

    assert_received {:mix_shell, :info, [candidate]}
    assert candidate =~ "Would delete"
    assert candidate =~ old_hash
    assert Repo.aggregate(WorkflowVersion, :count) == 2
  end

  test "non-terminal and stuck-unknown-version runs pin their hashes", %{current: current} do
    suspended_hash = "suspended-version-hash"
    stuck_hash = "stuck-version-hash"
    completed_hash = "completed-version-hash"

    workflow = current.workflow_string
    insert_version(workflow, suspended_hash, "SuspendedFlow")
    insert_version(workflow, stuck_hash, "StuckFlow")
    insert_version(workflow, completed_hash, "CompletedFlow")
    insert_version(workflow, current.version_hash, inspect(CurrentFlow))

    insert_run(workflow, suspended_hash, "suspended")
    insert_run(workflow, stuck_hash, "stuck_unknown_version")
    insert_run(workflow, completed_hash, "completed")

    Mix.Task.rerun("continuum.gc_versions", ["--repo", "Continuum.Test.Repo"])

    assert_received {:mix_shell, :info, [summary]}
    assert summary =~ "4 versions"
    assert summary =~ "2 pinned by non-terminal runs"
    assert summary =~ "1 candidates"

    assert_received {:mix_shell, :info, [candidate]}
    assert candidate =~ completed_hash
    refute candidate =~ suspended_hash
    refute candidate =~ stuck_hash
  end

  test "execute deletes only safe candidates and is idempotent", %{current: current} do
    candidate_hash = "candidate-version-hash"
    pinned_hash = "pinned-version-hash"
    workflow = current.workflow_string

    insert_version(workflow, candidate_hash, "CandidateFlow")
    insert_version(workflow, pinned_hash, "PinnedFlow")
    insert_version(workflow, current.version_hash, inspect(CurrentFlow))
    insert_version(inspect(UnloadedLogicalFlow), "unloaded-hash", "UnloadedFlow")
    insert_run(workflow, pinned_hash, "running")

    Mix.Task.rerun("continuum.gc_versions", ["--repo", "Continuum.Test.Repo", "--execute"])

    delete_line = receive_info_matching("Delete")
    assert delete_line =~ "Delete"
    assert delete_line =~ candidate_hash
    assert_receive {:mix_shell, :info, ["Deleted 1 workflow_versions rows"]}

    refute version_exists?(workflow, candidate_hash)
    assert version_exists?(workflow, pinned_hash)
    assert version_exists?(workflow, current.version_hash)
    assert version_exists?(inspect(UnloadedLogicalFlow), "unloaded-hash")

    flush_mix_shell()
    Mix.Task.rerun("continuum.gc_versions", ["--repo", "Continuum.Test.Repo", "--execute"])
    assert_received {:mix_shell, :info, ["Deleted 0 workflow_versions rows"]}
  end

  defp insert_version(workflow, version_hash, entrypoint) do
    %WorkflowVersion{}
    |> Ecto.Changeset.change(%{
      workflow: workflow,
      version_hash: version_hash,
      entrypoint: entrypoint,
      registered_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
    })
    |> Repo.insert!()
  end

  defp insert_run(workflow, version_hash, state) do
    %Run{}
    |> Ecto.Changeset.change(%{
      id: Ecto.UUID.generate(),
      workflow: workflow,
      version_hash: version_hash,
      state: state,
      input: :erlang.term_to_binary(%{})
    })
    |> Repo.insert!()
  end

  defp version_exists?(workflow, version_hash) do
    Repo.exists?(
      from(v in WorkflowVersion,
        where: v.workflow == ^workflow and v.version_hash == ^version_hash
      )
    )
  end

  defp flush_mix_shell do
    receive do
      {:mix_shell, _, _} -> flush_mix_shell()
    after
      0 -> :ok
    end
  end

  defp receive_info_matching(pattern) do
    receive do
      {:mix_shell, :info, [message]} ->
        if message =~ pattern, do: message, else: receive_info_matching(pattern)
    after
      1_000 -> flunk("did not receive Mix shell info matching #{inspect(pattern)}")
    end
  end
end
