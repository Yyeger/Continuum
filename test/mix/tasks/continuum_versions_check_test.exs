defmodule Mix.Tasks.Continuum.Versions.CheckTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Schema.Run

  defmodule DeploymentFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: input
  end

  setup do
    previous_shell = Mix.shell()
    previous_modules = Application.get_env(:continuum, :workflow_modules)
    Mix.shell(Mix.Shell.Process)
    Application.put_env(:continuum, :workflow_modules, [DeploymentFlow])
    Repo.delete_all(Run)

    on_exit(fn ->
      Mix.shell(previous_shell)

      if is_nil(previous_modules) do
        Application.delete_env(:continuum, :workflow_modules)
      else
        Application.put_env(:continuum, :workflow_modules, previous_modules)
      end
    end)

    :ok
  end

  test "reports every live version present in the release" do
    metadata = DeploymentFlow.__continuum_workflow__()
    insert_run(inspect(DeploymentFlow), metadata.version_hash, "suspended")
    insert_run(inspect(DeploymentFlow), "ignored-terminal", "completed")

    Mix.Task.rerun("continuum.versions.check", ["--repo", "Continuum.Test.Repo", "--strict"])

    assert_received {:mix_shell, :info, [summary]}
    assert summary =~ "ready"
    assert summary =~ "required=1 loaded=1 missing=0"
  end

  test "strict mode fails when a live version is absent" do
    insert_run(inspect(DeploymentFlow), "retired-release-hash", "running")

    assert_raise Mix.Error, ~r/1 version\(s\) missing/, fn ->
      Mix.Task.rerun("continuum.versions.check", [
        "--repo",
        "Continuum.Test.Repo",
        "--strict"
      ])
    end

    assert_received {:mix_shell, :info, [summary]}
    assert summary =~ "missing_versions"
  end

  test "JSON output is suitable for deployment automation" do
    metadata = DeploymentFlow.__continuum_workflow__()
    insert_run(inspect(DeploymentFlow), metadata.version_hash, "stuck_unknown_version")

    Mix.Task.rerun("continuum.versions.check", [
      "--repo",
      "Continuum.Test.Repo",
      "--format",
      "json"
    ])

    assert_received {:mix_shell, :info, [json]}

    assert %{"status" => "ready", "missing_count" => 0, "requirements" => [_]} =
             Jason.decode!(json)
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
end
