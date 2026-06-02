defmodule Mix.Tasks.ContinuumAuditTest do
  use Continuum.Test.DataCase, async: false

  import ExUnit.CaptureIO

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run}

  defmodule AuditFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      if Continuum.patched?(:audit_patch), do: :new, else: :old
    end
  end

  setup do
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    Mix.shell(Mix.Shell.Process)
    Continuum.VersionRegistry.ensure_registered(AuditFlow)
    :ok
  end

  test "reports still-in-use and safe-to-remove patch verdicts" do
    pre_patch = start_run!()
    post_patch = start_run!()
    site = hd(AuditFlow.__continuum_workflow__().patch_sites)

    :ok =
      Postgres.append!(
        Continuum.Runtime.Instance.default(),
        post_patch,
        %{
          type: :patched,
          patch_name: :audit_patch,
          value: true,
          command_id: :erlang.append_element(site.command_id, 0)
        },
        nil
      )

    Mix.Task.rerun("continuum.audit", ["--repo", "Continuum.Test.Repo"])
    output = shell_output()
    assert output =~ "Continuum audit"
    assert output =~ "still-in-use"
    assert output =~ "pre_patch=1"

    Repo.update_all(from(r in Run, where: r.id == ^pre_patch), set: [state: "completed"])

    Mix.Task.rerun("continuum.audit", ["--repo", "Continuum.Test.Repo"])
    assert shell_output() =~ "safe-to-remove"
  end

  test "emits json report" do
    _run_id = start_run!()

    output =
      capture_io(fn ->
        Mix.shell(Mix.Shell.IO)
        Mix.Task.rerun("continuum.audit", ["--repo", "Continuum.Test.Repo", "--format", "json"])
      end)

    assert %{"workflows" => workflows} = Jason.decode!(output)
    assert Enum.any?(workflows, &(&1["workflow"] =~ "AuditFlow"))
  after
    Mix.shell(Mix.Shell.Process)
  end

  defp start_run! do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, AuditFlow, %{})
    run_id
  end

  defp shell_output(acc \\ [])

  defp shell_output(acc) do
    receive do
      {:mix_shell, :info, [line]} -> shell_output([line | acc])
    after
      0 -> acc |> Enum.reverse() |> Enum.join("\n")
    end
  end
end
