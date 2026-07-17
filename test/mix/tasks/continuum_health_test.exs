defmodule Mix.Tasks.ContinuumHealthTest do
  use Continuum.Test.DataCase, async: false

  import ExUnit.CaptureIO

  alias Continuum.Schema.{ActivityTask, Event, HealthReview, Run, Signal, Timer}

  setup do
    Repo.delete_all(HealthReview)
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Timer)
    Repo.delete_all(Signal)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    Mix.shell(Mix.Shell.Process)
    :ok
  end

  test "prints text and machine-readable health reports" do
    Mix.Task.rerun("continuum.health", [
      "--repo",
      "Continuum.Test.Repo",
      "--partition-months",
      "1"
    ])

    output = shell_output()
    assert output =~ "Continuum health:"
    assert output =~ "runtime: ready ready=true"
    assert output =~ "partitions:"
    assert output =~ "overflow=0"
    assert output =~ "workflow_versions:"
    assert output =~ "activities:"
    assert output =~ "signals:"

    json =
      capture_io(fn ->
        Mix.shell(Mix.Shell.IO)

        Mix.Task.rerun("continuum.health", [
          "--repo",
          "Continuum.Test.Repo",
          "--partition-months",
          "1",
          "--format",
          "json"
        ])
      end)

    assert %{"status" => status, "partitions" => %{}, "activities" => %{}} = Jason.decode!(json)
    assert status in ["ok", "degraded"]
  after
    Mix.shell(Mix.Shell.Process)
  end

  test "repair command is a dry run unless execute is passed" do
    run_id = Ecto.UUID.generate()
    future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.truncate(:microsecond)

    Repo.insert!(%Run{
      id: run_id,
      workflow: "HealthFlow",
      version_hash: <<1>>,
      state: "suspended",
      input: :erlang.term_to_binary(%{}),
      lease_owner: "owner",
      lease_token: 77,
      lease_expires_at: future
    })

    args = [
      "--repo",
      "Continuum.Test.Repo",
      "--repair",
      "wake",
      "--target",
      run_id,
      "--lease-token",
      "77"
    ]

    Mix.Task.rerun("continuum.health", args)
    assert shell_output() =~ "planned: wake"
    assert Repo.get!(Run, run_id).next_wakeup_at == nil

    Mix.Task.rerun("continuum.health", args ++ ["--execute"])
    assert shell_output() =~ "executed: wake"
    assert %DateTime{} = Repo.get!(Run, run_id).next_wakeup_at
  end

  defp shell_output(acc \\ []) do
    receive do
      {:mix_shell, :info, [line]} -> shell_output([line | acc])
    after
      0 -> acc |> Enum.reverse() |> Enum.join("\n")
    end
  end
end
