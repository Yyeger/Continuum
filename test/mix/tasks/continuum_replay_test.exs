defmodule Mix.Tasks.ContinuumReplayTest do
  use Continuum.Test.DataCase, async: false

  import ExUnit.CaptureIO

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run, Signal}

  defmodule ReplayFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      doubled = Continuum.side_effect(fn -> input.seed * 2 end)
      {:ok, await(signal(:approve)) || doubled}
    end
  end

  defmodule CompletingFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, Continuum.side_effect(fn -> input.seed * 2 end)}
    end
  end

  defmodule DriftedFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      Continuum.side_effect(fn -> :something_else end)
    end
  end

  setup do
    start_supervised!(
      {Continuum.VersionRegistry,
       instance: Instance.default(), workflow_modules: [ReplayFlow, CompletingFlow, DriftedFlow]}
    )

    previous_journal = Application.get_env(:continuum, :journal)
    Application.put_env(:continuum, :journal, Postgres)
    Mix.shell(Mix.Shell.Process)

    on_exit(fn ->
      case previous_journal do
        nil -> Application.delete_env(:continuum, :journal)
        journal -> Application.put_env(:continuum, :journal, journal)
      end
    end)

    :ok
  end

  test "reports the suspend reason for a wedged run without touching it" do
    run_id = suspended_run()

    {:ok, ^run_id, :delivered} =
      Postgres.deliver_signal!(Instance.default(), run_id, :approve, :yes, [])

    before = fingerprint(run_id)

    Mix.Task.rerun("continuum.replay", [run_id, "--repo", "Continuum.Test.Repo"])
    output = shell_output()

    assert output =~ "Continuum replay #{run_id}"
    assert output =~ "stored_state: suspended"
    assert output =~ "outcome: suspended"
    assert output =~ "awaiting_signal"
    assert output =~ "events: 2"

    assert fingerprint(run_id) == before
  end

  test "reports the terminal result for a completed run and that it agrees" do
    run_id = completed_run()

    Mix.Task.rerun("continuum.replay", [run_id, "--repo", "Continuum.Test.Repo"])
    output = shell_output()

    assert output =~ "outcome: completed"
    assert output =~ "stored_state: completed"
    assert output =~ "agrees with the stored terminal result"
  end

  test "names the cursor and both sides when code has drifted from history" do
    run_id = completed_run()

    Mix.Task.rerun("continuum.replay", [
      run_id,
      "--repo",
      "Continuum.Test.Repo",
      "--against",
      "Mix.Tasks.ContinuumReplayTest.DriftedFlow"
    ])

    output = shell_output()

    assert output =~ "outcome: drift"
    assert output =~ "cursor: 0"
    assert output =~ "journaled:"
    assert output =~ "code asked for:"
  end

  test "reports a version this node cannot load as unknown_version, not drift" do
    run_id = completed_run()

    Repo.update_all(from(r in Run, where: r.id == ^run_id),
      set: [version_hash: :crypto.strong_rand_bytes(32)]
    )

    assert_raise Mix.Error, ~r/this node cannot replay this version/, fn ->
      Mix.Task.rerun("continuum.replay", [run_id, "--repo", "Continuum.Test.Repo"])
    end
  end

  test "reports a missing run" do
    missing = Ecto.UUID.generate()

    assert_raise Mix.Error, ~r/no run #{missing}/, fn ->
      Mix.Task.rerun("continuum.replay", [missing, "--repo", "Continuum.Test.Repo"])
    end
  end

  test "requires exactly one run id" do
    assert_raise Mix.Error, ~r/expected exactly one run id/, fn ->
      Mix.Task.rerun("continuum.replay", ["--repo", "Continuum.Test.Repo"])
    end
  end

  test "emits a json report" do
    run_id = completed_run()

    output =
      capture_io(fn ->
        Mix.shell(Mix.Shell.IO)

        Mix.Task.rerun("continuum.replay", [
          run_id,
          "--repo",
          "Continuum.Test.Repo",
          "--format",
          "json"
        ])
      end)

    assert %{
             "run_id" => ^run_id,
             "outcome" => "completed",
             "stored_state" => "completed",
             "agrees_with_stored_result" => true,
             "event_count" => 1
           } = Jason.decode!(output)
  after
    Mix.shell(Mix.Shell.Process)
  end

  test "--no-snapshot reloads the complete event history" do
    run_id = completed_run()
    instance = Instance.default()
    run = Repo.get!(Run, run_id)
    events = Postgres.load(instance, run_id)

    assert {:ok, snapshot} =
             Continuum.Snapshot.compact(run_id, run.version_hash, events)

    :ok = Postgres.take_snapshot!(instance, snapshot)

    Mix.Task.rerun("continuum.replay", [
      run_id,
      "--repo",
      "Continuum.Test.Repo",
      "--no-snapshot"
    ])

    output = shell_output()
    assert output =~ "outcome: completed"
    assert output =~ "events: 1 (no snapshot)"
    assert output =~ "agrees with the stored terminal result"
  end

  defp completed_run do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(CompletingFlow, %{seed: 3}, journal: Postgres)

    {:ok, _} = Continuum.await(run_id, 2_000, journal: Postgres)
    run_id
  end

  defp suspended_run do
    {:ok, run_id} = Continuum.Runtime.Engine.start_run(ReplayFlow, %{seed: 3}, journal: Postgres)
    wait_for_state(run_id, "suspended")
    run_id
  end

  defp wait_for_state(run_id, state, attempts \\ 400) do
    run = Repo.get(Run, run_id)

    cond do
      run && run.state == state ->
        :ok

      attempts == 0 ->
        flunk("run #{run_id} never reached #{state}, got #{inspect(run && run.state)}")

      true ->
        wait_for_state(run_id, state, attempts - 1)
    end
  end

  defp fingerprint(run_id) do
    run = Repo.get(Run, run_id)

    %{
      state: run.state,
      lease_token: run.lease_token,
      next_wakeup_at: run.next_wakeup_at,
      events: Repo.aggregate(from(e in Event, where: e.run_id == ^run_id), :count),
      pending:
        Repo.aggregate(
          from(s in Signal, where: s.run_id == ^run_id and s.delivered == false),
          :count
        )
    }
  end

  defp shell_output(acc \\ []) do
    receive do
      {:mix_shell, :info, [line]} -> shell_output([line | acc])
      {:mix_shell, :error, [line]} -> shell_output([line | acc])
    after
      0 -> acc |> Enum.reverse() |> Enum.join("\n")
    end
  end
end
