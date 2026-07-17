defmodule Mix.Tasks.ContinuumActivitiesTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Schema.{ActivityAttempt, ActivityOperation, ActivityTask, Run}

  setup do
    Repo.delete_all(ActivityOperation)
    Repo.delete_all(ActivityAttempt)
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Run)
    Mix.shell(Mix.Shell.Process)
    :ok
  end

  test "inspects and classifies an activity without direct table edits" do
    {run_id, task_id} = insert_failed_task!()

    Mix.Task.rerun("continuum.activities", [
      "--repo",
      "Continuum.Test.Repo",
      "--action",
      "inspect",
      "--target",
      task_id
    ])

    output = shell_output()
    assert output =~ "Activity lineage #{task_id}"
    assert output =~ "task #{task_id} state=discarded"

    args = [
      "--repo",
      "Continuum.Test.Repo",
      "--action",
      "classify",
      "--target",
      task_id,
      "--classification",
      "retryable",
      "--operator",
      "oncall",
      "--reason",
      "dependency recovered"
    ]

    Mix.Task.rerun("continuum.activities", args)
    assert shell_output() =~ "planned: classify #{task_id}"
    refute Repo.exists?(ActivityOperation)

    Mix.Task.rerun("continuum.activities", args ++ ["--execute"])
    assert shell_output() =~ "executed: classify #{task_id}"

    assert %ActivityOperation{
             run_id: ^run_id,
             task_id: ^task_id,
             classification: "retryable",
             operator: "oncall",
             reason: "dependency recovered"
           } = Repo.one!(ActivityOperation)
  end

  defp insert_failed_task! do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    run_id = Ecto.UUID.generate()
    task_id = Ecto.UUID.generate()

    Repo.insert!(%Run{
      id: run_id,
      workflow: inspect(__MODULE__),
      version_hash: <<1>>,
      state: "failed",
      input: :erlang.term_to_binary(%{}),
      error: :erlang.term_to_binary(:failed),
      correlation_id: run_id,
      started_at: now,
      completed_at: now
    })

    Repo.insert!(%ActivityTask{
      id: task_id,
      run_id: run_id,
      lineage_id: task_id,
      seq: 0,
      mfa:
        :erlang.term_to_binary(%{
          id: task_id,
          seq: 0,
          mfa: {__MODULE__, :activity, []},
          retry: [max_attempts: 1],
          timeout_ms: 30_000,
          command_id: {:activity, __MODULE__, :run, 1, <<1>>, 0}
        }),
      attempt: 1,
      state: "discarded",
      scheduled_at: now,
      available_at: now,
      error: :erlang.term_to_binary(:failed)
    })

    {run_id, task_id}
  end

  defp shell_output(acc \\ []) do
    receive do
      {:mix_shell, :info, [line]} -> shell_output([line | acc])
    after
      0 -> acc |> Enum.reverse() |> Enum.join("\n")
    end
  end
end
