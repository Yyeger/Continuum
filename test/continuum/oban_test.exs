defmodule Continuum.ObanTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Instance

  test "enqueue inserts a one-shot Oban job with stable task identifiers" do
    oban_name = unique_name("oban")

    start_supervised!(
      {Oban, name: oban_name, repo: Repo, queues: false, plugins: false, testing: :manual}
    )

    instance_name = unique_name("continuum")

    instance =
      Instance.new(
        name: instance_name,
        repo: Repo,
        activity_executor: {:oban, name: oban_name, queue: :continuum_activities}
      )

    task_id = Ecto.UUID.generate()

    assert {:ok, job} = Continuum.Oban.enqueue(instance, %{id: task_id, attempt: 3})

    assert job.worker == "Continuum.Oban.Worker"
    assert job.queue == "continuum_activities"
    assert job.max_attempts == 1
    assert arg(job.args, "task_id") == task_id
    assert arg(job.args, "attempt") == 3

    assert job.args |> arg("instance") |> Continuum.Oban.decode_instance() == instance_name
  end

  defp arg(args, key) do
    Map.get(args, key, Map.get(args, String.to_atom(key)))
  end

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end
end
