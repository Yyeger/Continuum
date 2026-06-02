defmodule Continuum.Cluster.DispatchRaceTest do
  use Continuum.Test.ClusterCase, async: false

  test "two dispatchers racing one runnable run claim it once" do
    peer_a = start_peer!(:dispatch_a)
    peer_b = start_peer!(:dispatch_b)

    try do
      run_id = Ecto.UUID.generate()
      instance = Continuum.Runtime.Instance.default()

      :ok =
        Journal.Postgres.start_run(instance, run_id, ClusterFlows.SideEffectFlow, %{value: 42})

      task_a =
        Task.async(fn ->
          peer_call(peer_a, Dispatcher, :dispatch_once, [
            [owner: "peer-a", batch_size: 1, ttl_seconds: 5]
          ])
        end)

      task_b =
        Task.async(fn ->
          peer_call(peer_b, Dispatcher, :dispatch_once, [
            [owner: "peer-b", batch_size: 1, ttl_seconds: 5]
          ])
        end)

      results = [Task.await(task_a, 15_000), Task.await(task_b, 15_000)]

      assert Enum.sort(results) == [{:ok, 0}, {:ok, 1}]

      assert {:ok, %{state: :completed, result: {:ok, 42}}} =
               Engine.await(run_id, 5_000, journal: Journal.Postgres)

      assert ["side_effect"] = event_types(run_id)

      run = Repo.one!(from(r in Run, where: r.id == ^run_id))
      assert run.state == "completed"
    after
      stop_peer(peer_a)
      stop_peer(peer_b)
    end
  end

  defp event_types(run_id) do
    Repo.all(
      from(e in Event,
        where: e.run_id == ^run_id,
        order_by: [asc: e.seq],
        select: e.event_type
      )
    )
  end
end
