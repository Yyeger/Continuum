defmodule Continuum.Cluster.ActivityNodeDeathTest do
  use Continuum.Test.ClusterCase, async: false

  test "an activity task leased by a dead node is requeued after lease expiry" do
    peer_a = start_peer!(:activity_a)
    peer_b = start_peer!(:activity_b)
    test_pid = self()

    try do
      run_id =
        peer_call(peer_a, Continuum.Test.ClusterScenarios, :start_activity_run, [
          %{test_pid: test_pid, value: 7},
          [lease_ttl_seconds: 1]
        ])

      wait_until(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)

      assert {:ok, 1} =
               peer_call(peer_a, ActivityWorker.Dispatcher, :dispatch_once, [
                 [owner: "activity-a", batch_size: 1, ttl_seconds: 1]
               ])

      assert_receive {:cluster_activity_started, node_a}, 5_000
      assert String.contains?(Atom.to_string(node_a), "activity_a")

      stop_peer(peer_a)

      wait_until(fn ->
        task = Repo.one!(ActivityTask)

        task.state == "leased" and
          DateTime.compare(task.lease_expires_at, DateTime.utc_now()) == :lt
      end)

      assert {:ok, %{activity_tasks: 1}} =
               peer_call(peer_b, Recovery, :recover_once, [[instance: Continuum]])

      assert {:ok, 1} =
               peer_call(peer_b, Dispatcher, :dispatch_once, [
                 [owner: "activity-run-b", batch_size: 1, ttl_seconds: 5]
               ])

      assert {:ok, 1} =
               peer_call(peer_b, ActivityWorker.Dispatcher, :dispatch_once, [
                 [owner: "activity-b", batch_size: 1, ttl_seconds: 5]
               ])

      assert_receive {:cluster_activity_started, node_b}, 5_000
      assert String.contains?(Atom.to_string(node_b), "activity_b")

      assert {:ok, %{state: :completed, result: {:ok, 14}}} =
               Engine.await(run_id, 5_000, journal: Journal.Postgres)

      assert ["activity_scheduled", "activity_completed"] = event_types(run_id)

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
