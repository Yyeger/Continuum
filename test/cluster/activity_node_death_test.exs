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
          [lease_ttl_seconds: 30]
        ])

      wait_until(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)

      assert {:ok, 1} =
               dispatch_until_claimed(peer_a, ActivityWorker.Dispatcher,
                 owner: "activity-a",
                 batch_size: 1,
                 ttl_seconds: 30
               )

      assert_receive {:cluster_activity_started, node_a}, 5_000
      assert String.contains?(Atom.to_string(node_a), "activity_a")

      # Node A dies mid-activity (the activity is parked in Process.sleep(:infinity)),
      # so its run and task leases are left dangling. Rather than wait out a real
      # lease TTL, fast-forward both to expired via SQL — the same trick the
      # single-node lease/recovery tests use — so node B deterministically recovers
      # the orphaned work.
      stop_peer(peer_a)
      force_expire_leases!(run_id)

      assert {:ok, %{activity_tasks: 1}} =
               peer_call(peer_b, Recovery, :recover_once, [[instance: Continuum]])

      assert {:ok, 1} =
               dispatch_until_claimed(peer_b, Dispatcher,
                 owner: "activity-run-b",
                 batch_size: 1,
                 ttl_seconds: 5
               )

      assert {:ok, 1} =
               dispatch_until_claimed(peer_b, ActivityWorker.Dispatcher,
                 owner: "activity-b",
                 batch_size: 1,
                 ttl_seconds: 5
               )

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
