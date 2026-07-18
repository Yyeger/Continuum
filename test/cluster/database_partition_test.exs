defmodule Continuum.Cluster.DatabasePartitionTest do
  use Continuum.Test.ClusterCase, async: false

  test "a transient database partition fences the reconnected stale owner" do
    isolated = start_peer!(:partitioned_owner)
    replacement = start_peer!(:partition_replacement)
    test_pid = self()

    try do
      run_id =
        peer_call(isolated, Continuum.Test.ClusterScenarios, :start_signal_run, [
          test_pid,
          [lease_ttl_seconds: 30]
        ])

      assert_receive {:signal_run_started, ^run_id, _owner_node, _engine_pid}, 5_000

      wait_until(fn -> Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended" end)

      assert :ok =
               peer_call(isolated, Continuum.Test.ClusterScenarios, :attach_lease_lost, [test_pid])

      assert :ok = peer_call(isolated, Continuum.Test.ClusterNode, :disconnect_repo, [])

      force_expire_leases!(run_id)

      assert {:ok, 1} =
               peer_call(replacement, Dispatcher, :dispatch_once, [
                 [owner: "partition-replacement", batch_size: 1, ttl_seconds: 30]
               ])

      assert :ok = peer_call(isolated, Continuum.Test.ClusterNode, :reconnect_repo, [])

      assert :ok =
               peer_call(isolated, Continuum.Runtime.Lease.Heartbeater, :renew_once, [
                 Continuum.Runtime.Instance.default()
               ])

      assert_receive {:cluster_lease_lost, ^run_id, _metadata}, 5_000

      wait_until(fn ->
        peer_call(isolated, Registry, :lookup, [Continuum.Runtime.Registry, run_id]) == []
      end)

      assert :ok =
               peer_call(replacement, Continuum, :signal, [
                 run_id,
                 :continue,
                 :recovered,
                 [journal: Journal.Postgres]
               ])

      assert {:ok, %{state: :completed, result: :recovered}} =
               Engine.await(run_id, 5_000, journal: Journal.Postgres)
    after
      stop_peer(isolated)
      stop_peer(replacement)
    end
  end
end
