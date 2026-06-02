defmodule Continuum.Cluster.LeaseStealTest do
  use Continuum.Test.ClusterCase, async: false

  test "a second node steals an expired lease and the stale owner stops on renewal" do
    peer_a = start_peer!(:lease_a)
    peer_b = start_peer!(:lease_b)
    test_pid = self()

    try do
      run_id =
        peer_call(peer_a, Continuum.Test.ClusterScenarios, :start_signal_run, [
          test_pid,
          [lease_ttl_seconds: 1]
        ])

      assert_receive {:signal_run_started, ^run_id, _node_a, _engine_pid}, 5_000

      wait_until(fn ->
        run = Repo.one!(from(r in Run, where: r.id == ^run_id))
        DateTime.compare(run.lease_expires_at, DateTime.utc_now()) == :lt
      end)

      assert {:ok, 1} =
               peer_call(peer_b, Dispatcher, :dispatch_once, [
                 [owner: "peer-b", batch_size: 1, ttl_seconds: 30]
               ])

      assert :ok =
               peer_call(peer_a, Continuum.Test.ClusterScenarios, :attach_lease_lost, [
                 test_pid
               ])

      assert :ok =
               peer_call(peer_a, Continuum.Runtime.Lease.Heartbeater, :renew_once, [
                 Continuum.Runtime.Instance.default()
               ])

      assert_receive {:cluster_lease_lost, ^run_id, _metadata}, 5_000

      wait_until(fn ->
        peer_call(peer_a, Registry, :lookup, [Continuum.Runtime.Registry, run_id]) == []
      end)
    after
      stop_peer(peer_a)
      stop_peer(peer_b)
    end
  end
end
