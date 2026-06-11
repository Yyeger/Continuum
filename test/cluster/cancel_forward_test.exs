defmodule Continuum.Cluster.CancelForwardTest do
  use Continuum.Test.ClusterCase, async: false

  test "cancel issued on another node reaches the healthy owning engine" do
    peer_a = start_peer!(:cancel_a)
    peer_b = start_peer!(:cancel_b)
    test_pid = self()

    try do
      # :pg membership only syncs across connected nodes.
      assert peer_call(peer_b, Node, :connect, [peer_a.node])

      run_id =
        peer_call(peer_a, Continuum.Test.ClusterScenarios, :start_signal_run, [test_pid, []])

      assert_receive {:signal_run_started, ^run_id, _node_a, _engine_pid}, 5_000

      # Parked on the signal await with a live, heartbeated lease: the durable
      # lease-steal path cannot cancel this run, only the owning engine can.
      wait_until(fn ->
        Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
      end)

      assert :ok =
               peer_call(peer_b, Continuum, :cancel, [
                 run_id,
                 [journal: Continuum.Runtime.Journal.Postgres]
               ])

      run = Repo.one!(from(r in Run, where: r.id == ^run_id))
      assert run.state == "failed"
      assert :erlang.binary_to_term(run.error) == :cancelled
    after
      stop_peer(peer_a)
      stop_peer(peer_b)
    end
  end
end
