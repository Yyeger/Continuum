defmodule Continuum.Cluster.GracefulHandoffTest do
  use Continuum.Test.ClusterCase, async: false

  test "a rolling shutdown releases the run lease for immediate takeover" do
    owner = start_peer!(:handoff_owner)
    replacement = start_peer!(:handoff_replacement)

    try do
      run_id =
        peer_call(owner, Continuum.Test.ClusterScenarios, :start_signal_run, [self(), []])

      assert_receive {:signal_run_started, ^run_id, _owner_node, _engine_pid}, 5_000

      wait_until(fn ->
        Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
      end)

      leased = Repo.one!(from(r in Run, where: r.id == ^run_id))
      assert %DateTime{} = leased.lease_expires_at
      assert DateTime.compare(leased.lease_expires_at, DateTime.utc_now()) == :gt

      assert :ok =
               peer_call(owner, Supervisor, :terminate_child, [
                 Continuum.Supervisor,
                 Continuum.Test.ClusterRuntimeSupervisor
               ])

      wait_until(fn ->
        run = Repo.one!(from(r in Run, where: r.id == ^run_id))
        is_nil(run.lease_owner) and is_nil(run.lease_token) and is_nil(run.lease_expires_at)
      end)

      assert {:ok, 1} =
               peer_call(replacement, Dispatcher, :dispatch_once, [
                 [owner: "replacement", batch_size: 1, ttl_seconds: 30]
               ])

      wait_until(fn ->
        peer_call(replacement, Registry, :lookup, [Continuum.Runtime.Registry, run_id]) != []
      end)

      assert :ok =
               peer_call(replacement, Continuum, :signal, [
                 run_id,
                 :continue,
                 :resumed,
                 [journal: Journal.Postgres]
               ])

      assert {:ok, %{state: :completed, result: :resumed}} =
               Engine.await(run_id, 5_000, journal: Journal.Postgres)
    after
      stop_peer(owner)
      stop_peer(replacement)
    end
  end
end
