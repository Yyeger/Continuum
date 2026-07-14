defmodule Continuum.Cluster.LostWakeTest do
  use Continuum.Test.ClusterCase, async: false

  alias Continuum.Runtime.SignalRouter

  test "a remote activity commit is recovered without its immediate wake" do
    owner = start_peer!(:wake_owner)
    writer = start_peer!(:wake_writer)

    try do
      run_id =
        peer_call(owner, Continuum.Test.ClusterScenarios, :start_activity_run, [
          %{value: 7},
          [lease_ttl_seconds: 30]
        ])

      wait_until(fn ->
        Repo.one(from(r in Run, where: r.id == ^run_id))
        |> then(&match?(%Run{state: "suspended"}, &1))
      end)

      task = Repo.one!(from(t in ActivityTask, where: t.run_id == ^run_id))

      assert [{engine_pid, _}] =
               peer_call(owner, Registry, :lookup, [Continuum.Runtime.Registry, run_id])

      assert node(engine_pid) == owner.node

      assert :ok =
               peer_call(
                 writer,
                 Continuum.Test.ClusterScenarios,
                 :complete_activity_without_wake,
                 [
                   task.id,
                   task.attempt,
                   {:ok, 14}
                 ]
               )

      run = Repo.one!(from(r in Run, where: r.id == ^run_id))
      assert %DateTime{} = run.next_wakeup_at
      assert run.state == "suspended"

      assert :ok = peer_call(owner, SignalRouter, :catch_up_once, [[]])

      assert {:ok, %{state: :completed, result: {:ok, 14}}} =
               Engine.await(run_id, 5_000, journal: Journal.Postgres)

      assert ["activity_scheduled", "activity_completed"] = event_types(run_id)
    after
      stop_peer(owner)
      stop_peer(writer)
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
