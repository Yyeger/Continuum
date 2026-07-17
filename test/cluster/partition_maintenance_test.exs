defmodule Continuum.Cluster.PartitionMaintenanceTest do
  use Continuum.Test.ClusterCase, async: false

  @month ~D[2099-01-01]
  @partitions ~w(continuum_events_y2099_m01 continuum_events_y2099_m02)

  test "two nodes concurrently ensure one partition horizon" do
    Enum.each(@partitions, &Repo.query!("DROP TABLE IF EXISTS \"#{&1}\""))
    peer_a = start_peer!(:partition_a)
    peer_b = start_peer!(:partition_b)

    try do
      tasks =
        Enum.map([peer_a, peer_b], fn peer ->
          Task.async(fn ->
            peer_call(
              peer,
              Continuum.Partitions,
              :ensure,
              [[instance: Continuum, start_month: @month, months: 2]],
              30_000
            )
          end)
        end)

      results = Enum.map(tasks, &Task.await(&1, 35_000))

      assert Enum.all?(results, &match?({:ok, %{status: :ok}}, &1))

      assert 2 ==
               Enum.sum(
                 for {:ok, summary} <- results do
                   length(summary.created)
                 end
               )

      Enum.each(@partitions, fn partition ->
        assert %{rows: [[true]]} =
                 Repo.query!("SELECT to_regclass($1) IS NOT NULL", [partition])
      end)
    after
      stop_peer(peer_a)
      stop_peer(peer_b)
      Enum.each(@partitions, &Repo.query!("DROP TABLE IF EXISTS \"#{&1}\""))
    end
  end
end
