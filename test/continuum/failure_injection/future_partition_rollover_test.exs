defmodule Continuum.FailureInjection.FuturePartitionRolloverTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Partitions

  @moduletag :failure_injection

  @future_month ~D[2098-11-01]
  @future_partition "continuum_events_y2098_m11"

  test "a simulated future database month rolls overflow events into its partition" do
    drop_partition()

    try do
      run_id = Ecto.UUID.generate()

      Repo.query!(
        """
        INSERT INTO continuum_events (run_id, seq, event_type, payload, inserted_at)
        VALUES ($1, 0, 'side_effect', $2, $3::timestamptz)
        """,
        [
          dump_uuid(run_id),
          :erlang.term_to_binary(%{payload: :future_clock}),
          ~U[2098-11-17 12:00:00Z]
        ]
      )

      assert event_partition(run_id) == "continuum_events_default"

      assert {:ok, summary} =
               Partitions.ensure(repo: Repo, start_month: @future_month, months: 1)

      assert summary.created == [@future_partition]
      assert summary.moved_row_count == 1
      assert event_partition(run_id) == @future_partition
    after
      drop_partition()
    end
  end

  defp event_partition(run_id) do
    %{rows: [[partition]]} =
      Repo.query!(
        "SELECT tableoid::regclass::text FROM continuum_events WHERE run_id = $1",
        [dump_uuid(run_id)]
      )

    partition
  end

  defp drop_partition do
    Repo.query!("DROP TABLE IF EXISTS \"#{@future_partition}\"")
  end

  defp dump_uuid(run_id) do
    {:ok, dumped} = Ecto.UUID.dump(run_id)
    dumped
  end
end
