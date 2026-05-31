defmodule Continuum.Runtime.SnapshotterTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{ActivityWorker.Dispatcher, Instance, Journal.Postgres, Snapshotter}
  alias Continuum.Schema.{ActivityTask, Run, Snapshot}

  defmodule SnapshotFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      result =
        Enum.reduce(input.steps, 0, fn step, acc ->
          Continuum.side_effect(fn -> acc + step end)
        end)

      {:ok, result}
    end
  end

  defmodule LargeSnapshotFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      Continuum.side_effect(fn -> String.duplicate("x", 1_000) end)
    end
  end

  defmodule SnapshotActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(value), do: {:ok, value * 2}
  end

  defmodule ActivitySnapshotFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, value} = activity(SnapshotActivity.run(input.seed))
      {:ok, value + 1}
    end
  end

  test "writes a compacted snapshot and replays from it" do
    steps = [1, 2, 3, 4, 5, 6]

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SnapshotFlow, %{steps: steps}, journal: Postgres)

    assert {:ok, %{state: :completed, result: {:ok, 21}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    :ok =
      Snapshotter.snapshot_once(Instance.default(), run_id,
        snapshot_threshold: 5,
        lease_token: lease_token(run_id)
      )

    row = Repo.one!(Snapshot)
    snapshot = Continuum.Snapshot.decode(row.payload)

    assert row.through_seq == 5
    assert row.format_version == Continuum.Snapshot.format_version()
    assert snapshot.through_seq == 5
    assert map_size(snapshot.steps_by_seq) == 6

    assert {^snapshot, []} =
             Postgres.load_with_snapshot(Instance.default(), run_id, lease_token(run_id))

    assert {:ok, {:ok, 21}} =
             Continuum.Test.replay(SnapshotFlow, %{steps: steps}, [],
               journal: Postgres,
               snapshot: snapshot
             )
  end

  test "compacts a Postgres activity scheduled/completed pair and emits taken telemetry" do
    handler_id = {__MODULE__, self(), make_ref()}
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :snapshot, :taken],
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivitySnapshotFlow, %{seed: 5}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 1 end)
    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "snapshot-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 11}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    :ok =
      Snapshotter.snapshot_once(Instance.default(), run_id,
        snapshot_threshold: 1,
        lease_token: lease_token(run_id)
      )

    row = Repo.one!(Snapshot)
    snapshot = Continuum.Snapshot.decode(row.payload)

    assert snapshot.through_seq == 1

    assert %{
             advance_by: 2,
             command_id: command_id,
             effect_type: :activity,
             result: {:ok, 10},
             shape: {SnapshotActivity, :run, 1}
           } = Map.fetch!(snapshot.steps_by_seq, 0)

    assert is_tuple(command_id)
    assert_receive {:telemetry, [:continuum, :snapshot, :taken], measurements, metadata}
    assert measurements.event_count == 1
    assert metadata.run_id == run_id
    assert metadata.through_seq == 1
    assert metadata.format_version == Continuum.Snapshot.format_version()
    assert metadata.compacted_prefix_length == 1

    assert {:ok, {:ok, 11}} =
             Continuum.Test.replay(ActivitySnapshotFlow, %{seed: 5}, [],
               journal: Postgres,
               snapshot: snapshot
             )
  end

  test "oversized snapshots are skipped and emit telemetry" do
    handler_id = {__MODULE__, self(), make_ref()}
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :snapshot, :skipped],
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(LargeSnapshotFlow, %{}, journal: Postgres)

    assert {:ok, %{state: :completed, result: result}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert byte_size(result) == 1_000

    :ok =
      Snapshotter.snapshot_once(Instance.default(), run_id,
        snapshot_threshold: 1,
        snapshot_max_size_bytes: 100,
        lease_token: lease_token(run_id)
      )

    assert Repo.aggregate(Snapshot, :count) == 0
    assert_receive {:telemetry, [:continuum, :snapshot, :skipped], %{}, metadata}
    assert {:snapshot_too_large, _actual, 100} = metadata.reason
  end

  test "an incompatible snapshot can be discarded while full history still replays" do
    steps = [3, 4]

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SnapshotFlow, %{steps: steps}, journal: Postgres)

    assert {:ok, %{state: :completed, result: {:ok, 7}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    :ok =
      Snapshotter.snapshot_once(Instance.default(), run_id,
        snapshot_threshold: 1,
        lease_token: lease_token(run_id)
      )

    row = Repo.one!(Snapshot)
    bad_snapshot = %{Continuum.Snapshot.decode(row.payload) | version_hash: <<1::256>>}
    full_history = Postgres.load(Instance.default(), run_id)

    assert {:ok, {:ok, 7}} =
             Continuum.Test.replay(SnapshotFlow, %{steps: steps}, full_history,
               journal: Postgres,
               snapshot: bad_snapshot
             )
  end

  test "compaction rejects paired events without command identity" do
    events = [
      %{type: :activity_scheduled, mfa: {SnapshotActivity, :run, [5]}, seq: 0},
      %{type: :activity_completed, mfa: {SnapshotActivity, :run, [5]}, payload: {:ok, 10}, seq: 1}
    ]

    assert {:error, {:missing_command_id, 0}} =
             Continuum.Snapshot.compact(
               "missing-command",
               ActivitySnapshotFlow.__continuum_workflow__().version_hash,
               events
             )
  end

  defp lease_token(run_id) do
    Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))
  end

  defp assert_eventually(fun, attempts \\ 20)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
