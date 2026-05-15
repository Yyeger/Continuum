defmodule SnapshotBench.Flow do
  use Continuum.Workflow, version: 1

  def run(input) do
    result =
      Enum.reduce(input.steps, 0, fn step, acc ->
        Continuum.side_effect(fn -> acc + step end)
      end)

    {:ok, result}
  end
end

defmodule SnapshotBench do
  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.InMemory

  def run(event_count \\ 10_000) do
    Continuum.Test.reset_in_memory!()
    steps = Enum.to_list(1..event_count)
    input = %{steps: steps}
    expected = {:ok, div(event_count * (event_count + 1), 2)}

    {prepare_us, history} =
      :timer.tc(fn ->
        {:ok, run_id} = Continuum.Test.start_synchronous(SnapshotBench.Flow, input)
        {:ok, %{state: :completed, result: ^expected}} = Continuum.await(run_id, 30_000)
        InMemory.load(Instance.default(), run_id)
      end)

    {compact_us, {:ok, snapshot}} =
      :timer.tc(fn ->
        Continuum.Snapshot.compact(
          "bench-run",
          SnapshotBench.Flow.__continuum_workflow__().version_hash,
          history
        )
      end)

    {raw_replay_us, {:ok, ^expected}} =
      :timer.tc(fn ->
        Continuum.Test.replay(SnapshotBench.Flow, input, history)
      end)

    {snapshot_replay_us, {:ok, ^expected}} =
      :timer.tc(fn ->
        Continuum.Test.replay(SnapshotBench.Flow, input, [], snapshot: snapshot)
      end)

    IO.inspect(%{
      events: event_count,
      prepare_history_ms: div(prepare_us, 1_000),
      compact_ms: div(compact_us, 1_000),
      raw_replay_ms: div(raw_replay_us, 1_000),
      snapshot_replay_ms: div(snapshot_replay_us, 1_000),
      replay_speedup: Float.round(raw_replay_us / max(snapshot_replay_us, 1), 1),
      snapshot_size_bytes: byte_size(Continuum.Snapshot.encode(snapshot)),
      compacted_steps: map_size(snapshot.steps_by_seq)
    })
  end
end

event_count =
  case System.argv() do
    [value] -> String.to_integer(value)
    _ -> 10_000
  end

SnapshotBench.run(event_count)
