defmodule ReplayHotPathBench.Activity do
  def add(acc, step), do: acc + step
  def ok(step), do: {:ok, step}
end

defmodule ReplayHotPathBench.Compensation do
  def undo(step), do: {:undone, step}
end

defmodule ReplayHotPathBench.Flow do
  use Continuum.Workflow, version: 1

  def run(input) do
    result =
      input.ops
      |> Enum.with_index(1)
      |> Enum.reduce(0, fn
        {{:side_effect, step}, _index}, acc ->
          Continuum.side_effect(fn -> acc + step end)

        {{:activity, step}, _index}, acc ->
          activity(ReplayHotPathBench.Activity.add(acc, step))

        {{:patched, step}, _index}, acc ->
          if Continuum.patched?(:replay_hot_path_bench) do
            acc + step
          else
            acc
          end

        {{:compensated, step}, _index}, acc ->
          {:ok, ref} =
            activity(ReplayHotPathBench.Activity.ok(step),
              compensate: {ReplayHotPathBench.Compensation, :undo, [step]}
            )

          acc + ref.result
      end)

    compensate_all()
    {:ok, result}
  end
end

defmodule ReplayHotPathBench do
  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.InMemory

  @cycle [:side_effect, :activity, :patched, :compensated]

  def run(op_count \\ 10_000) do
    Continuum.Test.reset_in_memory!()

    ops =
      1..op_count
      |> Enum.map(fn index -> {Enum.at(@cycle, rem(index - 1, length(@cycle))), 1} end)

    input = %{ops: ops}
    expected = {:ok, op_count}

    {prepare_us, history} =
      :timer.tc(fn ->
        {:ok, run_id} = Continuum.Test.start_synchronous(ReplayHotPathBench.Flow, input)
        {:ok, %{state: :completed, result: ^expected}} = Continuum.await(run_id, 30_000)
        InMemory.load(Instance.default(), run_id)
      end)

    {raw_replay_us, {:ok, ^expected}} =
      :timer.tc(fn ->
        Continuum.Test.replay(ReplayHotPathBench.Flow, input, history)
      end)

    {compact_us, {:ok, snapshot}} =
      :timer.tc(fn ->
        Continuum.Snapshot.compact(
          "replay-hot-path-bench",
          ReplayHotPathBench.Flow.__continuum_workflow__().version_hash,
          history
        )
      end)

    remaining = Enum.drop(history, snapshot.through_seq + 1)

    {snapshot_replay_us, {:ok, ^expected}} =
      :timer.tc(fn ->
        Continuum.Test.replay(ReplayHotPathBench.Flow, input, remaining, snapshot: snapshot)
      end)

    event_count = length(history)

    IO.inspect(%{
      logical_ops: op_count,
      events: event_count,
      prepare_history_ms: div(prepare_us, 1_000),
      raw_replay_ms: div(raw_replay_us, 1_000),
      raw_replay_us_per_event: Float.round(raw_replay_us / max(event_count, 1), 2),
      compact_ms: div(compact_us, 1_000),
      snapshot_replay_ms: div(snapshot_replay_us, 1_000),
      snapshot_replay_us_per_step:
        Float.round(snapshot_replay_us / max(map_size(snapshot.steps_by_seq), 1), 2),
      snapshot_speedup: Float.round(raw_replay_us / max(snapshot_replay_us, 1), 1),
      compacted_steps: map_size(snapshot.steps_by_seq)
    })
  end
end

op_count =
  case System.argv() do
    [value] -> String.to_integer(value)
    _ -> 10_000
  end

ReplayHotPathBench.run(op_count)
