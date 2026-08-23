defmodule Continuum.Runtime.SnapshotterBoundTest do
  @moduledoc """
  The per-run counter is bounded.

  Every path other than "a snapshot was taken" used to keep its entry forever,
  including `threshold == :infinity` and every run that completes below its
  threshold — which is most runs. The counter is only an optimisation, so it is
  bounded rather than reference-counted.
  """

  use ExUnit.Case, async: false

  alias Continuum.Runtime.Snapshotter

  defmodule CountingFlow do
    use Continuum.Workflow, version: 1, snapshot_threshold: 1_000_000

    def run(_input), do: {:ok, :done}
  end

  setup do
    Continuum.Test.reset_in_memory!()
    :ok
  end

  test "tracking stays bounded across far more runs than the cap" do
    name = :"snapshotter_bound_#{System.unique_integer([:positive])}"
    instance = %{Continuum.Runtime.Instance.default() | snapshotter: name}

    pid =
      start_supervised!(
        {Snapshotter, instance: instance, snapshot_threshold: 1_000_000},
        id: name
      )

    for n <- 1..25_000 do
      GenServer.call(pid, {:maybe_snapshot, "run-#{n}", nil, Continuum.Runtime.Journal.InMemory})
    end

    state = :sys.get_state(pid)
    tracked = map_size(state.run_counts) + map_size(state.previous_run_counts)

    assert tracked <= 20_000
    assert tracked < 25_000
  end

  test "a run's count survives a generation rotation" do
    name = :"snapshotter_promote_#{System.unique_integer([:positive])}"
    instance = %{Continuum.Runtime.Instance.default() | snapshotter: name}

    pid =
      start_supervised!(
        {Snapshotter, instance: instance, snapshot_threshold: 1_000_000},
        id: name
      )

    GenServer.call(pid, {:maybe_snapshot, "kept", nil, Continuum.Runtime.Journal.InMemory})

    for n <- 1..10_000 do
      GenServer.call(pid, {:maybe_snapshot, "run-#{n}", nil, Continuum.Runtime.Journal.InMemory})
    end

    # "kept" has been rotated into the previous generation, not dropped.
    GenServer.call(pid, {:maybe_snapshot, "kept", nil, Continuum.Runtime.Journal.InMemory})

    assert %{count: 2} = :sys.get_state(pid).run_counts["kept"]
  end
end
