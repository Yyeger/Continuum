defmodule Continuum.ReplayTest do
  @moduledoc """
  End-to-end replay tests against the in-memory journal.

  Each test starts a workflow, drives it through some signals, kills the
  workflow process, and asserts that loading the journal and re-running the
  workflow takes the same branches and arrives at the same final state.
  """

  use ExUnit.Case, async: false

  alias Continuum.Runtime.Journal.InMemory

  describe "happy-path execution + replay" do
    defmodule TwoStepFlow do
      use Continuum.Workflow, version: 1

      def run(input) do
        n = Continuum.side_effect(fn -> input.seed * 2 end)
        m = Continuum.side_effect(fn -> n + 1 end)
        {:ok, m}
      end
    end

    test "executes synchronously via side_effect chain" do
      {:ok, run_id} = Continuum.start(TwoStepFlow, %{seed: 5})
      assert {:ok, %{state: :completed, result: {:ok, 11}}} = Continuum.await(run_id, 1_000)
    end

    test "replaying the same history produces the same result" do
      {:ok, run_id} = Continuum.start(TwoStepFlow, %{seed: 5})
      {:ok, _} = Continuum.await(run_id, 1_000)

      events = InMemory.load(Continuum.Runtime.Instance.default(), run_id)
      assert length(events) == 2
      assert Enum.all?(events, &(&1.type == :side_effect))
      assert Enum.all?(events, &(is_tuple(&1.command_id) and tuple_size(&1.command_id) >= 5))
      assert Enum.uniq_by(events, & &1.command_id) == events

      # Manually replay against a fresh context.
      ctx = %Continuum.Runtime.Context{
        run_id: "replay-test",
        history: events,
        cursor: 0,
        workflow_module: TwoStepFlow,
        lease_token: nil,
        journal: InMemory
      }

      Continuum.Runtime.Context.put(ctx)

      try do
        result = TwoStepFlow.run(%{seed: 5})
        assert result == {:ok, 11}
      after
        Continuum.Runtime.Context.clear()
      end
    end

    test "replaying a side_effect does NOT invoke the producer again" do
      events = [
        %{type: :side_effect, kind: :user, payload: 42, seq: 0}
      ]

      ctx = %Continuum.Runtime.Context{
        run_id: "isolated-replay",
        history: events,
        cursor: 0,
        workflow_module: TwoStepFlow,
        lease_token: nil,
        journal: InMemory
      }

      Continuum.Runtime.Context.put(ctx)

      try do
        # Producer would crash if called — proving replay returned the journaled value.
        producer = fn -> raise "producer must not run on replay" end
        assert 42 == Continuum.Runtime.Effect.run({:side_effect, :user}, producer)
      after
        Continuum.Runtime.Context.clear()
      end
    end
  end

  describe "signal-driven branching" do
    defmodule SignalBranchFlow do
      use Continuum.Workflow, version: 1

      def run(_input) do
        case await(signal(:decision)) do
          :go -> {:ok, :went}
          :stop -> {:error, :stopped}
        end
      end
    end

    test "an :approved signal drives the workflow to :went" do
      {:ok, run_id} = Continuum.start(SignalBranchFlow, %{})
      :ok = Continuum.signal(run_id, :decision, :go)
      assert {:ok, %{state: :completed, result: {:ok, :went}}} = Continuum.await(run_id, 1_000)
    end

    test "a :rejected signal drives the workflow to :stopped" do
      {:ok, run_id} = Continuum.start(SignalBranchFlow, %{})
      :ok = Continuum.signal(run_id, :decision, :stop)

      assert {:ok, %{state: :completed, result: {:error, :stopped}}} =
               Continuum.await(run_id, 1_000)
    end
  end

  describe "replay drift detection" do
    defmodule DriftActivity do
      def run(value), do: {:ok, value}
    end

    defmodule DriftFlow do
      use Continuum.Workflow, version: 1

      def run(_input) do
        Continuum.side_effect(fn -> :first end)
      end
    end

    defmodule ActivityDriftFlow do
      use Continuum.Workflow, version: 1

      def run(input) do
        activity(DriftActivity.run(input.value))
      end
    end

    defmodule BuiltinCommandSourceFlow do
      use Continuum.Workflow, version: 1

      def run(_input) do
        Continuum.now()
      end
    end

    defmodule BuiltinCommandChangedFlow do
      use Continuum.Workflow, version: 1

      def run(_input) do
        Continuum.now()
      end
    end

    defmodule CommandIdSourceFlow do
      use Continuum.Workflow, version: 1

      def run(_input) do
        Continuum.side_effect(fn -> :original end)
      end
    end

    defmodule CommandIdChangedFlow do
      use Continuum.Workflow, version: 1

      def run(_input) do
        Continuum.side_effect(fn -> :changed end)
      end
    end

    defmodule SnapshotPrefixFlow do
      use Continuum.Workflow, version: 1

      def run(input) do
        first = Continuum.side_effect(fn -> input.seed * 2 end)
        Continuum.side_effect(fn -> first + 1 end)
      end
    end

    test "raises ReplayDriftError when journaled type doesn't match the requested effect" do
      # Forge a journal where the first event is a signal — but the workflow
      # asks for a side_effect. This simulates an incompatible code change.
      events = [%{type: :signal_received, name: :foo, payload: :bar, seq: 0}]

      ctx = %Continuum.Runtime.Context{
        run_id: "drift",
        history: events,
        cursor: 0,
        workflow_module: DriftFlow,
        lease_token: nil,
        journal: InMemory
      }

      Continuum.Runtime.Context.put(ctx)

      try do
        assert_raise Continuum.ReplayDriftError, fn ->
          DriftFlow.run(%{})
        end
      after
        Continuum.Runtime.Context.clear()
      end
    end

    test "raises ReplayDriftError when scheduled activity is followed by an unexpected event" do
      events = [
        %{
          type: :activity_scheduled,
          task_id: "task-1",
          mfa: {DriftActivity, :run, [1]},
          seq: 0
        },
        %{type: :timer_fired, timer_id: "timer-1", seq: 1}
      ]

      ctx = %Continuum.Runtime.Context{
        run_id: "activity-drift",
        history: events,
        cursor: 0,
        workflow_module: ActivityDriftFlow,
        lease_token: nil,
        journal: InMemory
      }

      Continuum.Runtime.Context.put(ctx)

      try do
        assert_raise Continuum.ReplayDriftError, fn ->
          ActivityDriftFlow.run(%{value: 1})
        end
      after
        Continuum.Runtime.Context.clear()
      end
    end

    test "raises ReplayDriftError when command identity changes but event shape still matches" do
      {:ok, run_id} = Continuum.start(CommandIdSourceFlow, %{})
      {:ok, _} = Continuum.await(run_id, 1_000)
      events = InMemory.load(Continuum.Runtime.Instance.default(), run_id)

      ctx = %Continuum.Runtime.Context{
        run_id: "command-id-drift",
        history: events,
        cursor: 0,
        workflow_module: CommandIdChangedFlow,
        lease_token: nil,
        journal: InMemory
      }

      Continuum.Runtime.Context.put(ctx)

      try do
        assert_raise Continuum.ReplayDriftError, fn ->
          CommandIdChangedFlow.run(%{})
        end
      after
        Continuum.Runtime.Context.clear()
      end
    end

    test "raises ReplayDriftError when builtin primitive command identity changes" do
      {:ok, run_id} = Continuum.start(BuiltinCommandSourceFlow, %{})
      {:ok, _} = Continuum.await(run_id, 1_000)
      events = InMemory.load(Continuum.Runtime.Instance.default(), run_id)

      assert [%{type: :side_effect, kind: :now, command_id: command_id}] = events

      assert {:side_effect, BuiltinCommandSourceFlow, {:run, 1}, _line, _shape_hash, 0} =
               command_id

      ctx = %Continuum.Runtime.Context{
        run_id: "builtin-command-id-drift",
        history: events,
        cursor: 0,
        workflow_module: BuiltinCommandChangedFlow,
        lease_token: nil,
        journal: InMemory
      }

      Continuum.Runtime.Context.put(ctx)

      try do
        assert_raise Continuum.ReplayDriftError, fn ->
          BuiltinCommandChangedFlow.run(%{})
        end
      after
        Continuum.Runtime.Context.clear()
      end
    end

    test "raises ReplayDriftError after a compacted snapshot prefix" do
      {:ok, run_id} = Continuum.start(SnapshotPrefixFlow, %{seed: 5})
      {:ok, _} = Continuum.await(run_id, 1_000)
      [first_event, _second_event] = InMemory.load(Continuum.Runtime.Instance.default(), run_id)

      {:ok, snapshot} =
        Continuum.Snapshot.compact(
          "snapshot-drift",
          SnapshotPrefixFlow.__continuum_workflow__().version_hash,
          [first_event]
        )

      ctx = %Continuum.Runtime.Context{
        run_id: "snapshot-drift",
        history: [%{type: :signal_received, name: :oops, payload: :bad, seq: 1}],
        history_offset: 1,
        snapshot_steps: snapshot.steps_by_seq,
        cursor: 0,
        workflow_module: SnapshotPrefixFlow,
        lease_token: nil,
        journal: InMemory
      }

      Continuum.Runtime.Context.put(ctx)

      try do
        assert_raise Continuum.ReplayDriftError, fn ->
          SnapshotPrefixFlow.run(%{seed: 5})
        end
      after
        Continuum.Runtime.Context.clear()
      end
    end
  end
end
