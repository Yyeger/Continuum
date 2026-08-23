defmodule Continuum.WorkflowLogTest do
  use Continuum.Test.DataCase, async: false

  import ExUnit.CaptureLog

  defmodule LoggingFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      :ok = Continuum.log(input.level, input.message)
      {:ok, :logged}
    end
  end

  setup do
    Continuum.Test.reset_in_memory!()
    :ok
  end

  test "journals once and emits Logger and telemetry only at the live tail" do
    marker = "workflow-log-#{System.unique_integer([:positive])}"
    handler_id = "workflow-log-test-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :workflow, :log],
        fn event, measurements, metadata, test_pid ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    log =
      capture_log([level: :info], fn ->
        {:ok, run_id} =
          Continuum.Test.start_synchronous(LoggingFlow, %{level: :warning, message: marker})

        assert {:ok, %{state: :completed, result: {:ok, :logged}}} =
                 Continuum.await(run_id, 1_000)

        send(self(), {:history, Continuum.Test.history(run_id)})
      end)

    assert log =~ marker
    assert_receive {:history, [%{type: :workflow_log, level: :warning, message: ^marker} = event]}
    assert event.command_id

    assert_receive {:telemetry, [:continuum, :workflow, :log], %{count: 1}, metadata}
    assert metadata.level == :warning
    assert metadata.message == marker

    replay_log =
      capture_log([level: :info], fn ->
        assert {:ok, {:ok, :logged}} =
                 Continuum.Test.replay(LoggingFlow, %{level: :warning, message: marker}, [event])
      end)

    refute replay_log =~ marker
    refute_receive {:telemetry, [:continuum, :workflow, :log], _, _}

    {:ok, snapshot} =
      Continuum.Snapshot.compact(
        "workflow-log-snapshot",
        LoggingFlow.__continuum_workflow__().version_hash,
        [event]
      )

    snapshot_replay_log =
      capture_log([level: :info], fn ->
        assert {:ok, {:ok, :logged}} =
                 Continuum.Test.replay(
                   LoggingFlow,
                   %{level: :warning, message: marker},
                   [],
                   snapshot: snapshot
                 )
      end)

    refute snapshot_replay_log =~ marker
  end

  test "detects log content drift on replay" do
    event = %{
      type: :workflow_log,
      level: :info,
      message: "recorded",
      seq: 0
    }

    assert {:error, {:error, %Continuum.ReplayDriftError{}, _stacktrace}} =
             Continuum.Test.replay(
               LoggingFlow,
               %{level: :info, message: "changed"},
               [event]
             )
  end

  defmodule MetadataFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      :ok = Continuum.log(:info, input.message, order_id: input.order_id, attempt: 1)
      {:ok, :logged}
    end
  end

  describe "log/3" do
    test "journals metadata, passes it to Logger, and reports it in telemetry" do
      marker = "workflow-log-meta-#{System.unique_integer([:positive])}"
      handler_id = "workflow-log-meta-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler_id,
          [:continuum, :workflow, :log],
          fn event, measurements, metadata, test_pid ->
            send(test_pid, {:telemetry, event, measurements, metadata})
          end,
          self()
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      capture_log([level: :info], fn ->
        {:ok, run_id} =
          Continuum.Test.start_synchronous(MetadataFlow, %{message: marker, order_id: "o-9"})

        assert {:ok, %{state: :completed}} = Continuum.await(run_id, 1_000)
        send(self(), {:history, Continuum.Test.history(run_id)})
      end)

      assert_receive {:history, [event]}
      assert event.metadata == [order_id: "o-9", attempt: 1]

      assert_receive {:telemetry, [:continuum, :workflow, :log], _measurements, metadata}
      assert metadata.metadata == [order_id: "o-9", attempt: 1]
    end

    test "replays the journaled event and treats changed metadata as drift" do
      input = %{message: "steady", order_id: "o-1"}

      {{:ok, run_id}, _log} =
        with_log(fn -> Continuum.Test.start_synchronous(MetadataFlow, input) end)

      {:ok, _} = Continuum.await(run_id, 1_000)
      history = Continuum.Test.history(run_id)

      assert {:ok, {:ok, :logged}} = Continuum.Test.replay(MetadataFlow, input, history)

      assert {:error, {:error, %Continuum.ReplayDriftError{}, _stack}} =
               Continuum.Replay.run(MetadataFlow, %{input | order_id: "o-2"}, history)
    end

    test "survives snapshotting" do
      input = %{message: "snapshotted", order_id: "o-3"}

      {{:ok, run_id}, _log} =
        with_log(fn -> Continuum.Test.start_synchronous(MetadataFlow, input) end)

      {:ok, _} = Continuum.await(run_id, 1_000)
      history = Continuum.Test.history(run_id)

      {:ok, snapshot} =
        Continuum.Snapshot.compact(
          run_id,
          MetadataFlow.__continuum_workflow__().version_hash,
          history
        )

      {result, _log} =
        with_log(fn -> Continuum.Test.replay(MetadataFlow, input, [], snapshot: snapshot) end)

      assert result == {:ok, {:ok, :logged}}
    end

    test "a log/2 call still journals an event with no metadata key" do
      marker = "workflow-log-compat-#{System.unique_integer([:positive])}"

      capture_log(fn ->
        {:ok, run_id} =
          Continuum.Test.start_synchronous(LoggingFlow, %{level: :info, message: marker})

        {:ok, _} = Continuum.await(run_id, 1_000)
        send(self(), {:history, Continuum.Test.history(run_id)})
      end)

      assert_receive {:history, [event]}
      refute Map.has_key?(event, :metadata)
    end

    test "a pre-log/3 history still replays against a log/2 call site" do
      legacy = %{type: :workflow_log, level: :info, message: "legacy", seq: 0}

      assert {:ok, {:ok, :logged}} =
               Continuum.Test.replay(LoggingFlow, %{level: :info, message: "legacy"}, [legacy])
    end
  end

  test "validates level and bounded binary messages" do
    assert_raise ArgumentError, ~r/invalid workflow log level/, fn ->
      run_direct_log(:verbose, "message")
    end

    assert_raise ArgumentError, ~r/must be a binary/, fn ->
      run_direct_log(:info, %{message: "no"})
    end

    assert_raise ArgumentError, ~r/exceeds 16 KiB/, fn ->
      run_direct_log(:info, String.duplicate("x", 16_385))
    end

    assert_raise ArgumentError, ~r/must be a keyword list/, fn ->
      run_direct_log(:info, "message", %{order: 1})
    end

    assert_raise Continuum.DurableTermError, ~r/non-durable PID/, fn ->
      run_direct_log(:info, "message", pid: self())
    end
  end

  defp run_direct_log(level, message, metadata \\ []) do
    context = %Continuum.Runtime.Context{
      run_id: "log-validation",
      workflow_module: LoggingFlow,
      journal: Continuum.Runtime.Journal.InMemory,
      history: []
    }

    Continuum.Runtime.Context.put(context)

    try do
      Continuum.__log__(level, message, metadata, {:test, :log})
    after
      Continuum.Runtime.Context.clear()
    end
  end
end
