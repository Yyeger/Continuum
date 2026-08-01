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
  end

  defp run_direct_log(level, message) do
    context = %Continuum.Runtime.Context{
      run_id: "log-validation",
      workflow_module: LoggingFlow,
      journal: Continuum.Runtime.Journal.InMemory,
      history: []
    }

    Continuum.Runtime.Context.put(context)

    try do
      Continuum.__log__(level, message, {:test, :log})
    after
      Continuum.Runtime.Context.clear()
    end
  end
end
