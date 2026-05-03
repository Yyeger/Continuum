defmodule Continuum.TelemetryTest do
  use ExUnit.Case, async: false

  defmodule TelemetryFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      Continuum.side_effect(fn -> {:ok, input.value + 1} end)
    end
  end

  test "documents stable event names" do
    assert [:continuum, :run, :completed] in Continuum.Telemetry.events()
    assert [:continuum, :activity, :scheduled] in Continuum.Telemetry.events()
    assert [:continuum, :dispatcher, :claimed] in Continuum.Telemetry.events()
  end

  test "emits run lifecycle events" do
    handler_id = {__MODULE__, self(), make_ref()}
    parent = self()

    events = [
      [:continuum, :run, :started],
      [:continuum, :run, :completed]
    ]

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    try do
      Continuum.Test.reset_in_memory!()
      {:ok, run_id} = Continuum.Test.start_in_memory(TelemetryFlow, %{value: 2})

      assert {:ok, %{state: :completed, result: {:ok, 3}}} = Continuum.await(run_id, 1_000)
      assert_receive {:telemetry, [:continuum, :run, :started], %{}, %{run_id: ^run_id}}
      assert_receive {:telemetry, [:continuum, :run, :completed], %{}, %{run_id: ^run_id}}
    after
      :telemetry.detach(handler_id)
    end
  end
end
