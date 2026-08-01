defmodule ContinuumDownstream.ConsumerTest do
  use ExUnit.Case, async: false

  defmodule EchoFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: {:ok, input}
  end

  test "starts and awaits a workflow through the packaged public API" do
    {:ok, run_id} = Continuum.start(EchoFlow, %{source: :downstream})

    assert {:ok, %{state: :completed, result: {:ok, %{source: :downstream}}}} =
             Continuum.await(run_id, 1_000)
  end
end
