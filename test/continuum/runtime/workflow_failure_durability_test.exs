defmodule Continuum.Runtime.WorkflowFailureDurabilityTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.{DurableTerm, DurableTermError, RunFailure}
  alias Continuum.Runtime.{Engine, Instance}
  alias Continuum.Runtime.Journal.Postgres

  defmodule FunctionResult do
    use Continuum.Workflow
    def run(_input), do: fn -> :ok end
  end

  defmodule FunctionFailure do
    use Continuum.Workflow
    def run(_input), do: throw({:bad_value, fn -> :ok end})
  end

  test "invalid workflow returns become persisted terminal failures" do
    {:ok, run_id} = Engine.start_run(FunctionResult, %{}, journal: Postgres)

    assert {:error, %{state: :failed, error: %RunFailure{reason: %DurableTermError{}}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert %{state: :failed, error: %RunFailure{reason: %DurableTermError{}}} =
             Postgres.get_run(Instance.default(), run_id)
  end

  test "nondurable thrown reasons persist with the same public failure" do
    {:ok, run_id} = Engine.start_run(FunctionFailure, %{}, journal: Postgres)

    assert {:error, %{state: :failed, error: %RunFailure{kind: :throw} = failure}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert is_binary(failure.reason)
    assert failure.reason =~ "bad_value"

    assert %{error: ^failure, error_stacktrace: stacktrace} =
             Postgres.get_run(Instance.default(), run_id)

    assert :ok = DurableTerm.validate(stacktrace)
  end

  test "failure normalization bounds unsafe stack arguments and oversized reasons" do
    stacktrace = [{__MODULE__, :example, [self()], []}]
    {failure, stacktrace} = RunFailure.split({:error, self(), stacktrace})
    assert :ok = DurableTerm.validate({failure, stacktrace})
    assert is_binary(failure.reason)

    {failure, _} = RunFailure.split({:throw, String.duplicate("x", 100_000)})
    assert byte_size(failure.reason) <= 4_099
  end
end
