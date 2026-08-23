defmodule Continuum.Integration.PublicCrashResumeTest do
  @moduledoc """
  The crash-resume loop written the way a user can write it.

  Deliberately reaches for nothing under `Continuum.Runtime.*`: if this test
  needs an internal module, the public test surface is incomplete.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Test

  defmodule Step do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(value), do: {:ok, value + 1}
  end

  defmodule ActivityTimerActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, first} = activity(Step.run(input.seed))
      timer(days(30))
      {:ok, second} = activity(Step.run(first))
      {:ok, second}
    end
  end

  test "a run survives losing its engine mid-flight and finishes on another" do
    {:ok, run_id} = Test.start_postgres(ActivityTimerActivityFlow, %{seed: 40})

    # Get as far as the 30-day timer, then lose the node holding the run.
    :ok = Test.drive_until_state(run_id, [:suspended])
    assert Test.history(run_id, journal: :postgres) != []

    :ok = Test.crash!(run_id)
    :ok = Test.expire_lease!(run_id)
    :ok = Test.elapse_timers!(run_id)

    assert {:ok, %{state: :completed, result: {:ok, 42}}} = Test.drive(run_id)

    assert event_types(run_id) == [
             :activity_scheduled,
             :activity_completed,
             :timer_started,
             :timer_fired,
             :activity_scheduled,
             :activity_completed
           ]
  end

  defmodule WaitingFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: {:ok, await(signal(:go))}
  end

  test "crash! reports when there is no engine to kill" do
    {:ok, run_id} = Test.start_postgres(WaitingFlow, %{})
    :ok = Test.drive_until_state(run_id, [:suspended])
    :ok = Test.crash!(run_id)

    assert {:error, :no_engine} = Test.crash!(run_id)
  end

  defp event_types(run_id) do
    run_id
    |> Test.history(journal: :postgres)
    |> Enum.map(& &1.type)
  end
end
