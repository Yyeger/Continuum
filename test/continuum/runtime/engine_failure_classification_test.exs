defmodule Continuum.Runtime.EngineFailureClassificationTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Continuum.Runtime.{Engine, Instance}
  alias Continuum.Runtime.Journal.InMemory

  defmodule TrivialFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: {:ok, :done}
  end

  defmodule FlakyCompleteJournal do
    @moduledoc """
    Delegates to the in-memory journal but fails every `complete!` with a
    transient connection error — the run's logic succeeded, only the final
    journal write did not.
    """

    @behaviour Continuum.Runtime.Journal

    defdelegate start_run(instance, run_id, workflow, input), to: InMemory
    defdelegate append!(instance, run_id, event, lease_token), to: InMemory
    defdelegate load(instance, run_id), to: InMemory
    defdelegate load_with_snapshot(instance, run_id, lease_token), to: InMemory
    defdelegate take_snapshot!(instance, snapshot), to: InMemory
    defdelegate suspend!(instance, run_id, lease_token), to: InMemory
    defdelegate fail!(instance, run_id, error, lease_token), to: InMemory
    defdelegate get_run(instance, run_id), to: InMemory

    def complete!(_instance, _run_id, _result, _lease_token) do
      raise DBConnection.ConnectionError, "tcp recv: closed"
    end
  end

  setup do
    on_exit(fn -> InMemory.reset() end)
    :ok
  end

  test "a transient journal failure during completion crashes the engine, not the run" do
    run_id = Ecto.UUID.generate()

    capture_log(fn ->
      {:ok, ^run_id} =
        Engine.start_run(TrivialFlow, %{}, run_id: run_id, journal: FlakyCompleteJournal)

      # The engine crashes (crash-and-resume would replay and complete); it
      # must NOT route the DB exception into fail! and bury the result.
      assert_eventually(fn ->
        Registry.lookup(Continuum.Runtime.Registry, run_id) == []
      end)
    end)

    run = InMemory.get_run(Instance.default(), run_id)
    assert run.state == :running
    assert run.result == nil
    assert run.error == nil
  end

  defp assert_eventually(fun, attempts \\ 50)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
