defmodule Continuum.Replay.PostgresTest do
  @moduledoc """
  End-to-end replay tests against the Postgres journal adapter.

  These mirror the tests in `Continuum.ReplayTest` but route through
  `Continuum.Runtime.Journal.Postgres` instead of `InMemory`.

  These tests use SQL Sandbox shared mode so the workflow GenServer can
  use the test process' checked-out connection.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres

  describe "happy-path execution with Postgres journal" do
    defmodule TwoStepPgFlow do
      use Continuum.Workflow, version: 1

      def run(input) do
        n = Continuum.side_effect(fn -> input.seed * 2 end)
        m = Continuum.side_effect(fn -> n + 1 end)
        {:ok, m}
      end
    end

    test "executes synchronously via side_effect chain" do
      {:ok, run_id} =
        Continuum.Runtime.Engine.start_run(TwoStepPgFlow, %{seed: 5}, journal: Postgres)

      assert {:ok, %{state: :completed, result: {:ok, 11}}} =
               Continuum.await(run_id, 1_000, journal: Postgres)
    end

    test "events are persisted to Postgres" do
      {:ok, run_id} =
        Continuum.Runtime.Engine.start_run(TwoStepPgFlow, %{seed: 5}, journal: Postgres)

      {:ok, _} = Continuum.await(run_id, 1_000, journal: Postgres)

      events = Postgres.load(run_id)
      assert length(events) == 2
      assert Enum.all?(events, &(&1.type == :side_effect))
    end

    test "replaying the same history produces the same result" do
      {:ok, run_id} =
        Continuum.Runtime.Engine.start_run(TwoStepPgFlow, %{seed: 5}, journal: Postgres)

      {:ok, _} = Continuum.await(run_id, 1_000, journal: Postgres)

      events = Postgres.load(run_id)

      ctx = %Continuum.Runtime.Context{
        run_id: "pg-replay-test",
        history: events,
        cursor: 0,
        workflow_module: TwoStepPgFlow,
        lease_token: nil,
        journal: Postgres
      }

      Continuum.Runtime.Context.put(ctx)

      try do
        result = TwoStepPgFlow.run(%{seed: 5})
        assert result == {:ok, 11}
      after
        Continuum.Runtime.Context.clear()
      end
    end
  end

  describe "signal-driven branching with Postgres journal" do
    defmodule SignalBranchPgFlow do
      use Continuum.Workflow, version: 1

      def run(_input) do
        case await(signal(:decision)) do
          :go -> {:ok, :went}
          :stop -> {:error, :stopped}
        end
      end
    end

    test "an :approved signal drives the workflow to :went" do
      {:ok, run_id} =
        Continuum.Runtime.Engine.start_run(SignalBranchPgFlow, %{}, journal: Postgres)

      :ok = Continuum.signal(run_id, :decision, :go)

      assert {:ok, %{state: :completed, result: {:ok, :went}}} =
               Continuum.await(run_id, 1_000, journal: Postgres)
    end

    test "a :rejected signal drives the workflow to :stopped" do
      {:ok, run_id} =
        Continuum.Runtime.Engine.start_run(SignalBranchPgFlow, %{}, journal: Postgres)

      :ok = Continuum.signal(run_id, :decision, :stop)

      assert {:ok, %{state: :completed, result: {:error, :stopped}}} =
               Continuum.await(run_id, 1_000, journal: Postgres)
    end
  end

  describe "replay drift detection with Postgres journal" do
    defmodule DriftPgFlow do
      use Continuum.Workflow, version: 1

      def run(_input) do
        Continuum.side_effect(fn -> :first end)
      end
    end

    test "raises ReplayDriftError when journaled type doesn't match" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, DriftPgFlow, %{})

      event = %{type: :signal_received, name: :foo, payload: :bar, seq: 0}
      :ok = Postgres.append!(run_id, event, nil)

      events = Postgres.load(run_id)

      ctx = %Continuum.Runtime.Context{
        run_id: "pg-drift",
        history: events,
        cursor: 0,
        workflow_module: DriftPgFlow,
        lease_token: nil,
        journal: Postgres
      }

      Continuum.Runtime.Context.put(ctx)

      try do
        assert_raise Continuum.ReplayDriftError, fn ->
          DriftPgFlow.run(%{})
        end
      after
        Continuum.Runtime.Context.clear()
      end
    end
  end

  defp generate_uuid do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)
    <<a::32, b::16, c::16, d::16, e::48>> = <<u0::48, 4::4, u1::12, 2::2, u2::62>>

    :io_lib.format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [a, b, c, d, e])
    |> IO.iodata_to_binary()
  end
end
