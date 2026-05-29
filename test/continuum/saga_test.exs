defmodule Continuum.SagaTest do
  @moduledoc """
  Compensation / saga DSL (V0.3 PR 4, §3.1) against the in-memory journal.
  """

  use ExUnit.Case, async: false

  alias Continuum.Runtime.{Context, Instance}
  alias Continuum.Runtime.Journal.InMemory

  defmodule SagaActivities do
    def charge(_id), do: {:ok, :charged}
    def reserve(_id), do: {:ok, :reserved}
    def refund(_id), do: :refunded
    def release(_id), do: :released
    def boom(_id), do: raise("compensation blew up")
  end

  defmodule HappyFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, _charge} =
        activity(SagaActivities.charge(input.id),
          compensate: {SagaActivities, :refund, [input.id]}
        )

      {:ok, :shipped}
    end
  end

  defmodule RejectFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, charge} =
        activity(SagaActivities.charge(input.id),
          compensate: {SagaActivities, :refund, [input.id]}
        )

      compensate(charge)
      {:error, :rejected}
    end
  end

  defmodule DoubleFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, a} =
        activity(SagaActivities.charge(input.id),
          compensate: {SagaActivities, :refund, [input.id]}
        )

      {:ok, _b} =
        activity(SagaActivities.reserve(input.id),
          compensate: {SagaActivities, :release, [input.id]}
        )

      compensate(a)
      compensate_all()
      {:error, :rolled_back}
    end
  end

  defmodule RescueFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, _a} =
        activity(SagaActivities.charge(input.id),
          compensate: {SagaActivities, :refund, [input.id]}
        )

      {:ok, _b} =
        activity(SagaActivities.reserve(input.id),
          compensate: {SagaActivities, :release, [input.id]}
        )

      raise "boom"
    rescue
      e ->
        compensate_all()
        reraise e, __STACKTRACE__
    end
  end

  defmodule FailingCompensationFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, charge} =
        activity(SagaActivities.charge(input.id), compensate: {SagaActivities, :boom, [input.id]})

      result = compensate(charge)
      {:ok, {:compensation, result}}
    end
  end

  setup do
    Continuum.Test.reset_in_memory!()
    :ok
  end

  test "happy path fires no compensation" do
    {:ok, run_id} = Continuum.Test.start_synchronous(HappyFlow, %{id: "o1"})
    assert {:ok, %{state: :completed, result: {:ok, :shipped}}} = Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)
    refute Enum.any?(history, &(&1.type in [:compensation_completed, :compensation_failed]))
    assert {:ok, {:ok, :shipped}} = Continuum.Test.replay(HappyFlow, %{id: "o1"}, history)
  end

  test "manual compensate/1 runs exactly one compensation and replays identically" do
    {:ok, run_id} = Continuum.Test.start_synchronous(RejectFlow, %{id: "o2"})

    assert {:ok, %{state: :completed, result: {:error, :rejected}}} =
             Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)
    assert Enum.count(history, &(&1.type == :compensation_completed)) == 1

    assert {:ok, {:error, :rejected}} = Continuum.Test.replay(RejectFlow, %{id: "o2"}, history)
  end

  test "compensate/1 then compensate_all/0 does not double-run a compensation" do
    {:ok, run_id} = Continuum.Test.start_synchronous(DoubleFlow, %{id: "o3"})

    assert {:ok, %{state: :completed, result: {:error, :rolled_back}}} =
             Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)
    compensations = Enum.filter(history, &(&1.type == :compensation_completed))

    # Two distinct targets, each compensated exactly once.
    assert length(compensations) == 2
    assert compensations |> Enum.map(& &1.target_activity_id) |> Enum.uniq() |> length() == 2
  end

  test "rescue + compensate_all/0 compensates in LIFO order" do
    {:ok, run_id} = Continuum.Test.start_synchronous(RescueFlow, %{id: "o4"})
    assert {:error, %{state: :failed}} = Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)

    [charge_id, reserve_id] =
      history
      |> Enum.filter(&(&1.type == :activity_completed))
      |> Enum.map(& &1.command_id)

    compensated_order =
      history
      |> Enum.filter(&(&1.type == :compensation_completed))
      |> Enum.sort_by(& &1.seq)
      |> Enum.map(& &1.target_activity_id)

    # Most-recent activity (reserve) compensated first.
    assert compensated_order == [reserve_id, charge_id]
  end

  test "a compensation that fails terminally is journaled and the run continues" do
    {:ok, run_id} = Continuum.Test.start_synchronous(FailingCompensationFlow, %{id: "o5"})

    assert {:ok, %{state: :completed, result: {:ok, {:compensation, {:error, _error}}}}} =
             Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)
    assert Enum.any?(history, &(&1.type == :compensation_failed))
    refute Enum.any?(history, &(&1.type == :compensation_completed))
  end

  test "tampering a journaled compensation event surfaces as replay drift" do
    {:ok, run_id} = Continuum.Test.start_synchronous(RejectFlow, %{id: "o6"})
    {:ok, _} = Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)

    tampered =
      Enum.map(history, fn
        %{type: :compensation_completed} = event -> %{event | command_id: {:tampered}}
        event -> event
      end)

    ctx = %Context{
      run_id: "saga-drift",
      history: tampered,
      cursor: 0,
      workflow_module: RejectFlow,
      lease_token: nil,
      instance: Instance.default(),
      journal: InMemory,
      command_counts: %{}
    }

    Context.put(ctx)

    try do
      assert_raise Continuum.ReplayDriftError, fn -> RejectFlow.run(%{id: "o6"}) end
    after
      Context.clear()
    end
  end
end
