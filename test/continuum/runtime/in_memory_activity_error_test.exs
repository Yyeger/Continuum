defmodule Continuum.Runtime.InMemoryActivityErrorTest do
  @moduledoc """
  Audit finding 2.4: in-memory inline activities normalize exceptions into
  `{:error, error}` values exactly like the durable worker
  (`ActivityWorker.run_activity/1`), so saga branches take the same control
  path under `Continuum.Test.start_synchronous/3` as in production.
  """

  use ExUnit.Case, async: false

  defmodule RaisingActivity do
    def run(_input), do: raise("card declined")
  end

  defmodule ThrowingActivity do
    def run(_input), do: throw(:ball)
  end

  defmodule ReserveActivity do
    def reserve(x), do: {:ok, {:reserved, x}}
    def unreserve(x), do: {:unreserved, x}
  end

  defmodule HandledErrorFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      case activity(RaisingActivity.run(input)) do
        {:error, %RuntimeError{message: "card declined"}} -> {:ok, :handled}
        other -> {:ok, {:unexpected, other}}
      end
    end
  end

  defmodule ThrownErrorFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(ThrowingActivity.run(input))
    end
  end

  defmodule SagaErrorFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok, _ref} =
        activity(ReserveActivity.reserve(input.order),
          compensate: {ReserveActivity, :unreserve, [input.order]}
        )

      case activity(RaisingActivity.run(input.order), compensate: :none) do
        {:error, _error} ->
          compensate_all()
          {:error, :payment_failed}

        {:ok, _} ->
          {:ok, :paid}
      end
    end
  end

  test "a raising activity hands the workflow {:error, exception} instead of crashing the run" do
    {:ok, run_id} = Continuum.Test.start_synchronous(HandledErrorFlow, %{})

    assert {:ok, %{state: :completed, result: {:ok, :handled}}} = Continuum.await(run_id, 1_000)
  end

  test "the error value is journaled and replays identically" do
    {:ok, run_id} = Continuum.Test.start_synchronous(HandledErrorFlow, %{})
    {:ok, _} = Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)

    assert [%{type: :activity_completed, payload: {:error, %RuntimeError{}}}] = history
    assert {:ok, {:ok, :handled}} = Continuum.Test.replay(HandledErrorFlow, %{}, history)
  end

  test "throws and exits normalize like the durable worker: {:error, {kind, reason}}" do
    {:ok, run_id} = Continuum.Test.start_synchronous(ThrownErrorFlow, %{})

    assert {:ok, %{state: :completed, result: {:error, {:throw, :ball}}}} =
             Continuum.await(run_id, 1_000)
  end

  test "the canonical saga path (payment fails -> compensate_all) works inline" do
    {:ok, run_id} = Continuum.Test.start_synchronous(SagaErrorFlow, %{order: "ord-1"})

    assert {:ok, %{state: :completed, result: {:error, :payment_failed}}} =
             Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)
    types = Enum.map(history, & &1.type)

    assert types == [:activity_completed, :activity_completed, :compensation_completed]

    assert Enum.any?(history, fn event ->
             event.type == :compensation_completed and event.result == {:unreserved, "ord-1"}
           end)
  end
end
