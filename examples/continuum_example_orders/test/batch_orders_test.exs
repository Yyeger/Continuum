defmodule ContinuumExampleOrders.BatchOrdersTest do
  @moduledoc """
  The parent/child fan-out, in memory.

  Child workflows are a journal capability rather than a Postgres-only feature,
  so a batch that starts one `OrderFlow` per order is testable without a
  database — including the signal each child waits on.
  """

  use ExUnit.Case, async: false

  alias ContinuumExampleOrders.Activities.{
    CapturePayment,
    RefundPayment,
    ShipOrder,
    ValidateOrder
  }

  alias ContinuumExampleOrders.BatchOrders
  alias Continuum.Test

  setup do
    Test.reset_in_memory!()
    :ok
  end

  test "a batch fans out one child order per input and collects every result" do
    orders =
      for id <- ["a", "b"], do: %{"order_id" => id, "items" => [%{"qty" => 1, "price" => 100}]}

    {:ok, batch_id} =
      Test.start_synchronous(BatchOrders, %{"batch_id" => "batch-1", "orders" => orders},
        activities: stubs()
      )

    for child_id <- child_run_ids(batch_id, length(orders)) do
      :ok = Test.inject_signal(child_id, :fraud_review, :approved)
    end

    assert {:ok, %{state: :completed, result: {:ok, results}}} = Continuum.await(batch_id, 2_000)

    assert [{:ok, first}, {:ok, second}] = results
    assert first.order_id == "a"
    assert second.order_id == "b"
  end

  # The batch journals a `child_started` per order, in order, so the test reads
  # the child ids out of the parent's history rather than guessing them.
  defp child_run_ids(batch_id, count, attempts \\ 200)

  defp child_run_ids(batch_id, count, attempts) when attempts > 0 do
    started =
      batch_id
      |> Test.history()
      |> Enum.filter(&(&1.type == :child_started))
      |> Enum.map(& &1.child_run_id)

    if length(started) == count do
      started
    else
      Process.sleep(5)
      child_run_ids(batch_id, count, attempts - 1)
    end
  end

  defp child_run_ids(_batch_id, count, 0), do: flunk("only some of #{count} children started")

  defp stubs do
    %{
      {ValidateOrder, :run} => fn %{"items" => items} ->
        {:ok,
         %{
           total_cents: Enum.sum(Enum.map(items, &(&1["qty"] * &1["price"]))),
           item_count: length(items)
         }}
      end,
      {CapturePayment, :run} => fn %{"total_cents" => total} ->
        {:ok, %{payment_id: "pay_test", total_cents: total}}
      end,
      {ShipOrder, :run} => {:ok, %{shipment_id: "ship_test"}},
      {RefundPayment, :run} => fn order_id -> {:ok, %{refund_id: "refund_#{order_id}"}} end
    }
  end
end
