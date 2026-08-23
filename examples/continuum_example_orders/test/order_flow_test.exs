defmodule ContinuumExampleOrders.OrderFlowTest do
  @moduledoc """
  Unit tests for the order checkout workflow.

  No Postgres, no worker pool, no web server: the workflow runs on the
  in-memory journal with its activities stubbed, so each branch is exercised
  without the payment or shipping dependencies being real.
  """

  use ExUnit.Case, async: false

  alias ContinuumExampleOrders.Activities.{
    CapturePayment,
    RefundPayment,
    ShipOrder,
    ValidateOrder
  }

  alias ContinuumExampleOrders.OrderFlow
  alias Continuum.Test

  @order %{
    "order_id" => "order-1",
    "items" => [%{"qty" => 2, "price" => 1_500}]
  }

  setup do
    Test.reset_in_memory!()
    :ok
  end

  test "an approved order captures, ships, and reports both ids" do
    {:ok, run_id} = Test.start_synchronous(OrderFlow, @order, activities: happy_path())

    :ok = Test.inject_signal(run_id, :fraud_review, :approved)

    assert {:ok, %{state: :completed, result: {:ok, result}}} = Continuum.await(run_id, 1_000)

    assert result == %{
             order_id: "order-1",
             charge: %{payment_id: "pay_test", total_cents: 3_000},
             shipment: %{shipment_id: "ship_test"}
           }
  end

  test "a rejected order refunds the exact charge it captured and fails" do
    test_pid = self()

    stubs =
      Map.put(happy_path(), {RefundPayment, :run}, fn order_id ->
        send(test_pid, {:refunded, order_id})
        {:ok, %{refund_id: "refund_#{order_id}"}}
      end)

    {:ok, run_id} = Test.start_synchronous(OrderFlow, @order, activities: stubs)

    :ok = Test.inject_signal(run_id, :fraud_review, :rejected)

    assert {:ok, %{state: :completed, result: {:error, failure}}} = Continuum.await(run_id, 1_000)
    assert failure == %{order_id: "order-1", reason: :fraud_rejected}

    assert_received {:refunded, "order-1"}
  end

  test "the workflow never ships an order it could not validate" do
    stubs = Map.put(happy_path(), {ValidateOrder, :run}, {:error, :empty_order})

    {:ok, run_id} = Test.start_synchronous(OrderFlow, @order, activities: stubs)

    # The workflow matches `{:ok, validated}`, so an invalid order fails the run
    # rather than continuing to a capture.
    assert {:error, %{state: :failed}} = Continuum.await(run_id, 1_000)

    assert Test.history(run_id)
           |> Enum.flat_map(&List.wrap(Map.get(&1, :mfa)))
           |> Enum.reject(&(&1 in [ValidateOrder, :run]))
           |> Enum.all?(&(&1 != ShipOrder))
  end

  defp happy_path do
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
