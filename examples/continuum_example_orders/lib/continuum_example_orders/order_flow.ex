defmodule ContinuumExampleOrders.OrderFlow do
  use Continuum.Workflow, version: 1

  alias ContinuumExampleOrders.Activities.{CapturePayment, RefundPayment, ShipOrder, ValidateOrder}

  def run(%{"order_id" => order_id, "items" => items}) do
    {:ok, validated} = activity ValidateOrder.run(%{"items" => items})

    # The payment capture carries a compensation: if the order is later rejected
    # (or a downstream step fails) we refund the exact charge we captured.
    {:ok, charge} =
      activity CapturePayment.run(%{
                 "order_id" => order_id,
                 "total_cents" => validated.total_cents
               }),
        idempotency_key: "capture:#{order_id}",
        compensate: {RefundPayment, :run, [order_id]}

    case await signal(:fraud_review) do
      :approved ->
        {:ok, shipment} = activity ShipOrder.run(%{"order_id" => order_id})
        {:ok, %{order_id: order_id, charge: charge.result, shipment: shipment}}

      :rejected ->
        # Roll the charge back, then fail the run.
        compensate(charge)
        {:error, %{order_id: order_id, reason: :fraud_rejected}}
    end
  end
end
