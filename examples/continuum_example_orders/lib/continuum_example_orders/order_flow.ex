defmodule ContinuumExampleOrders.OrderFlow do
  use Continuum.Workflow, version: 1

  alias ContinuumExampleOrders.Activities.{CapturePayment, ShipOrder, ValidateOrder}

  def run(%{"order_id" => order_id, "items" => items}) do
    {:ok, validated} = activity ValidateOrder.run(%{"items" => items})

    {:ok, charge} =
      activity CapturePayment.run(%{
                 "order_id" => order_id,
                 "total_cents" => validated.total_cents
               }),
        idempotency_key: "capture:#{order_id}"

    case await signal(:fraud_review) do
      :approved ->
        {:ok, shipment} = activity ShipOrder.run(%{"order_id" => order_id})
        {:ok, %{order_id: order_id, charge: charge, shipment: shipment}}

      :rejected ->
        {:error, %{order_id: order_id, charge: charge, reason: :fraud_rejected}}
    end
  end
end
