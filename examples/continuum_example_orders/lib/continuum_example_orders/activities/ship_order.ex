defmodule ContinuumExampleOrders.Activities.ShipOrder do
  use Continuum.Activity,
    retry: [max_attempts: 3, backoff: :exponential, base_ms: 1_000],
    timeout: {:seconds, 10}

  @impl true
  def run(%{"order_id" => order_id}) do
    {:ok, %{shipment_id: "ship_#{order_id}"}}
  end
end
