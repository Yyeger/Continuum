defmodule ContinuumExampleOrders.Activities.CapturePayment do
  use Continuum.Activity,
    retry: [max_attempts: 5, backoff: :exponential, base_ms: 500],
    timeout: {:seconds, 10}

  @impl true
  def run(%{"order_id" => order_id, "total_cents" => total_cents}) do
    {:ok, %{payment_id: "pay_#{order_id}", total_cents: total_cents}}
  end

  @impl true
  def idempotency_key([%{"order_id" => order_id} | _]) do
    "capture:#{order_id}"
  end
end
