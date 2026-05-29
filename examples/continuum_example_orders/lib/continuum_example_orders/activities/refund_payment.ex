defmodule ContinuumExampleOrders.Activities.RefundPayment do
  @moduledoc """
  Compensation for `CapturePayment` — refunds a captured charge.

  Marked idempotent so a crash-resumed compensation reuses the prior committed
  refund instead of issuing a second one.
  """

  use Continuum.Activity,
    retry: [max_attempts: 5, backoff: :exponential, base_ms: 500],
    timeout: {:seconds, 10}

  @impl true
  def run(order_id) do
    {:ok, %{refund_id: "refund_#{order_id}"}}
  end

  @impl true
  def idempotency_key([order_id | _]) do
    "refund:#{order_id}"
  end
end
