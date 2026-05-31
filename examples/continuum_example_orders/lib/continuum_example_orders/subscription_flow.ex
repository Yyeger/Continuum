defmodule ContinuumExampleOrders.SubscriptionFlow do
  @moduledoc """
  `continue_as_new` demo for a bounded subscription billing loop.

  Each physical run bills one cycle. Intermediate cycles sleep briefly and then
  continue as a fresh run so no single run accumulates an unbounded history.
  """

  use Continuum.Workflow, version: 1, snapshot_threshold: 2

  alias ContinuumExampleOrders.Activities.CapturePayment

  def run(%{
        "subscription_id" => subscription_id,
        "cycles_done" => cycles_done,
        "max_cycles" => max_cycles,
        "amount_cents" => amount_cents
      }) do
    cycle = cycles_done + 1

    {:ok, charge} =
      activity(
        CapturePayment.run(%{
          "order_id" => "#{subscription_id}:cycle:#{cycle}",
          "total_cents" => amount_cents
        }),
        idempotency_key: "subscription:#{subscription_id}:cycle:#{cycle}"
      )

    if cycle >= max_cycles do
      {:ok,
       %{
         subscription_id: subscription_id,
         cycles_done: cycle,
         last_charge: charge
       }}
    else
      timer(seconds(1))

      continue_as_new(%{
        "subscription_id" => subscription_id,
        "cycles_done" => cycle,
        "max_cycles" => max_cycles,
        "amount_cents" => amount_cents
      })
    end
  end
end
