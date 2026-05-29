defmodule ContinuumExampleOrders.BatchOrders do
  @moduledoc """
  Parent/child fan-out demo: a batch run starts one `OrderFlow` child per order
  and awaits them all.

  Each child run id is deterministic from this parent and the per-order `id:`,
  so re-running the batch never starts duplicate children. Cancelling the batch
  cascades to every in-flight child order.

  Each child `OrderFlow` awaits a `:fraud_review` signal, so drive the demo by
  signalling each child run (their ids are visible in the Observer under the
  batch run's child links).
  """

  use Continuum.Workflow, version: 1

  alias ContinuumExampleOrders.OrderFlow

  def run(%{"batch_id" => _batch_id, "orders" => orders}) do
    results =
      orders
      |> Enum.map(fn %{"order_id" => order_id} = order ->
        start_child(OrderFlow, order, id: order_id)
      end)
      |> Enum.map(&await_child/1)

    {:ok, results}
  end
end
