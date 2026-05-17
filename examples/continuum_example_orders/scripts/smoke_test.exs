input = %{
  "order_id" => "order-smoke",
  "items" => [%{"sku" => "sku_1", "qty" => 1, "price" => 1200}]
}

workflow_opts = [
  instance: :continuum_example_orders,
  journal: Continuum.Runtime.Journal.Postgres
]

{:ok, run_id} = Continuum.start(ContinuumExampleOrders.OrderFlow, input, workflow_opts)
IO.puts("started #{run_id}")

:ok = Continuum.signal(run_id, :fraud_review, :approved, instance: :continuum_example_orders)
IO.inspect(Continuum.await(run_id, 10_000, workflow_opts), label: "result")

instance = Continuum.Runtime.Instance.lookup(:continuum_example_orders)
history = Continuum.Runtime.Journal.Postgres.load(instance, run_id)
IO.inspect(Enum.map(history, & &1.type), label: "events")
