input = %{
  "order_id" => "order-smoke",
  "items" => [%{"sku" => "sku_1", "qty" => 1, "price" => 1200}]
}

{:ok, run_id} = Continuum.start(ContinuumExampleOrders.OrderFlow, input)
IO.puts("started #{run_id}")

:ok = Continuum.signal(run_id, :fraud_review, :approved)
IO.inspect(Continuum.await(run_id, 10_000), label: "result")

history = Continuum.Runtime.Journal.Postgres.load(run_id)
IO.inspect(Enum.map(history, & &1.type), label: "events")
