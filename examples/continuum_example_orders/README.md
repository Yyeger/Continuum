# Continuum Example Orders

Minimal Phoenix example app for Continuum. It starts an order checkout
workflow, waits for a fraud-review signal, then ships or compensates the
payment capture. It also includes a parent/child batch workflow that fans out
one order child per input order.

The application supervises a named Continuum instance after
`ContinuumExampleOrders.Repo`, matching the required startup order for
Postgres-backed runtime pollers.

## Setup

```bash
cd examples/continuum_example_orders
docker compose up -d
mix deps.get
mix ecto.create
mix ecto.migrate
mix phx.server
```

The compose file starts Postgres and Jaeger. Continuum spans are exported to
Jaeger's OTLP HTTP endpoint at `http://localhost:4318`; open the Jaeger UI at
`http://localhost:16686`.

This example includes `opentelemetry`, `opentelemetry_api`, and
`opentelemetry_exporter` as runtime dependencies so traces are exported by
default. Applications that copy this layout are opting into those runtime deps.

## Observer

The Continuum Observer is mounted at:

```text
http://localhost:4000/admin/continuum
```

The example uses hardcoded basic auth credentials:

```text
username: admin
password: admin
```

The Observer stylesheet is served directly from Continuum's
`priv/static/observer.css` with a dedicated `Plug.Static` entry in the example
endpoint. Host apps may alternatively copy the file into their own
`priv/static` directory.

Snapshots are intentionally not demonstrated in this app. They remain
experimental and opt-in; see `../../guides/snapshots.md` and
`../../bench/snapshot_bench.exs`.

## Smoke Test

Start an order:

```bash
curl -s -X POST http://localhost:4000/orders \
  -H 'content-type: application/json' \
  -d '{"order_id":"o1","items":[{"sku":"sku_1","qty":1,"price":1200}]}'
```

Approve it using the returned `run_id`:

```bash
curl -s -X POST http://localhost:4000/runs/$RUN_ID/fraud-review \
  -H 'content-type: application/json' \
  -d '{"decision":"approved"}'
```

Rejecting the order runs the payment compensation exactly once:

```bash
curl -s -X POST http://localhost:4000/runs/$RUN_ID/fraud-review \
  -H 'content-type: application/json' \
  -d '{"decision":"rejected"}'
```

The `continuum_events` table will contain `compensation_scheduled` and
`compensation_completed` for the refund.

## Batch Demo

`ContinuumExampleOrders.BatchOrders` demonstrates parent/child fan-out. Start a
batch run from IEx or your own controller:

```elixir
Continuum.start(
  ContinuumExampleOrders.BatchOrders,
  %{
    "batch_id" => "b1",
    "orders" => [
      %{"order_id" => "o1", "items" => [%{"sku" => "sku_1", "qty" => 1, "price" => 1200}]},
      %{"order_id" => "o2", "items" => [%{"sku" => "sku_2", "qty" => 2, "price" => 900}]},
      %{"order_id" => "o3", "items" => [%{"sku" => "sku_3", "qty" => 1, "price" => 2200}]}
    ]
  },
  instance: :continuum_example_orders
)
```

Use the Observer to find the child run ids, then send each child its
`:fraud_review` signal. Cancelling the batch cascades to any in-flight child
orders.

Manual crash-resume check:

1. Start an order and leave it waiting for fraud review.
2. Stop the BEAM while the run is suspended.
3. Restart `mix phx.server`.
4. Send the fraud-review signal.
5. Inspect `continuum_events` and verify the run replays and completes.

The same flow is available as an IEx script:

```bash
mix run scripts/smoke_test.exs
```
