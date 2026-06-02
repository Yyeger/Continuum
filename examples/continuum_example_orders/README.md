# Continuum Example Orders

Minimal Phoenix example app for Continuum. It starts an order checkout
workflow, waits for a fraud-review signal, then ships or compensates the
payment capture. It also includes a parent/child batch workflow that fans out
one order child per input order, plus a subscription-style `continue_as_new`
workflow with a per-workflow snapshot threshold. The smoke script covers the
v0.5 namespace and query API by starting orders in two namespaces and querying
their search attributes.

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

`ContinuumExampleOrders.SubscriptionFlow` opts into snapshots with
`snapshot_threshold: 2` to demonstrate the v0.4 per-workflow setting. Snapshots
remain opt-in; see `../../guides/snapshots.md` and
`../../bench/snapshot_bench.exs`.

The script starts one approved order in the `retail` namespace and one rejected
order in the `enterprise` namespace, then uses `Continuum.query/1` and
`Continuum.set_attributes/3` to assert that namespace-scoped searches do not
cross tenants.

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

## Subscription Demo

`ContinuumExampleOrders.SubscriptionFlow` demonstrates `continue_as_new` for a
bounded subscription loop. Each run bills one cycle, waits on a short timer, and
continues as a fresh run until `max_cycles` is reached. The smoke script starts a
two-cycle subscription and asserts the root run journals
`run_continued_as_new`.

Manual crash-resume check:

1. Start an order and leave it waiting for fraud review.
2. Stop the BEAM while the run is suspended.
3. Restart `mix phx.server`.
4. Send the fraud-review signal.
5. Inspect `continuum_events` and verify the run replays and completes.

Manual cluster smoke:

1. Form two distributed Erlang nodes that both run this app and share the
   example Postgres database.
2. Start an order on node A and leave it waiting for fraud review.
3. Send the fraud-review signal from node B.
4. Confirm the run wakes and completes, then repeat by stopping node A and
   verifying node B resumes after the lease TTL.

The same flow is available as an IEx script:

```bash
mix run scripts/smoke_test.exs
```
