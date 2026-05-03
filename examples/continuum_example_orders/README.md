# Continuum Example Orders

Minimal Phoenix example app for the Continuum v0.1 smoke test. It starts an
order checkout workflow, waits for a fraud-review signal, then ships or rejects
the order.

The application supervises `Continuum.children()` after
`ContinuumExampleOrders.Repo`, matching the required startup order for
Postgres-backed runtime pollers.

## Setup

```bash
cd examples/continuum_example_orders
mix deps.get
mix ecto.create
mix continuum.gen.migration --repo ContinuumExampleOrders.Repo
mix ecto.migrate
mix phx.server
```

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
