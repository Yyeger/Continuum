# Observability

Continuum emits stable `:telemetry` events under the `[:continuum, ...]`
prefix. See `Continuum.Telemetry.events/0` for the complete list.

## OpenTelemetry

Continuum does not depend on OpenTelemetry directly. To export spans, add and
configure the OpenTelemetry packages in your application, then attach the
bridge after your exporter is configured:

```elixir
{:ok, _handler_id} = Continuum.OpenTelemetry.setup()
```

The bridge creates short spans instead of multi-day spans:

* `continuum.run_attempt` starts on `[:continuum, :run, :started]` and ends
  when the attempt suspends, completes, fails, is cancelled, or loses its
  lease.
* `continuum.activity_attempt` starts on
  `[:continuum, :activity, :started]` and ends when the attempt completes,
  fails, or is retried.

Every span includes `continuum.run_id`. If the run has a persisted W3C
`traceparent` in `continuum_runs.trace_context`, each run-attempt span also
links back to that original trace context using OpenTelemetry span links. This
is how resumed attempts remain correlated without keeping one long span open
for the full lifetime of the workflow.

In Postgres mode, activities execute in `ActivityWorker.Worker` processes after
the engine has suspended. Most activity-attempt spans therefore show up as
sibling roots, not children of the run-attempt span. Use `continuum.run_id` to
correlate them in your backend. Inline in-memory activity spans can be children
of the run-attempt span because they happen in the same process.

Timer, signal, activity scheduling, and idempotency-hit events are added as
span events on the active run-attempt span when they happen in the same process.
Some runtime events happen in separate processes after the run has suspended;
those still carry `continuum.run_id` as attributes for backend correlation.

The current CI suite tests the bridge with a fake tracer so Continuum can keep
OpenTelemetry optional. Before tagging v0.2, run a smoke test in an application
that includes `:opentelemetry`, `:opentelemetry_api`, and your exporter.
