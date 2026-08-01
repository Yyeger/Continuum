# Replay-Safe Workflow Logging

Workflow execution may replay the same code many times. Calling `Logger`
directly would duplicate breadcrumbs, so workflow modules should use the
journal-identified logging effect:

```elixir
def run(input) do
  Continuum.log(:info, "starting order #{input.order_id}")
  # ...
end
```

`Continuum.log/2` appends a `workflow_log` event carrying the command identity,
level, and message. Only after that append succeeds does the live execution
emit both `Logger` output and `[:continuum, :workflow, :log]` telemetry. Replay
validates and consumes the event without emitting either output again.

Levels follow Logger (`:debug` through `:emergency`). Messages must be durable
binaries and are capped at 16 KiB. Logs are workflow breadcrumbs, so avoid
putting secrets in them; use an activity when the operation itself is an
external side effect rather than observability.
