# Replay-Safe Workflow Logging

Workflow execution may replay the same code many times. Calling `Logger`
directly would duplicate breadcrumbs, so `Continuum.AstCheck` rejects `Logger`
calls in workflow and `Continuum.Pure` code. Use the journal-identified logging
effect instead:

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

Levels follow Logger (`:debug` through `:emergency`), so every `Logger.<level>`
call has a direct replacement and the compile error names it. `Logger.log/3` and
`Logger.bare_log/3` map to `Continuum.log(level, message)`.
`Logger.metadata/1` and the process-level functions are rejected outright:
Logger process state is not durable, so pass context as workflow input instead.

Messages must be durable binaries and are capped at 16 KiB. Logs are workflow
breadcrumbs, so avoid putting secrets in them; use an activity when the
operation itself is an external side effect rather than observability.
