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

## Structured metadata

`Continuum.log/3` adds a keyword list:

```elixir
Continuum.log(:info, "payment accepted", order_id: order.id, cents: total)
```

The metadata is journaled with the event, merged into the `Logger` metadata
alongside Continuum's own `continuum_run_id`, `continuum_workflow`, and
`continuum_replay` keys, and included in the telemetry metadata under
`:metadata`.

Because it is journaled, metadata has to survive the journal: values go through
`Continuum.DurableTerm`, so a PID, reference, port, or function is rejected at
the call rather than written and rendered meaningless on replay. Metadata is
also part of what replay matches, exactly as the message already is — changing
it for a run already in flight is drift, not a silent no-op.

A `log/2` call is unchanged on disk: the metadata key is omitted entirely when
the list is empty, so existing histories, snapshots, and golden fixtures still
match.

Levels follow Logger (`:debug` through `:emergency`), so every `Logger.<level>`
call has a direct replacement and the compile error names it. `Logger.log/3` and
`Logger.bare_log/3` map to `Continuum.log(level, message)`, or to
`Continuum.log(level, message, metadata)` when they carried metadata.
`Logger.metadata/1` and the process-level functions are rejected outright:
Logger process state is not durable, so pass context as workflow input instead.

Messages must be durable binaries and are capped at 16 KiB. Logs are workflow
breadcrumbs, so avoid putting secrets in them; use an activity when the
operation itself is an external side effect rather than observability.
