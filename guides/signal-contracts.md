# Signal Contracts

Workflows can close their external signal surface by declaring accepted names
and payload validators:

```elixir
defmodule MyApp.ApprovalFlow do
  use Continuum.Workflow,
    version: 1,
    signals: [approved: :map, rejected: {MyApp.Signals, :valid_rejection?}]

  def run(_input) do
    await(signal(:approved))
  end
end
```

Built-in validators are `:any`, `:atom`, `:binary`, `:boolean`, `:integer`,
`:list`, `:map`, and `:number`. An MFA validator is written as
`{Module, :function}` and receives the payload. It may return `true` or `:ok`
to accept, and `false` or `{:error, reason}` to reject.

Once `:signals` is present, delivery rejects undeclared names and invalid
payloads before writing the signal mailbox. Literal `await(signal(:name))`
calls are checked during compilation, catching spelling mistakes before a
workflow version is deployed. Dynamic signal names remain possible but are
checked at delivery time.

Workflows that omit `:signals` retain the open signal behavior used by earlier
Continuum releases. Observer reads the exact contract from the run's journaled
workflow version and presents declared names in its signal form.
