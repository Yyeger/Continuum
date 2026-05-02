# Continuum

OTP-native durable execution engine for Elixir. Write workflow code as straight-line Elixir; failures, restarts, and node death cause the workflow to resume exactly where it left off.

> **Status: pre-alpha.** This repository is the v0.1 implementation in progress. APIs will change.

## Quickstart

```elixir
defmodule MyApp.OrderFlow do
  use Continuum.Workflow, version: 1

  def run(%{order_id: id, items: items}) do
    {:ok, validated} = activity Validation.check(items)
    {:ok, _charge}   = activity Payments.charge(id, validated.total),
                                retry: [max_attempts: 5, backoff: :exponential]

    case await signal(:fraud_review, timeout: hours(24)) do
      {:ok, :approved} -> activity Fulfillment.ship(id)
      {:ok, :rejected} -> {:error, :rejected}
      :timeout         -> activity Fulfillment.ship(id)
    end
  end
end
```

See `lib/continuum.ex` for the public facade; `Continuum.Workflow` for the DSL; `Continuum.Activity` for activities.

## License

Apache-2.0.
