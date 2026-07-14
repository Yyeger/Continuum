defmodule Continuum.DurableTermError do
  @moduledoc "Raised when a value contains a node-local term that cannot be replayed durably."

  defexception [:path, :kind]

  @impl true
  def message(%__MODULE__{path: path, kind: kind}) do
    "non-durable #{kind} at #{Continuum.DurableTerm.format_path(path)}; " <>
      "PIDs, references, ports, and functions cannot be journaled"
  end
end
