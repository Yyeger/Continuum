defmodule Continuum.DurableTermError do
  @moduledoc "Raised when a value contains a node-local term that cannot be replayed durably."

  defexception [:path, :kind, :value]

  @type t :: %__MODULE__{path: [term()] | nil, kind: atom(), value: term()}

  @impl true
  def message(%__MODULE__{path: nil, kind: :undecodable}) do
    "could not decode a journaled term: the stored bytes are corrupt, or they " <>
      "encode an atom this node has never defined. Continuum decodes with " <>
      "`:safe`, which never creates atoms; a dynamically built atom " <>
      "(`String.to_atom/1` on external input) is not durable across nodes"
  end

  def message(%__MODULE__{path: path, kind: :unknown_atom, value: value}) do
    "unknown journal atom #{inspect(value)} at #{Continuum.DurableTerm.format_path(path)}; " <>
      "Continuum only restores atoms already present in deployed code"
  end

  def message(%__MODULE__{path: path, kind: kind}) do
    "non-durable #{kind} at #{Continuum.DurableTerm.format_path(path)}; " <>
      "PIDs, references, ports, and functions cannot be journaled"
  end
end
