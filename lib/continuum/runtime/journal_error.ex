defmodule Continuum.Runtime.JournalError do
  @moduledoc """
  Raised by the Postgres journal adapter when a transactional write is
  rejected or fails.

  `reason` carries the structured rollback reason (for example
  `{:lease_mismatch, ...}` or `{:run_not_active, "completed"}`), so the
  engine and activity workers classify failures by pattern matching instead
  of message substrings. `op` names the journal operation that failed.
  """

  defexception [:op, :reason]

  @impl true
  def message(%{op: op, reason: reason}) do
    "Continuum.Runtime.Journal.Postgres #{op} failed: #{inspect(reason)}"
  end

  @doc """
  Whether this error means the caller's run-lease authority is gone
  (fencing token rotated, or the CAS write matched no row).
  """
  def lease_lost?(%__MODULE__{reason: reason}), do: lease_reason?(reason)

  defp lease_reason?({:lease_mismatch, _detail}), do: true
  defp lease_reason?({:cas_failed, _run_id}), do: true
  defp lease_reason?({_op, :lease_mismatch}), do: true
  defp lease_reason?(_reason), do: false
end
