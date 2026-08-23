defmodule Continuum.ReadOnlyJournalError do
  @moduledoc """
  Raised when read-only replay reaches a journal write.

  `Continuum.Replay` and `mix continuum.replay` are structurally read-only: the
  context they build is given its history up front and carries
  `Continuum.Runtime.Journal.ReadOnly`, which implements every journal callback
  as a raise. Reaching one means a replay path tried to mutate durable state,
  so the raise is a bug report rather than something a caller should rescue.
  """

  defexception [:operation, :run_id]

  @type t :: %__MODULE__{operation: atom(), run_id: binary() | nil}

  @impl true
  def message(%__MODULE__{operation: operation, run_id: run_id}) do
    "read-only replay attempted #{operation}" <>
      run_suffix(run_id) <>
      "; replay never writes to the journal. Please report this as a Continuum bug"
  end

  defp run_suffix(nil), do: ""
  defp run_suffix(run_id), do: " on run #{run_id}"
end
