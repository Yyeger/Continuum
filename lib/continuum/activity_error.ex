defmodule Continuum.ActivityError do
  @moduledoc """
  Durable fallback for activity failures whose original error contains
  process-local data or exceeds the journal error-size limit.
  """

  defexception [:kind, :class, :message, :reason, stacktrace: []]

  @type t :: %__MODULE__{
          kind: :error | :throw | :exit,
          class: String.t() | nil,
          message: String.t(),
          reason: String.t(),
          stacktrace: [String.t()]
        }

  @impl true
  def message(%__MODULE__{message: message}), do: message
end
