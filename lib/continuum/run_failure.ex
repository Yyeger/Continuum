defmodule Continuum.RunFailure do
  @moduledoc """
  Stable public description of a failed workflow run.

  `Continuum.await/2` and `Continuum.get_run/2` return this value for workflow
  failures regardless of whether completion was observed through PubSub or by
  polling the journal. Diagnostic stacktraces are exposed separately as
  `:error_stacktrace` by `Continuum.get_run/2`.
  """

  @enforce_keys [:kind, :reason]
  defstruct [:kind, :reason]

  @type kind :: :error | :throw | :exit
  @type t :: %__MODULE__{kind: kind(), reason: term()}

  @doc false
  @spec split(term()) :: {term(), list() | nil}
  def split(%__MODULE__{} = failure), do: {failure, nil}

  def split({kind, reason, stacktrace})
      when kind in [:error, :throw, :exit] and is_list(stacktrace) do
    {%__MODULE__{kind: kind, reason: reason}, stacktrace}
  end

  def split({kind, reason}) when kind in [:error, :throw, :exit] do
    {%__MODULE__{kind: kind, reason: reason}, nil}
  end

  def split(other), do: {other, nil}
end
