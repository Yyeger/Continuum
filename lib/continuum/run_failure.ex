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

  @max_value_bytes 65_536
  @max_text_bytes 4_096
  @max_stacktrace_entries 20

  @type kind :: :error | :throw | :exit
  @type t :: %__MODULE__{kind: kind(), reason: term()}

  @doc false
  @spec split(term()) :: {term(), list() | nil}
  def split(%__MODULE__{} = failure),
    do: {%{failure | reason: durable_value(failure.reason)}, nil}

  def split({kind, reason, stacktrace})
      when kind in [:error, :throw, :exit] and is_list(stacktrace) do
    {%__MODULE__{kind: kind, reason: durable_value(reason)},
     stacktrace |> Enum.take(@max_stacktrace_entries) |> Enum.map(&durable_value/1)}
  end

  def split({kind, reason}) when kind in [:error, :throw, :exit] do
    {%__MODULE__{kind: kind, reason: durable_value(reason)}, nil}
  end

  def split(other), do: {durable_value(other), nil}

  # Failure persistence must not fail on the value that caused the failure.
  # Keep ordinary errors intact for callers that pattern-match on them, and
  # replace node-local or oversized values with a bounded diagnostic.
  defp durable_value(value) do
    with :ok <- Continuum.DurableTerm.validate(value, :workflow_error),
         true <- byte_size(:erlang.term_to_binary(value)) <= @max_value_bytes do
      value
    else
      _ -> bounded_inspect(value)
    end
  end

  defp bounded_inspect(value) do
    text = inspect(value, limit: 20, printable_limit: @max_text_bytes, width: 80)

    if byte_size(text) <= @max_text_bytes,
      do: text,
      else: binary_part(text, 0, @max_text_bytes) <> "..."
  rescue
    _ -> "#<uninspectable workflow failure>"
  catch
    _, _ -> "#<uninspectable workflow failure>"
  end
end
