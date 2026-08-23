defmodule Continuum.EventType do
  @moduledoc false

  alias Continuum.DurableTermError

  @types [
    :activity_batch_scheduled,
    :activity_completed,
    :activity_failed,
    :activity_retry_scheduled,
    :activity_scheduled,
    :child_cancelled,
    :child_completed,
    :child_failed,
    :child_started,
    :compensation_completed,
    :compensation_failed,
    :compensation_scheduled,
    :patched,
    :run_continued_as_new,
    :side_effect,
    :signal_awaited,
    :signal_received,
    :timer_fired,
    :timer_started,
    :workflow_log
  ]

  @by_name Map.new(@types, &{Atom.to_string(&1), &1})
  @type_set MapSet.new(@types)

  @doc false
  @spec from_string!(binary()) :: atom()
  def from_string!(event_type) when is_binary(event_type) do
    case Map.fetch(@by_name, event_type) do
      {:ok, type} ->
        type

      :error ->
        raise DurableTermError,
          path: [:event_type],
          kind: :unknown_atom,
          value: event_type
    end
  end

  @doc false
  @spec to_string!(atom()) :: binary()
  def to_string!(event_type) when is_atom(event_type) do
    if MapSet.member?(@type_set, event_type) do
      Atom.to_string(event_type)
    else
      raise ArgumentError, "unknown Continuum journal event type: #{inspect(event_type)}"
    end
  end
end
