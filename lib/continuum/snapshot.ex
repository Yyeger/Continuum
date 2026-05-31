defmodule Continuum.Snapshot do
  @moduledoc """
  Compacted history prefix for long-running workflows.

  A snapshot does not capture a BEAM continuation. Workflow code still runs
  from the top; the snapshot only replaces an old prefix of raw journal events
  with compacted steps that validate the requested effect and return the
  previously journaled result.
  """

  @format_version 1
  @envelope_tag :continuum_snapshot

  defstruct [
    :run_id,
    :through_seq,
    :version_hash,
    :taken_at,
    steps_by_seq: %{}
  ]

  @type step :: %{
          effect_type: atom(),
          command_id: term(),
          shape: term(),
          result: term(),
          advance_by: pos_integer()
        }

  @type t :: %__MODULE__{
          run_id: binary(),
          through_seq: non_neg_integer(),
          version_hash: binary(),
          steps_by_seq: %{non_neg_integer() => step()},
          taken_at: DateTime.t()
        }

  @doc "Encode a snapshot for storage."
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = snapshot) do
    :erlang.term_to_binary({@envelope_tag, @format_version, snapshot})
  end

  @doc "Decode a stored snapshot payload."
  @spec decode(binary()) :: t()
  def decode(binary) when is_binary(binary) do
    case :erlang.binary_to_term(binary) do
      {@envelope_tag, @format_version, %__MODULE__{} = snapshot} ->
        snapshot

      {@envelope_tag, version, _payload} ->
        raise ArgumentError,
              "snapshot format version #{inspect(version)} is not supported by this release"

      %__MODULE__{} = snapshot ->
        snapshot

      other ->
        raise ArgumentError, "invalid Continuum snapshot payload: #{inspect(other)}"
    end
  end

  @doc "Current snapshot payload format version."
  @spec format_version() :: pos_integer()
  def format_version, do: @format_version

  @doc "Return the encoded size in bytes."
  @spec encoded_size(t()) :: non_neg_integer()
  def encoded_size(%__MODULE__{} = snapshot), do: byte_size(encode(snapshot))

  @doc """
  Compact a contiguous event list into snapshot steps.

  Pass `:base` to extend an existing compatible snapshot. If the event list
  ends with a pending scheduled/awaited event, the compacted snapshot stops at
  the last complete step instead of covering the incomplete tail.
  """
  @spec compact(binary(), binary(), [map()], keyword()) ::
          {:ok, t()} | {:skip, term()} | {:error, term()}
  def compact(run_id, version_hash, events, opts \\ []) do
    base = Keyword.get(opts, :base)
    steps = if base, do: base.steps_by_seq || %{}, else: %{}

    events
    |> Enum.sort_by(&event_seq/1)
    |> compact_events(steps, nil)
    |> case do
      {:ok, _steps, nil} ->
        {:skip, :no_complete_steps}

      {:ok, steps, through_seq} ->
        {:ok,
         %__MODULE__{
           run_id: run_id,
           through_seq: through_seq,
           version_hash: version_hash,
           steps_by_seq: steps,
           taken_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp compact_events([], steps, through_seq), do: {:ok, steps, through_seq}

  defp compact_events([event | rest], steps, through_seq) do
    case step_from(event, rest) do
      {:ok, step, advance_by} ->
        consumed = Enum.take(rest, advance_by - 1)
        last_event = List.last(consumed) || event

        rest
        |> Enum.drop(advance_by - 1)
        |> compact_events(Map.put(steps, event.seq, step), last_event.seq)

      {:incomplete, _reason} ->
        {:ok, steps, through_seq}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp step_from(%{seq: nil} = event, _rest), do: {:error, {:missing_seq, event.type}}

  defp step_from(%{type: :side_effect, kind: kind, payload: payload} = event, _rest) do
    one_step(event, :side_effect, kind, payload)
  end

  defp step_from(%{type: :activity_completed, mfa: mfa, payload: payload} = event, _rest) do
    one_step(event, :activity, activity_shape(mfa), payload)
  end

  defp step_from(%{type: :activity_failed, mfa: mfa, error: error} = event, _rest) do
    one_step(event, :activity, activity_shape(mfa), {:error, error})
  end

  defp step_from(%{type: :signal_received, name: name, payload: payload} = event, _rest) do
    one_step(event, :await_signal, name, payload)
  end

  defp step_from(%{type: :timer_fired} = event, _rest) do
    one_step(event, :timer, :timer, :ok)
  end

  defp step_from(%{type: :patched, patch_name: patch_name, value: value} = event, _rest) do
    one_step(event, :patched, patch_name, value)
  end

  defp step_from(
         %{type: :compensation_completed, target_activity_id: tid, result: result} = event,
         _rest
       ) do
    one_step(event, :compensation, tid, {:ok, result})
  end

  defp step_from(
         %{type: :compensation_failed, target_activity_id: tid, error: error} = event,
         _rest
       ) do
    one_step(event, :compensation, tid, {:error, error})
  end

  defp step_from(
         %{type: :child_started, workflow: workflow, child_run_id: child_run_id} = event,
         _rest
       ) do
    one_step(event, :start_child, workflow, child_run_id)
  end

  defp step_from(%{type: :run_continued_as_new, next_run_id: next_run_id} = event, _rest) do
    one_step(event, :continue_as_new, :continue_as_new, next_run_id)
  end

  defp step_from(
         %{type: :child_completed, child_run_id: child_run_id, result: result} = event,
         _rest
       ) do
    one_step(event, :await_child, child_run_id, result)
  end

  defp step_from(%{type: :child_failed, child_run_id: child_run_id, error: error} = event, _rest) do
    one_step(event, :await_child, child_run_id, {:error, error})
  end

  defp step_from(%{type: :child_cancelled, child_run_id: child_run_id} = event, _rest) do
    one_step(event, :await_child, child_run_id, {:error, :child_cancelled})
  end

  defp step_from(%{type: :compensation_scheduled, target_activity_id: tid} = event, rest) do
    with {:ok, next} <- next_event(event, rest),
         :ok <- not_parallel_compensation_batch?(next),
         :ok <- same_command?(event, next) do
      case next.type do
        :compensation_completed ->
          paired_step(event, next, :compensation, tid, {:ok, next.result})

        :compensation_failed ->
          paired_step(event, next, :compensation, tid, {:error, next.error})

        other ->
          {:error, {:compensation_winner_mismatch, event.seq, other}}
      end
    end
  end

  defp step_from(%{type: :activity_scheduled} = event, rest) do
    with {:ok, next} <- next_event(event, rest),
         :ok <- same_command?(event, next),
         :ok <- same_activity?(event, next) do
      case next.type do
        :activity_completed ->
          paired_step(event, next, :activity, activity_shape(event.mfa), next.payload)

        :activity_failed ->
          paired_step(event, next, :activity, activity_shape(event.mfa), {:error, next.error})

        other ->
          {:error, {:activity_winner_mismatch, event.seq, other}}
      end
    end
  end

  defp step_from(%{type: :signal_awaited} = event, rest) do
    with {:ok, next} <- next_event(event, rest),
         :ok <- same_command?(event, next) do
      cond do
        next.type == :signal_received and next.name == event.name ->
          paired_step(event, next, :await_signal, event.name, next.payload)

        next.type == :timer_fired and
            Map.get(event, :timeout_timer_id) == Map.get(next, :timer_id) ->
          paired_step(event, next, :await_signal, event.name, :timeout)

        true ->
          {:error, {:signal_winner_mismatch, event.seq, next.type}}
      end
    end
  end

  defp step_from(%{type: :timer_started} = event, rest) do
    with {:ok, next} <- next_event(event, rest),
         :ok <- same_command?(event, next) do
      if next.type == :timer_fired and Map.get(next, :timer_id) == Map.get(event, :timer_id) do
        paired_step(event, next, :timer, :timer, :ok)
      else
        {:error, {:timer_winner_mismatch, event.seq, next.type}}
      end
    end
  end

  defp step_from(%{type: type, seq: seq}, _rest), do: {:error, {:unsupported_event, type, seq}}

  defp not_parallel_compensation_batch?(%{type: :compensation_scheduled}),
    do: {:incomplete, :parallel_compensation_batch}

  defp not_parallel_compensation_batch?(_next), do: :ok

  defp next_event(event, []) do
    {:incomplete, {event.type, event.seq}}
  end

  defp next_event(event, [next | _rest]) do
    if next.seq == event.seq + 1 do
      {:ok, next}
    else
      {:error, {:non_contiguous_pair, event.seq, next.seq}}
    end
  end

  defp paired_step(event, _next, effect_type, shape, result) do
    with {:ok, command_id} <- command_id(event) do
      {:ok,
       %{
         effect_type: effect_type,
         command_id: command_id,
         shape: shape,
         result: result,
         advance_by: 2
       }, 2}
    end
  end

  defp one_step(event, effect_type, shape, result) do
    with {:ok, command_id} <- command_id(event) do
      {:ok,
       %{
         effect_type: effect_type,
         command_id: command_id,
         shape: shape,
         result: result,
         advance_by: 1
       }, 1}
    end
  end

  defp same_activity?(%{mfa: {mod, fun, _}}, %{mfa: {mod, fun, _}}), do: :ok
  defp same_activity?(%{seq: seq}, _next), do: {:error, {:activity_mfa_mismatch, seq}}

  defp same_command?(event, next) do
    case {Map.get(event, :command_id), Map.get(next, :command_id)} do
      {nil, _} -> {:error, {:missing_command_id, event.seq}}
      {_, nil} -> {:error, {:missing_command_id, next.seq}}
      {command_id, command_id} -> :ok
      _ -> {:error, {:command_id_mismatch, event.seq}}
    end
  end

  defp command_id(%{command_id: nil, seq: seq}), do: {:error, {:missing_command_id, seq}}
  defp command_id(%{command_id: command_id}), do: {:ok, command_id}
  defp command_id(%{seq: seq}), do: {:error, {:missing_command_id, seq}}

  defp activity_shape({mod, fun, args}), do: {mod, fun, length(args || [])}

  defp event_seq(%{seq: nil}), do: -1
  defp event_seq(%{seq: seq}), do: seq
end
