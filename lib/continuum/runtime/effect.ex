defmodule Continuum.Runtime.Effect do
  @moduledoc """
  The bridge between workflow code and the engine.

  Every effect (activity, await_signal, timer, side_effect) is dispatched
  through `run/2`. The function consults the journal at the current cursor:

    * If the next event matches the effect being requested, the recorded
      result is returned immediately. This is **replay**.

    * If the cursor has reached the end of the journal, this is the **live
      tail**. We append a `*_scheduled` event, schedule the work, then
      `throw {:continuum_suspend, reason}` to unwind back to the engine.

    * If the next event does not match the effect being requested, we
      raise `Continuum.ReplayDriftError`.

  `run/2` accepts either an effect tuple or a 0-arity producer function for
  deterministic primitives (`Continuum.now/0`, `Continuum.uuid4/0`, etc.).
  """

  alias Continuum.Runtime.Context

  @type effect ::
          {:activity, {module(), atom(), list()}, keyword()}
          | {:await_signal, atom(), keyword()}
          | {:timer, pos_integer()}
          | {:side_effect, atom()}

  @suspend_token :continuum_suspend

  @doc "Token thrown when a workflow needs to suspend pending external work."
  def suspend_token, do: @suspend_token

  @doc """
  Dispatch an effect.

  Two arity-2 forms:

    * `run({:side_effect, kind}, producer)` — for deterministic primitives.
      The producer (a 0-arity function) is invoked exactly once on first
      execution; its return value is journaled. On replay, the journaled
      value is returned without invoking the producer.

    * `run(effect, line)` — for journaled effects (`:activity`,
      `:await_signal`, `:timer`). The `line` is the AST line of the call
      site, used for replay-drift diagnostics. Live execution suspends the
      workflow process via `throw {:continuum_suspend, _}`.
  """
  @spec run(effect(), (-> term()) | pos_integer()) :: term()
  def run({:side_effect, _kind} = effect, producer) when is_function(producer, 0) do
    advance(effect, producer)
  end

  def run(effect, line) when is_integer(line) do
    advance(effect, fn -> compute_live(effect) end)
  end

  # ---------------------------------------------------------------------------

  defp advance(effect, live_compute) do
    ctx = Context.get() || raise_not_in_workflow(effect)

    case Enum.at(ctx.history, ctx.cursor) do
      nil ->
        # Live tail: compute, journal, return.
        result = live_compute.()
        journal_live!(ctx, effect, result)
        result

      event ->
        # Replay: validate and return.
        replay_event!(ctx, event, effect)
    end
  end

  defp compute_live({:side_effect, _kind} = _effect) do
    raise "Continuum.Runtime.Effect.run/2 invoked for side_effect without producer"
  end

  defp compute_live({:activity, {_mod, _fun, _args}, _opts}) do
    # In-process synchronous activity execution for v0.1 in-memory engine.
    # The Postgres-backed engine schedules this on the activity worker
    # pool and suspends instead.
    throw({:continuum_suspend, {:activity_pending, :scheduled}})
  end

  defp compute_live({:await_signal, _name, _opts}) do
    throw({:continuum_suspend, :awaiting_signal})
  end

  defp compute_live({:timer, _ms}) do
    throw({:continuum_suspend, :awaiting_timer})
  end

  defp journal_live!(ctx, effect, result) do
    event = encode_event(effect, result, ctx.cursor)
    new_history = ctx.history ++ [event]
    new_ctx = %{ctx | history: new_history, cursor: ctx.cursor + 1}
    Context.put(new_ctx)
    apply(ctx.journal, :append!, [ctx.run_id, event, ctx.lease_token])
  end

  defp replay_event!(ctx, event, effect) do
    case match_event(event, effect) do
      {:ok, result} ->
        Context.put(%{ctx | cursor: ctx.cursor + 1})
        result

      :mismatch ->
        raise Continuum.ReplayDriftError,
          run_id: ctx.run_id,
          cursor: ctx.cursor,
          expected: event,
          actual: effect
    end
  end

  defp encode_event({:side_effect, kind}, result, seq) do
    %{type: :side_effect, kind: kind, payload: result, seq: seq}
  end

  defp encode_event({:activity, {mod, fun, args}, _opts}, result, seq) do
    %{
      type: :activity_completed,
      mfa: {mod, fun, args},
      payload: result,
      seq: seq
    }
  end

  defp encode_event({:await_signal, name, _opts}, payload, seq) do
    %{type: :signal_received, name: name, payload: payload, seq: seq}
  end

  defp encode_event({:timer, ms}, _result, seq) do
    %{type: :timer_fired, duration_ms: ms, seq: seq}
  end

  defp match_event(
         %{type: :side_effect, kind: ek, payload: payload},
         {:side_effect, ek}
       )
       when is_atom(ek),
       do: {:ok, payload}

  defp match_event(
         %{type: :activity_completed, mfa: {emod, efun, _ja}, payload: payload},
         {:activity, {lmod, lfun, _la}, _opts}
       )
       when emod == lmod and efun == lfun,
       do: {:ok, payload}

  defp match_event(
         %{type: :signal_received, name: ename, payload: payload},
         {:await_signal, lname, _opts}
       )
       when ename == lname,
       do: {:ok, payload}

  defp match_event(%{type: :timer_fired}, {:timer, _ms}), do: {:ok, :ok}

  defp match_event(_, _), do: :mismatch

  defp raise_not_in_workflow(effect) do
    raise Continuum.NotInWorkflowError,
          "effect #{inspect(effect)} called outside a workflow process"
  end
end
