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

  alias Continuum.{Runtime.Context, Telemetry}

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
        live_tail!(ctx, effect, live_compute)

      event ->
        # Replay: validate and return.
        replay_event!(ctx, event, effect)
    end
  end

  defp compute_live({:side_effect, _kind} = _effect) do
    raise "Continuum.Runtime.Effect.run/2 invoked for side_effect without producer"
  end

  defp compute_live({:activity, {_mod, _fun, _args}, _opts}) do
    raise "Continuum.Runtime.Effect.run/2 invoked for activity without scheduler"
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

  defp live_tail!(
         %{journal: Continuum.Runtime.Journal.Postgres} = ctx,
         {:activity, {mod, fun, args}, opts},
         _live_compute
       ) do
    task_id = Ecto.UUID.generate()

    event = %{
      type: :activity_scheduled,
      task_id: task_id,
      mfa: {mod, fun, args},
      opts: opts,
      seq: ctx.cursor
    }

    task = %{
      id: task_id,
      seq: ctx.cursor,
      mfa: {mod, fun, args},
      opts: opts,
      retry: retry_policy(mod, opts),
      timeout_ms: timeout_ms(mod, opts),
      idempotency_key: idempotency_key(mod, args, opts)
    }

    :ok =
      Continuum.Runtime.Journal.Postgres.schedule_activity!(
        ctx.run_id,
        event,
        task,
        ctx.lease_token
      )

    Telemetry.execute([:continuum, :activity, :scheduled], %{}, %{
      run_id: ctx.run_id,
      task_id: task_id,
      mfa: {mod, fun, args},
      seq: ctx.cursor
    })

    throw({:continuum_suspend, {:activity_pending, task_id}})
  end

  defp live_tail!(
         %{journal: Continuum.Runtime.Journal.Postgres} = ctx,
         {:timer, ms},
         _live_compute
       ) do
    timer_id = Ecto.UUID.generate()

    fires_at =
      DateTime.utc_now()
      |> DateTime.add(ms, :millisecond)
      |> DateTime.truncate(:microsecond)

    event = %{
      type: :timer_started,
      timer_id: timer_id,
      duration_ms: ms,
      fires_at: fires_at,
      seq: ctx.cursor
    }

    timer = %{id: timer_id, fires_at: fires_at}

    :ok =
      Continuum.Runtime.Journal.Postgres.schedule_timer!(
        ctx.run_id,
        event,
        timer,
        ctx.lease_token
      )

    Telemetry.execute([:continuum, :timer, :scheduled], %{duration_ms: ms}, %{
      run_id: ctx.run_id,
      timer_id: timer_id,
      fires_at: fires_at,
      seq: ctx.cursor
    })

    throw({:continuum_suspend, {:timer_pending, timer_id}})
  end

  defp live_tail!(
         %{journal: Continuum.Runtime.Journal.Postgres} = ctx,
         {:await_signal, name, opts},
         _live_compute
       ) do
    event =
      %{
        type: :signal_awaited,
        name: name,
        opts: opts,
        seq: ctx.cursor
      }
      |> Map.merge(signal_timeout(opts))

    :ok =
      Continuum.Runtime.Journal.Postgres.schedule_signal_await!(
        ctx.run_id,
        event,
        ctx.lease_token
      )

    Telemetry.execute([:continuum, :signal, :awaited], %{}, %{
      run_id: ctx.run_id,
      signal_name: name,
      seq: ctx.cursor
    })

    case Continuum.Runtime.Journal.Postgres.resolve_signal_await(
           ctx.run_id,
           event,
           ctx.lease_token
         ) do
      {:ok, payload, winner_event} ->
        Context.put(%{ctx | history: ctx.history ++ [winner_event], cursor: ctx.cursor + 2})

        Telemetry.execute([:continuum, :signal, :received], %{}, %{
          run_id: ctx.run_id,
          signal_name: name
        })

        payload

      {:timeout, winner_event} ->
        Context.put(%{ctx | history: ctx.history ++ [winner_event], cursor: ctx.cursor + 2})
        :timeout

      :none ->
        throw({:continuum_suspend, {:awaiting_signal, name}})
    end
  end

  defp live_tail!(ctx, {:activity, {mod, fun, args}, _opts} = effect, _live_compute) do
    Telemetry.execute([:continuum, :activity, :started], %{}, %{
      run_id: ctx.run_id,
      mfa: {mod, fun, args}
    })

    result = apply(mod, fun, args)
    journal_live!(ctx, effect, result)

    Telemetry.execute([:continuum, :activity, :completed], %{}, %{
      run_id: ctx.run_id,
      mfa: {mod, fun, args}
    })

    result
  end

  defp live_tail!(ctx, effect, live_compute) do
    result = live_compute.()
    journal_live!(ctx, effect, result)
    result
  end

  defp replay_event!(ctx, event, effect) do
    case match_event(ctx, event, effect) do
      {:ok, result} ->
        Context.put(%{ctx | cursor: ctx.cursor + 1})
        result

      {:ok, result, advance_by} ->
        Context.put(%{ctx | cursor: ctx.cursor + advance_by})
        result

      :pending ->
        throw({:continuum_suspend, pending_reason(event)})

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
         _ctx,
         %{type: :side_effect, kind: ek, payload: payload},
         {:side_effect, ek}
       )
       when is_atom(ek),
       do: {:ok, payload}

  defp match_event(
         ctx,
         %{type: :activity_scheduled, mfa: {emod, efun, _ja}} = event,
         {:activity, {lmod, lfun, _la}, _opts}
       )
       when emod == lmod and efun == lfun do
    case Enum.at(ctx.history, ctx.cursor + 1) do
      %{type: :activity_completed, mfa: {^emod, ^efun, _}, payload: payload} ->
        {:ok, payload, 2}

      %{type: :activity_failed, mfa: {^emod, ^efun, _}, error: error} ->
        {:ok, {:error, error}, 2}

      nil ->
        :pending

      _other ->
        if Map.has_key?(event, :task_id), do: :pending, else: :mismatch
    end
  end

  defp match_event(
         _ctx,
         %{type: :activity_completed, mfa: {emod, efun, _ja}, payload: payload},
         {:activity, {lmod, lfun, _la}, _opts}
       )
       when emod == lmod and efun == lfun,
       do: {:ok, payload}

  defp match_event(
         _ctx,
         %{type: :signal_received, name: ename, payload: payload},
         {:await_signal, lname, _opts}
       )
       when ename == lname,
       do: {:ok, payload}

  defp match_event(
         ctx,
         %{type: :signal_awaited, name: name} = event,
         {:await_signal, name, _opts}
       ) do
    timeout_timer_id = Map.get(event, :timeout_timer_id)

    case Enum.at(ctx.history, ctx.cursor + 1) do
      %{type: :signal_received, name: ^name, payload: payload} ->
        {:ok, payload, 2}

      %{type: :timer_fired, timer_id: ^timeout_timer_id} when not is_nil(timeout_timer_id) ->
        {:ok, :timeout, 2}

      nil ->
        case Continuum.Runtime.Journal.Postgres.resolve_signal_await(
               ctx.run_id,
               event,
               ctx.lease_token
             ) do
          {:ok, payload, _winner_event} ->
            Telemetry.execute([:continuum, :signal, :received], %{}, %{
              run_id: ctx.run_id,
              signal_name: name
            })

            {:ok, payload, 2}

          {:timeout, _winner_event} ->
            {:ok, :timeout, 2}

          :none ->
            :pending
        end

      _other ->
        :mismatch
    end
  end

  defp match_event(ctx, %{type: :timer_started, timer_id: timer_id}, {:timer, _ms}) do
    case Enum.at(ctx.history, ctx.cursor + 1) do
      %{type: :timer_fired, timer_id: ^timer_id} ->
        {:ok, :ok, 2}

      nil ->
        :pending

      _other ->
        :mismatch
    end
  end

  defp match_event(_ctx, %{type: :timer_fired}, {:timer, _ms}), do: {:ok, :ok}

  defp match_event(_ctx, _, _), do: :mismatch

  defp retry_policy(mod, opts) do
    Keyword.get(opts, :retry) || activity_metadata(mod)[:retry] || [max_attempts: 1]
  end

  defp timeout_ms(mod, opts) do
    opts
    |> Keyword.get(:timeout, activity_metadata(mod)[:timeout] || {:seconds, 30})
    |> duration_to_ms()
  end

  defp signal_timeout(opts) do
    case Keyword.fetch(opts, :timeout) do
      {:ok, timeout} ->
        ms = duration_to_ms(timeout)

        fires_at =
          DateTime.utc_now()
          |> DateTime.add(ms, :millisecond)
          |> DateTime.truncate(:microsecond)

        %{
          timeout_ms: ms,
          timeout_timer_id: Ecto.UUID.generate(),
          timeout_at: fires_at
        }

      :error ->
        %{}
    end
  end

  defp idempotency_key(mod, args, opts) do
    Keyword.get_lazy(opts, :idempotency_key, fn ->
      if function_exported?(mod, :idempotency_key, 1), do: mod.idempotency_key(args), else: nil
    end)
  end

  defp activity_metadata(mod) do
    if function_exported?(mod, :__continuum_activity__, 0),
      do: mod.__continuum_activity__(),
      else: %{}
  end

  defp duration_to_ms({:seconds, n}), do: n * 1_000
  defp duration_to_ms({:minutes, n}), do: n * 60 * 1_000
  defp duration_to_ms({:hours, n}), do: n * 60 * 60 * 1_000
  defp duration_to_ms(ms) when is_integer(ms), do: ms

  defp pending_reason(%{type: :activity_scheduled} = event) do
    {:activity_pending, Map.get(event, :task_id)}
  end

  defp pending_reason(%{type: :timer_started} = event) do
    {:timer_pending, Map.get(event, :timer_id)}
  end

  defp pending_reason(%{type: :signal_awaited} = event) do
    {:awaiting_signal, Map.get(event, :name)}
  end

  defp pending_reason(event), do: {:pending, Map.get(event, :type)}

  defp raise_not_in_workflow(effect) do
    raise Continuum.NotInWorkflowError,
          "effect #{inspect(effect)} called outside a workflow process"
  end
end
