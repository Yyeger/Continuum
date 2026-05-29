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

    * `run(effect, {:command, command_base})` — for workflow DSL effects.
      The command base is expanded from the AST call site and becomes part
      of the journaled command identity.

    * `run(effect, line)` — compatibility form for journaled effects
      (`:activity`, `:await_signal`, `:timer`). Live execution suspends the
      workflow process via `throw {:continuum_suspend, _}`.
  """
  @spec run(
          effect(),
          (-> term()) | pos_integer() | {:command, term()} | {:command, term(), (-> term())}
        ) ::
          term()
  def run({:side_effect, _kind} = effect, producer) when is_function(producer, 0) do
    advance(effect, producer, fn ctx -> side_effect_command_base(ctx, effect, producer) end)
  end

  def run({:side_effect, _kind} = effect, {:command, command_base, producer})
      when is_function(producer, 0) do
    advance(effect, producer, fn _ctx -> command_base end)
  end

  def run(effect, line) when is_integer(line) do
    advance(effect, fn -> compute_live(effect) end, fn ctx ->
      line_command_base(ctx, effect, line)
    end)
  end

  def run(effect, {:command, command_base}) do
    advance(effect, fn -> compute_live(effect) end, fn _ctx -> command_base end)
  end

  # ---------------------------------------------------------------------------

  # `{:patched, name}` is the only effect that may return *without advancing the
  # cursor*: it does so when replaying pre-patch history, so a run recorded
  # before the patch line existed stays on its original branch. The non-advance
  # is conditioned on `command_id` lookahead, not just type lookahead, so two
  # `patched?/1` calls at distinct command_ids each independently take this
  # branch on the same old history without consuming a downstream event of
  # another shape. A fresh (live-tail) call journals `value: true` and advances.
  defp advance({:patched, patch_name} = effect, _live_compute, command_base_fun) do
    ctx = Context.get() || raise_not_in_workflow(effect)
    {ctx, command_id} = assign_command_id(ctx, command_base_fun.(ctx))
    Context.put(ctx)

    case snapshot_step(ctx, ctx.cursor) do
      {:ok, step} ->
        replay_patched_step(ctx, step, patch_name, command_id)

      :none ->
        case history_event(ctx, ctx.cursor) do
          nil ->
            journal_patched!(ctx, patch_name, command_id)

          %{type: :patched} = event ->
            replay_patched_event!(ctx, event, patch_name, command_id)

          :compacted_gap ->
            patched_miss(ctx, patch_name)

          _other ->
            patched_miss(ctx, patch_name)
        end
    end
  end

  defp advance(effect, live_compute, command_base_fun) do
    ctx = Context.get() || raise_not_in_workflow(effect)
    {ctx, command_id} = assign_command_id(ctx, command_base_fun.(ctx))
    Context.put(ctx)

    case snapshot_step(ctx, ctx.cursor) do
      {:ok, step} ->
        replay_snapshot_step!(ctx, step, effect, command_id)

      :none ->
        case history_event(ctx, ctx.cursor) do
          :compacted_gap ->
            raise Continuum.ReplayDriftError,
              run_id: ctx.run_id,
              cursor: ctx.cursor,
              expected: :snapshot_step,
              actual: effect

          nil ->
            live_tail!(ctx, effect, live_compute, command_id)

          event ->
            # Replay: validate and return.
            replay_event!(ctx, event, effect, command_id)
        end
    end
  end

  defp compute_live({:side_effect, _kind} = _effect) do
    raise "Continuum.Runtime.Effect.run/2 invoked for side_effect without producer"
  end

  defp compute_live({:activity, {_mod, _fun, _args}, _opts}) do
    raise "Continuum.Runtime.Effect.run/2 invoked for activity without scheduler"
  end

  defp compute_live({:patched, _name}), do: true

  defp compute_live({:await_signal, _name, _opts}) do
    throw({:continuum_suspend, :awaiting_signal})
  end

  defp compute_live({:timer, _ms}) do
    throw({:continuum_suspend, :awaiting_timer})
  end

  defp journal_live!(ctx, effect, result, command_id) do
    event = encode_event(effect, result, ctx.cursor, command_id)
    new_history = ctx.history ++ [event]
    new_ctx = %{ctx | history: new_history, cursor: ctx.cursor + 1}
    Context.put(new_ctx)
    apply(ctx.journal, :append!, [ctx.instance, ctx.run_id, event, ctx.lease_token])
  end

  defp live_tail!(
         %{journal: Continuum.Runtime.Journal.Postgres} = ctx,
         {:activity, {mod, fun, args}, opts},
         _live_compute,
         command_id
       ) do
    task_id = Ecto.UUID.generate()

    event = %{
      type: :activity_scheduled,
      task_id: task_id,
      mfa: {mod, fun, args},
      opts: opts,
      command_id: command_id,
      seq: ctx.cursor
    }

    task = %{
      id: task_id,
      seq: ctx.cursor,
      mfa: {mod, fun, args},
      opts: opts,
      retry: retry_policy(mod, opts),
      timeout_ms: timeout_ms(mod, opts),
      idempotency_key: idempotency_key(mod, args, opts),
      command_id: command_id
    }

    :ok =
      Continuum.Runtime.Journal.Postgres.schedule_activity!(
        ctx.instance,
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
         _live_compute,
         command_id
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
      command_id: command_id,
      seq: ctx.cursor
    }

    timer = %{id: timer_id, fires_at: fires_at}

    :ok =
      Continuum.Runtime.Journal.Postgres.schedule_timer!(
        ctx.instance,
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

  defp live_tail!(ctx, {:timer, ms}, _live_compute, command_id) do
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
      command_id: command_id,
      seq: ctx.cursor
    }

    :ok = apply(ctx.journal, :append!, [ctx.instance, ctx.run_id, event, ctx.lease_token])

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
         _live_compute,
         command_id
       ) do
    case Continuum.Runtime.Journal.Postgres.consume_pending_signal!(
           ctx.instance,
           ctx.run_id,
           name,
           command_id,
           ctx.cursor,
           ctx.lease_token
         ) do
      {:ok, payload, winner_event} ->
        Context.put(%{ctx | history: ctx.history ++ [winner_event], cursor: ctx.cursor + 1})

        Telemetry.execute([:continuum, :signal, :received], %{}, %{
          run_id: ctx.run_id,
          signal_name: name
        })

        payload

      :none ->
        schedule_signal_await!(ctx, name, opts, command_id)
    end
  end

  defp live_tail!(
         ctx,
         {:activity, {mod, fun, args}, _opts} = effect,
         _live_compute,
         command_id
       ) do
    Telemetry.execute([:continuum, :activity, :started], %{}, %{
      run_id: ctx.run_id,
      mfa: {mod, fun, args}
    })

    result = apply(mod, fun, args)
    journal_live!(ctx, effect, result, command_id)

    Telemetry.execute([:continuum, :activity, :completed], %{}, %{
      run_id: ctx.run_id,
      mfa: {mod, fun, args}
    })

    result
  end

  defp live_tail!(ctx, effect, live_compute, command_id) do
    result = live_compute.()
    journal_live!(ctx, effect, result, command_id)
    result
  end

  defp schedule_signal_await!(ctx, name, opts, command_id) do
    event =
      %{
        type: :signal_awaited,
        name: name,
        opts: opts,
        command_id: command_id,
        seq: ctx.cursor
      }
      |> Map.merge(signal_timeout(opts))

    :ok =
      Continuum.Runtime.Journal.Postgres.schedule_signal_await!(
        ctx.instance,
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
           ctx.instance,
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

  defp journal_patched!(ctx, patch_name, command_id) do
    event = %{
      type: :patched,
      patch_name: patch_name,
      value: true,
      command_id: command_id,
      seq: ctx.cursor
    }

    :ok = apply(ctx.journal, :append!, [ctx.instance, ctx.run_id, event, ctx.lease_token])
    Context.put(%{ctx | history: ctx.history ++ [event], cursor: ctx.cursor + 1})
    emit_patched(ctx, patch_name, true)
    true
  end

  defp replay_patched_event!(ctx, %{type: :patched} = event, patch_name, command_id) do
    cond do
      command_matches?(event, command_id) and Map.get(event, :patch_name) == patch_name ->
        value = Map.get(event, :value)
        Context.put(%{ctx | cursor: ctx.cursor + 1})
        emit_patched(ctx, patch_name, value)
        value

      command_matches?(event, command_id) ->
        # Same command site, but a different patch name was journaled here.
        raise Continuum.ReplayDriftError,
          run_id: ctx.run_id,
          cursor: ctx.cursor,
          expected: event,
          actual: {:patched, patch_name}

      true ->
        # A `patched` marker for a *different* command site sits here; this
        # call did not exist when the history was recorded.
        patched_miss(ctx, patch_name)
    end
  end

  defp replay_patched_step(ctx, %{effect_type: :patched} = step, patch_name, command_id) do
    if command_matches?(step, command_id) and Map.get(step, :shape) == patch_name do
      Context.put(%{ctx | cursor: ctx.cursor + Map.fetch!(step, :advance_by)})
      value = Map.get(step, :result)
      emit_patched(ctx, patch_name, value)
      value
    else
      raise Continuum.ReplayDriftError,
        run_id: ctx.run_id,
        cursor: ctx.cursor,
        expected: step,
        actual: {:patched, patch_name}
    end
  end

  defp replay_patched_step(ctx, _step, patch_name, _command_id) do
    # A non-patched compacted step occupies this cursor → pre-patch history.
    patched_miss(ctx, patch_name)
  end

  defp patched_miss(ctx, patch_name) do
    emit_patched(ctx, patch_name, false)
    false
  end

  defp emit_patched(ctx, patch_name, value) do
    Telemetry.execute([:continuum, :patched, :hit], %{}, %{
      run_id: ctx.run_id,
      patch_name: patch_name,
      value: value
    })
  end

  defp replay_event!(ctx, event, effect, command_id) do
    result =
      if command_matches?(event, command_id),
        do: match_event(ctx, event, effect, command_id),
        else: :mismatch

    case result do
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

  defp replay_snapshot_step!(ctx, step, effect, command_id) do
    if command_matches?(step, command_id) and snapshot_step_matches?(step, effect) do
      Context.put(%{ctx | cursor: ctx.cursor + Map.fetch!(step, :advance_by)})
      Map.get(step, :result)
    else
      raise Continuum.ReplayDriftError,
        run_id: ctx.run_id,
        cursor: ctx.cursor,
        expected: step,
        actual: effect
    end
  end

  defp encode_event({:side_effect, kind}, result, seq, command_id) do
    %{type: :side_effect, kind: kind, payload: result, command_id: command_id, seq: seq}
  end

  defp encode_event({:activity, {mod, fun, args}, _opts}, result, seq, command_id) do
    %{
      type: :activity_completed,
      mfa: {mod, fun, args},
      payload: result,
      command_id: command_id,
      seq: seq
    }
  end

  defp encode_event({:await_signal, name, _opts}, payload, seq, command_id) do
    %{type: :signal_received, name: name, payload: payload, command_id: command_id, seq: seq}
  end

  defp encode_event({:timer, ms}, _result, seq, command_id) do
    %{type: :timer_fired, duration_ms: ms, command_id: command_id, seq: seq}
  end

  defp match_event(
         _ctx,
         %{type: :side_effect, kind: ek, payload: payload},
         {:side_effect, ek},
         _command_id
       )
       when is_atom(ek),
       do: {:ok, payload}

  defp match_event(
         ctx,
         %{type: :activity_scheduled, mfa: {emod, efun, _ja}},
         {:activity, {lmod, lfun, _la}, _opts},
         command_id
       )
       when emod == lmod and efun == lfun do
    case history_event(ctx, ctx.cursor + 1) do
      %{type: :activity_completed, mfa: {^emod, ^efun, _}, payload: payload} = event ->
        if command_matches?(event, command_id), do: {:ok, payload, 2}, else: :mismatch

      %{type: :activity_failed, mfa: {^emod, ^efun, _}, error: error} = event ->
        if command_matches?(event, command_id), do: {:ok, {:error, error}, 2}, else: :mismatch

      nil ->
        :pending

      _other ->
        :mismatch
    end
  end

  defp match_event(
         _ctx,
         %{type: :activity_completed, mfa: {emod, efun, _ja}, payload: payload},
         {:activity, {lmod, lfun, _la}, _opts},
         _command_id
       )
       when emod == lmod and efun == lfun,
       do: {:ok, payload}

  defp match_event(
         _ctx,
         %{type: :signal_received, name: ename, payload: payload},
         {:await_signal, lname, _opts},
         _command_id
       )
       when ename == lname,
       do: {:ok, payload}

  defp match_event(
         ctx,
         %{type: :signal_awaited, name: name} = event,
         {:await_signal, name, _opts},
         command_id
       ) do
    timeout_timer_id = Map.get(event, :timeout_timer_id)

    case history_event(ctx, ctx.cursor + 1) do
      %{type: :signal_received, name: ^name, payload: payload} = event ->
        if command_matches?(event, command_id), do: {:ok, payload, 2}, else: :mismatch

      %{type: :timer_fired, timer_id: ^timeout_timer_id} = event
      when not is_nil(timeout_timer_id) ->
        if command_matches?(event, command_id), do: {:ok, :timeout, 2}, else: :mismatch

      nil ->
        case Continuum.Runtime.Journal.Postgres.resolve_signal_await(
               ctx.instance,
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

  defp match_event(ctx, %{type: :timer_started, timer_id: timer_id}, {:timer, _ms}, command_id) do
    case history_event(ctx, ctx.cursor + 1) do
      %{type: :timer_fired, timer_id: ^timer_id} = event ->
        if command_matches?(event, command_id), do: {:ok, :ok, 2}, else: :mismatch

      nil ->
        :pending

      _other ->
        :mismatch
    end
  end

  defp match_event(_ctx, %{type: :timer_fired}, {:timer, _ms}, _command_id), do: {:ok, :ok}

  defp match_event(_ctx, _, _, _), do: :mismatch

  defp assign_command_id(ctx, base) do
    counts = ctx.command_counts || %{}
    ordinal = Map.get(counts, base, 0)
    command_id = Tuple.insert_at(base, tuple_size(base), ordinal)
    {%{ctx | command_counts: Map.put(counts, base, ordinal + 1)}, command_id}
  end

  defp line_command_base(ctx, effect, line) do
    {type, shape} = effect_shape(effect)
    {type, ctx.workflow_module, nil, line, hash_term(shape)}
  end

  defp side_effect_command_base(ctx, {:side_effect, kind}, producer) do
    {:side_effect, kind, producer_fingerprint(producer), hash_term(ctx.workflow_module)}
  end

  defp producer_fingerprint(fun) do
    info = :erlang.fun_info(fun)

    {
      Keyword.fetch!(info, :module),
      Keyword.fetch!(info, :name),
      Keyword.fetch!(info, :arity),
      Keyword.get(info, :new_index, Keyword.get(info, :index)),
      Keyword.get(info, :new_uniq, Keyword.get(info, :uniq))
    }
  end

  defp command_matches?(event, command_id) do
    case Map.get(event, :command_id) || Map.get(event, "command_id") do
      nil -> true
      ^command_id -> true
      _other -> false
    end
  end

  defp effect_shape({:side_effect, kind}), do: {:side_effect, kind}

  defp effect_shape({:activity, {mod, fun, args}, _opts}) do
    {:activity, {mod, fun, length(args || [])}}
  end

  defp effect_shape({:await_signal, name, _opts}), do: {:await_signal, name}
  defp effect_shape({:timer, _ms}), do: {:timer, :timer}

  defp snapshot_step(ctx, cursor) do
    case Map.get(ctx.snapshot_steps || %{}, cursor) do
      nil -> :none
      step -> {:ok, step}
    end
  end

  defp snapshot_step_matches?(step, effect) do
    {effect_type, shape} = effect_shape(effect)
    Map.get(step, :effect_type) == effect_type and Map.get(step, :shape) == shape
  end

  defp history_event(ctx, cursor) do
    offset = ctx.history_offset || 0

    cond do
      cursor < offset ->
        :compacted_gap

      true ->
        Enum.at(ctx.history, cursor - offset)
    end
  end

  defp hash_term(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

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
