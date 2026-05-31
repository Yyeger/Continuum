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

  def run({:activity, _mfa, opts} = effect, {:command, command_base}) when is_list(opts) do
    ctx = Context.get() || raise_not_in_workflow(effect)
    {ctx, command_id} = assign_command_id(ctx, command_base)
    Context.put(ctx)

    raw =
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
              live_tail!(ctx, effect, fn -> compute_live(effect) end, command_id)

            event ->
              replay_event!(ctx, event, effect, command_id)
          end
      end

    maybe_wrap_activity(raw, effect, opts, command_id)
  end

  def run(effect, {:command, command_base}) do
    advance(effect, fn -> compute_live(effect) end, fn _ctx -> command_base end)
  end

  @doc """
  Run the compensation of one successful compensated activity.

  Schedules `ref.compensate` through the activity worker (Postgres) or runs it
  inline (in-memory), journaling `compensation_scheduled`/`compensation_completed`
  (or `compensation_failed`), then removes the activity from the pending
  compensation set. Returns `{:ok, result}` or `{:error, reason}`.
  """
  @doc since: "0.3.0"
  def compensate(ref_or_ok, {:command, command_base}) do
    ref = unwrap_ref(ref_or_ok)
    ctx = Context.get() || raise_not_in_workflow({:compensate, ref.activity_id})

    {ctx, command_id} =
      assign_command_id(ctx, :erlang.append_element(command_base, ref.activity_id))

    Context.put(ctx)

    result = do_compensation(ref.activity_id, ref.compensate, command_id)
    mark_compensated(ref.activity_id)
    result
  end

  @doc """
  Run all pending compensations in LIFO order (most-recent first).

  Each entry is scheduled as a deterministic compensation effect with a stable
  per-item command id derived from the call site, the target activity id, and
  the LIFO index. Returns `:ok`.
  """
  @doc since: "0.3.0"
  def compensate_all(command, opts \\ [])

  def compensate_all({:command, command_base}, opts) do
    ctx = Context.get() || raise_not_in_workflow(:compensate_all)

    case Keyword.get(opts, :mode, :sequential) do
      :sequential ->
        run_compensate_all(ctx.compensation_stack, command_base, 0)

      :parallel ->
        run_parallel_compensate_all(ctx.compensation_stack, command_base)

      other ->
        raise ArgumentError,
              "expected compensate_all mode to be :sequential or :parallel, got: #{inspect(other)}"
    end

    :ok
  end

  @doc """
  Start a child workflow asynchronously and return a `%Continuum.ChildRef{}`.

  The child run id is derived deterministically from the parent run id, the
  start command id, and any `id:` option, so a parent at the same cursor never
  starts two children on replay. The child inherits the parent's trace context.
  """
  @doc since: "0.3.0"
  def start_child(workflow, input, opts, {:command, command_base}) do
    ctx = Context.get() || raise_not_in_workflow(:start_child)
    {ctx, command_id} = assign_command_id(ctx, command_base)
    Context.put(ctx)
    effect = {:start_child, workflow, input, opts}

    child_run_id =
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
              live_start_child!(ctx, workflow, input, opts, command_id)

            event ->
              replay_event!(ctx, event, effect, command_id)
          end
      end

    %Continuum.ChildRef{
      child_run_id: child_run_id,
      start_command_id: command_id,
      workflow: workflow
    }
  end

  @doc """
  Complete this run as `{:continued, next_run_id}` and start a fresh run on the
  same workflow with new input. Throws the `:continuum_continued_as_new`
  sentinel; the engine acknowledges it and stops without re-entering the
  workflow.
  """
  @doc since: "0.3.0"
  def continue_as_new(input, {:command, command_base}) do
    ctx = Context.get() || raise_not_in_workflow(:continue_as_new)
    {ctx, command_id} = assign_command_id(ctx, command_base)
    Context.put(ctx)

    case snapshot_step(ctx, ctx.cursor) do
      {:ok, %{effect_type: :continue_as_new} = step} ->
        if command_matches?(step, command_id) do
          throw({:continuum_continued_as_new, Map.get(step, :result)})
        else
          raise_continue_drift(ctx, step, command_id)
        end

      {:ok, step} ->
        raise_continue_drift(ctx, step, command_id)

      :none ->
        case history_event(ctx, ctx.cursor) do
          :compacted_gap ->
            raise_continue_drift(ctx, :snapshot_step, command_id)

          nil ->
            live_continue_as_new!(ctx, input, command_id)

          %{type: :run_continued_as_new} = event ->
            if command_matches?(event, command_id) do
              throw({:continuum_continued_as_new, Map.get(event, :next_run_id)})
            else
              raise_continue_drift(ctx, event, command_id)
            end

          other ->
            raise_continue_drift(ctx, other, command_id)
        end
    end
  end

  @doc """
  Suspend until the child referenced by `ref` terminates; return its result.

  Returns the child's result on completion, `{:error, error}` on child failure,
  and `{:error, :child_cancelled}` if the child was cancelled.
  """
  @doc since: "0.3.0"
  def await_child(%Continuum.ChildRef{} = ref, {:command, command_base}) do
    ctx = Context.get() || raise_not_in_workflow(:await_child)
    {ctx, command_id} = assign_command_id(ctx, command_base)
    Context.put(ctx)
    effect = {:await_child, ref.child_run_id}

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
            live_await_child!(ctx, ref, command_id)

          event ->
            replay_event!(ctx, event, effect, command_id)
        end
    end
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

  defp compute_live({:compensation, _target_id, _mfa}) do
    raise "Continuum.Runtime.Effect: compensation must run through do_compensation/3"
  end

  defp compute_live({:await_signal, _name, _opts}) do
    throw({:continuum_suspend, :awaiting_signal})
  end

  defp compute_live({:timer, _ms}) do
    throw({:continuum_suspend, :awaiting_timer})
  end

  defp journal_live!(ctx, effect, result, command_id) do
    event = encode_event(effect, result, ctx.cursor, command_id)

    new_ctx =
      ctx
      |> Context.append_history(event)
      |> Map.put(:cursor, ctx.cursor + 1)

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
        Context.put(
          ctx
          |> Context.append_history(winner_event)
          |> Map.put(:cursor, ctx.cursor + 1)
        )

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
        Context.put(
          ctx
          |> Context.append_history(winner_event)
          |> Map.put(:cursor, ctx.cursor + 2)
        )

        Telemetry.execute([:continuum, :signal, :received], %{}, %{
          run_id: ctx.run_id,
          signal_name: name
        })

        payload

      {:timeout, winner_event} ->
        Context.put(
          ctx
          |> Context.append_history(winner_event)
          |> Map.put(:cursor, ctx.cursor + 2)
        )

        :timeout

      :none ->
        throw({:continuum_suspend, {:awaiting_signal, name}})
    end
  end

  # --- Compensation (saga DSL) ----------------------------------------------

  defp run_compensate_all([], _command_base, _index), do: :ok

  defp run_compensate_all([{target_id, mfa} | rest], command_base, index) do
    ctx = Context.get()

    item_base =
      command_base
      |> :erlang.append_element(target_id)
      |> :erlang.append_element(index)

    {ctx, command_id} = assign_command_id(ctx, item_base)
    Context.put(ctx)

    _result = do_compensation(target_id, mfa, command_id)
    mark_compensated(target_id)
    run_compensate_all(rest, command_base, index + 1)
  end

  defp run_parallel_compensate_all([], _command_base), do: :ok

  defp run_parallel_compensate_all(stack, command_base) do
    ctx = Context.get()
    {ctx, items} = parallel_compensation_items(ctx, stack, command_base)
    Context.put(ctx)

    if ctx.journal == Continuum.Runtime.Journal.Postgres do
      do_parallel_compensations(items)
    else
      Enum.each(items, fn item ->
        _result = do_compensation(item.target_id, item.mfa, item.command_id)
        mark_compensated(item.target_id)
      end)
    end
  end

  defp parallel_compensation_items(ctx, stack, command_base) do
    Enum.map_reduce(Enum.with_index(stack), ctx, fn {{target_id, mfa}, index}, acc ->
      item_base =
        command_base
        |> :erlang.append_element(target_id)
        |> :erlang.append_element(index)

      {acc, command_id} = assign_command_id(acc, item_base)
      {%{target_id: target_id, mfa: mfa, index: index, command_id: command_id}, acc}
    end)
    |> then(fn {items, ctx} -> {ctx, items} end)
  end

  defp do_parallel_compensations(items) do
    ctx = Context.get()

    case history_event(ctx, ctx.cursor) do
      :compacted_gap ->
        raise Continuum.ReplayDriftError,
          run_id: ctx.run_id,
          cursor: ctx.cursor,
          expected: :snapshot_step,
          actual: {:compensate_all, :parallel}

      nil ->
        live_parallel_compensations!(ctx, items)

      _event ->
        replay_parallel_compensations!(ctx, items)
    end
  end

  defp live_parallel_compensations!(ctx, items) do
    scheduled =
      Enum.map(items, fn item ->
        task_id = Ecto.UUID.generate()
        {mod, _fun, args} = item.mfa
        seq = ctx.cursor + item.index

        event = %{
          type: :compensation_scheduled,
          target_activity_id: item.target_id,
          mfa: item.mfa,
          attempt: 1,
          command_id: item.command_id,
          seq: seq
        }

        task = %{
          id: task_id,
          seq: seq,
          kind: :compensation,
          target_activity_id: item.target_id,
          mfa: item.mfa,
          opts: [],
          retry: retry_policy(mod, []),
          timeout_ms: timeout_ms(mod, []),
          idempotency_key: idempotency_key(mod, args, []),
          command_id: item.command_id,
          parallel_batch?: true
        }

        %{event: event, task: task}
      end)

    :ok =
      Continuum.Runtime.Journal.Postgres.schedule_compensations!(
        ctx.instance,
        ctx.run_id,
        scheduled,
        ctx.lease_token
      )

    Enum.each(scheduled, fn %{event: event, task: task} ->
      Telemetry.execute([:continuum, :compensation, :scheduled], %{}, %{
        run_id: ctx.run_id,
        target_activity_id: event.target_activity_id,
        task_id: task.id,
        mfa: event.mfa,
        seq: event.seq
      })
    end)

    throw({:continuum_suspend, {:parallel_compensation_pending, Enum.map(items, & &1.target_id)}})
  end

  defp replay_parallel_compensations!(ctx, items) do
    expected_by_command = Map.new(items, &{&1.command_id, &1})

    with {:ok, cursor, pending} <- replay_parallel_schedules(ctx, items, expected_by_command),
         {:ok, cursor} <- replay_parallel_terminals(ctx, cursor, pending) do
      Context.put(%{ctx | cursor: cursor})
      Enum.each(items, &mark_compensated(&1.target_id))
      :ok
    else
      :pending ->
        throw(
          {:continuum_suspend, {:parallel_compensation_pending, Enum.map(items, & &1.target_id)}}
        )

      {:mismatch, event} ->
        raise Continuum.ReplayDriftError,
          run_id: ctx.run_id,
          cursor: ctx.cursor,
          expected: event,
          actual: {:compensate_all, :parallel}
    end
  end

  defp replay_parallel_schedules(ctx, items, expected_by_command) do
    Enum.reduce_while(items, {:ok, ctx.cursor}, fn item, {:ok, cursor} ->
      case history_event(ctx, cursor) do
        %{type: :compensation_scheduled, target_activity_id: target_id} = event ->
          cond do
            target_id != item.target_id ->
              {:halt, {:mismatch, event}}

            not command_matches?(event, item.command_id) ->
              {:halt, {:mismatch, event}}

            not Map.has_key?(expected_by_command, Map.get(event, :command_id)) ->
              {:halt, {:mismatch, event}}

            true ->
              {:cont, {:ok, cursor + 1}}
          end

        nil ->
          {:halt, :pending}

        other ->
          {:halt, {:mismatch, other}}
      end
    end)
    |> case do
      {:ok, cursor} -> {:ok, cursor, expected_by_command}
      other -> other
    end
  end

  defp replay_parallel_terminals(_ctx, cursor, pending) when map_size(pending) == 0 do
    {:ok, cursor}
  end

  defp replay_parallel_terminals(ctx, cursor, pending) do
    case history_event(ctx, cursor) do
      %{type: type} = event when type in [:compensation_completed, :compensation_failed] ->
        command_id = Map.get(event, :command_id)

        case Map.pop(pending, command_id) do
          {nil, _pending} ->
            {:mismatch, event}

          {_item, pending} ->
            replay_parallel_terminals(ctx, cursor + 1, pending)
        end

      nil ->
        :pending

      other ->
        {:mismatch, other}
    end
  end

  defp do_compensation(target_id, mfa, command_id) do
    ctx = Context.get()
    effect = {:compensation, target_id, mfa}

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
            live_compensation!(ctx, effect, command_id)

          event ->
            replay_event!(ctx, event, effect, command_id)
        end
    end
  end

  defp live_compensation!(
         %{journal: Continuum.Runtime.Journal.Postgres} = ctx,
         {:compensation, target_id, {mod, _fun, args} = mfa},
         command_id
       ) do
    task_id = Ecto.UUID.generate()

    event = %{
      type: :compensation_scheduled,
      target_activity_id: target_id,
      mfa: mfa,
      attempt: 1,
      command_id: command_id,
      seq: ctx.cursor
    }

    task = %{
      id: task_id,
      seq: ctx.cursor,
      kind: :compensation,
      target_activity_id: target_id,
      mfa: mfa,
      opts: [],
      retry: retry_policy(mod, []),
      timeout_ms: timeout_ms(mod, []),
      idempotency_key: idempotency_key(mod, args, []),
      command_id: command_id
    }

    :ok =
      Continuum.Runtime.Journal.Postgres.schedule_compensation!(
        ctx.instance,
        ctx.run_id,
        event,
        task,
        ctx.lease_token
      )

    Telemetry.execute([:continuum, :compensation, :scheduled], %{}, %{
      run_id: ctx.run_id,
      target_activity_id: target_id,
      mfa: mfa,
      seq: ctx.cursor
    })

    throw({:continuum_suspend, {:compensation_pending, task_id}})
  end

  defp live_compensation!(ctx, {:compensation, target_id, {mod, fun, args}} = effect, command_id) do
    Telemetry.execute([:continuum, :compensation, :scheduled], %{}, %{
      run_id: ctx.run_id,
      target_activity_id: target_id,
      mfa: {mod, fun, args}
    })

    result =
      try do
        {:ok, apply(mod, fun, args)}
      rescue
        error -> {:error, error}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    journal_live!(ctx, effect, result, command_id)
    emit_compensation_result(ctx, target_id, result)
    result
  end

  defp emit_compensation_result(ctx, target_id, {:ok, _result}) do
    Telemetry.execute([:continuum, :compensation, :completed], %{}, %{
      run_id: ctx.run_id,
      target_activity_id: target_id
    })
  end

  defp emit_compensation_result(ctx, target_id, {:error, error}) do
    Telemetry.execute([:continuum, :compensation, :failed], %{}, %{
      run_id: ctx.run_id,
      target_activity_id: target_id,
      error: error
    })
  end

  # --- Child workflows -------------------------------------------------------

  defp live_start_child!(
         %{journal: Continuum.Runtime.Journal.Postgres} = ctx,
         workflow,
         input,
         opts,
         command_id
       ) do
    child_run_id = deterministic_child_run_id(ctx.run_id, command_id, Keyword.get(opts, :id))

    event = %{
      type: :child_started,
      child_run_id: child_run_id,
      workflow: workflow,
      input_hash: hash_term(input),
      command_id: command_id,
      seq: ctx.cursor
    }

    child = %{
      child_run_id: child_run_id,
      workflow: workflow,
      input: input,
      parent_command_id: :erlang.term_to_binary(command_id),
      trace_context: ctx.trace_context,
      started_event: event
    }

    :ok =
      Continuum.Runtime.Journal.Postgres.start_child!(
        ctx.instance,
        ctx.run_id,
        child,
        ctx.lease_token
      )

    Context.put(
      ctx
      |> Context.append_history(event)
      |> Map.put(:cursor, ctx.cursor + 1)
    )

    Telemetry.execute([:continuum, :child, :started], %{}, %{
      parent_run_id: ctx.run_id,
      child_run_id: child_run_id,
      workflow: workflow
    })

    child_run_id
  end

  defp live_start_child!(_ctx, _workflow, _input, _opts, _command_id) do
    raise "child workflows require the Postgres journal (start_child is durable-only)"
  end

  defp live_await_child!(%{journal: Continuum.Runtime.Journal.Postgres} = ctx, ref, command_id) do
    case Continuum.Runtime.Journal.Postgres.await_child_terminal!(
           ctx.instance,
           ctx.run_id,
           ref.child_run_id,
           command_id,
           ctx.cursor,
           ctx.lease_token
         ) do
      {:completed, result, winner_event} ->
        advance_await_child(ctx, winner_event)
        emit_child(ctx, :completed, ref.child_run_id)
        result

      {:failed, error, winner_event} ->
        advance_await_child(ctx, winner_event)
        emit_child(ctx, :failed, ref.child_run_id)
        {:error, error}

      {:cancelled, winner_event} ->
        advance_await_child(ctx, winner_event)
        emit_child(ctx, :failed, ref.child_run_id)
        {:error, :child_cancelled}

      :pending ->
        throw({:continuum_suspend, {:await_child, ref.child_run_id}})
    end
  end

  defp live_await_child!(_ctx, _ref, _command_id) do
    raise "child workflows require the Postgres journal (await_child is durable-only)"
  end

  defp advance_await_child(ctx, winner_event) do
    Context.put(
      ctx
      |> Context.append_history(winner_event)
      |> Map.put(:cursor, ctx.cursor + 1)
    )
  end

  defp emit_child(ctx, :completed, child_run_id) do
    Telemetry.execute([:continuum, :child, :completed], %{}, %{
      parent_run_id: ctx.run_id,
      child_run_id: child_run_id
    })
  end

  defp emit_child(ctx, :failed, child_run_id) do
    Telemetry.execute([:continuum, :child, :failed], %{}, %{
      parent_run_id: ctx.run_id,
      child_run_id: child_run_id
    })
  end

  # --- continue_as_new -------------------------------------------------------

  defp live_continue_as_new!(
         %{journal: Continuum.Runtime.Journal.Postgres} = ctx,
         input,
         command_id
       ) do
    next_run_id = deterministic_continue_run_id(ctx.run_id, command_id)

    event = %{
      type: :run_continued_as_new,
      next_run_id: next_run_id,
      next_input_hash: hash_term(input),
      command_id: command_id,
      seq: ctx.cursor
    }

    correlation_id =
      Continuum.Runtime.Journal.Postgres.continue_as_new!(
        ctx.instance,
        ctx.run_id,
        next_run_id,
        input,
        event,
        ctx.lease_token
      )

    Telemetry.execute([:continuum, :run, :continued_as_new], %{}, %{
      from_run_id: ctx.run_id,
      to_run_id: next_run_id,
      correlation_id: correlation_id
    })

    throw({:continuum_continued_as_new, next_run_id})
  end

  defp live_continue_as_new!(_ctx, _input, _command_id) do
    raise "continue_as_new requires the Postgres journal (it is durable-only)"
  end

  defp raise_continue_drift(ctx, expected, _command_id) do
    raise Continuum.ReplayDriftError,
      run_id: ctx.run_id,
      cursor: ctx.cursor,
      expected: expected,
      actual: :continue_as_new
  end

  defp deterministic_continue_run_id(run_id, command_id) do
    <<u0::48, _::4, u1::12, _::2, u2::62, _rest::binary>> =
      :crypto.hash(:sha256, :erlang.term_to_binary({:continue_as_new, run_id, command_id}))

    Ecto.UUID.load!(<<u0::48, 5::4, u1::12, 2::2, u2::62>>)
  end

  defp deterministic_child_run_id(parent_run_id, command_id, id_opt) do
    <<u0::48, _::4, u1::12, _::2, u2::62, _rest::binary>> =
      :crypto.hash(:sha256, :erlang.term_to_binary({parent_run_id, command_id, id_opt}))

    Ecto.UUID.load!(<<u0::48, 5::4, u1::12, 2::2, u2::62>>)
  end

  defp maybe_wrap_activity(raw, {:activity, mfa, _opts}, opts, command_id) do
    case Keyword.get(opts, :compensate) do
      value when value in [nil, :none] -> raw
      compensate_mfa -> wrap_compensated(raw, mfa, compensate_mfa, command_id)
    end
  end

  defp wrap_compensated({:ok, value}, mfa, compensate_mfa, activity_id) do
    ctx = Context.get()

    Context.put(%{
      ctx
      | compensation_stack: [{activity_id, compensate_mfa} | ctx.compensation_stack]
    })

    {:ok,
     %Continuum.ActivityRef{
       activity_id: activity_id,
       result: value,
       raw_result: {:ok, value},
       mfa: mfa,
       compensate: compensate_mfa
     }}
  end

  defp wrap_compensated({:error, _reason} = error, _mfa, _compensate_mfa, _activity_id), do: error

  defp wrap_compensated(other, _mfa, _compensate_mfa, _activity_id) do
    raise ArgumentError, """
    an activity scheduled with `compensate:` must return {:ok, value} or \
    {:error, reason}, got:

      #{inspect(other)}

    Wrap the activity's return in {:ok, value}, or drop the `compensate:` option \
    if the activity does not need a compensation handle.
    """
  end

  defp unwrap_ref(%Continuum.ActivityRef{} = ref), do: ref
  defp unwrap_ref({:ok, %Continuum.ActivityRef{} = ref}), do: ref

  defp unwrap_ref(other) do
    raise ArgumentError,
          "compensate/1 expects the %Continuum.ActivityRef{} returned by a compensated " <>
            "activity (or {:ok, ref}), got: #{inspect(other)}"
  end

  defp mark_compensated(activity_id) do
    ctx = Context.get()
    stack = Enum.reject(ctx.compensation_stack, fn {id, _mfa} -> id == activity_id end)
    Context.put(%{ctx | compensation_stack: stack})
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

    Context.put(
      ctx
      |> Context.append_history(event)
      |> Map.put(:cursor, ctx.cursor + 1)
    )

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
    cond do
      command_matches?(step, command_id) and Map.get(step, :shape) == patch_name ->
        Context.put(%{ctx | cursor: ctx.cursor + Map.fetch!(step, :advance_by)})
        value = Map.get(step, :result)
        emit_patched(ctx, patch_name, value)
        value

      command_matches?(step, command_id) ->
        raise Continuum.ReplayDriftError,
          run_id: ctx.run_id,
          cursor: ctx.cursor,
          expected: step,
          actual: {:patched, patch_name}

      true ->
        # A compacted `patched` marker for a different command site sits here;
        # this call did not exist when the history was recorded.
        patched_miss(ctx, patch_name)
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

  defp encode_event({:compensation, target_id, _mfa}, {:ok, value}, seq, command_id) do
    %{
      type: :compensation_completed,
      target_activity_id: target_id,
      result: value,
      command_id: command_id,
      seq: seq
    }
  end

  defp encode_event({:compensation, target_id, _mfa}, {:error, error}, seq, command_id) do
    %{
      type: :compensation_failed,
      target_activity_id: target_id,
      error: error,
      attempt: 1,
      command_id: command_id,
      seq: seq
    }
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

  defp match_event(
         ctx,
         %{type: :compensation_scheduled, target_activity_id: etid},
         {:compensation, ltid, _mfa},
         command_id
       )
       when etid == ltid do
    case history_event(ctx, ctx.cursor + 1) do
      %{type: :compensation_completed, target_activity_id: ^etid, result: result} = event ->
        if command_matches?(event, command_id), do: {:ok, {:ok, result}, 2}, else: :mismatch

      %{type: :compensation_failed, target_activity_id: ^etid, error: error} = event ->
        if command_matches?(event, command_id), do: {:ok, {:error, error}, 2}, else: :mismatch

      nil ->
        :pending

      _other ->
        :mismatch
    end
  end

  defp match_event(
         _ctx,
         %{type: :compensation_completed, target_activity_id: etid, result: result},
         {:compensation, ltid, _mfa},
         _command_id
       )
       when etid == ltid,
       do: {:ok, {:ok, result}}

  defp match_event(
         _ctx,
         %{type: :compensation_failed, target_activity_id: etid, error: error},
         {:compensation, ltid, _mfa},
         _command_id
       )
       when etid == ltid,
       do: {:ok, {:error, error}}

  defp match_event(
         _ctx,
         %{type: :child_started, child_run_id: child_run_id},
         {:start_child, _workflow, _input, _opts},
         _command_id
       ),
       do: {:ok, child_run_id}

  defp match_event(
         _ctx,
         %{type: :child_completed, child_run_id: ecid, result: result},
         {:await_child, lcid},
         _command_id
       )
       when ecid == lcid,
       do: {:ok, result}

  defp match_event(
         _ctx,
         %{type: :child_failed, child_run_id: ecid, error: error},
         {:await_child, lcid},
         _command_id
       )
       when ecid == lcid,
       do: {:ok, {:error, error}}

  defp match_event(
         _ctx,
         %{type: :child_cancelled, child_run_id: ecid},
         {:await_child, lcid},
         _command_id
       )
       when ecid == lcid,
       do: {:ok, {:error, :child_cancelled}}

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
  defp effect_shape({:compensation, target_id, _mfa}), do: {:compensation, target_id}
  defp effect_shape({:start_child, workflow, _input, _opts}), do: {:start_child, workflow}
  defp effect_shape({:await_child, child_run_id}), do: {:await_child, child_run_id}

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
    Context.history_event(ctx, cursor)
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

  defp pending_reason(%{type: :compensation_scheduled} = event) do
    {:compensation_pending, Map.get(event, :target_activity_id)}
  end

  defp pending_reason(event), do: {:pending, Map.get(event, :type)}

  defp raise_not_in_workflow(effect) do
    raise Continuum.NotInWorkflowError,
          "effect #{inspect(effect)} called outside a workflow process"
  end
end
