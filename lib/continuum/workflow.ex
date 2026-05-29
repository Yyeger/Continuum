defmodule Continuum.Workflow do
  @moduledoc """
  Defines a durable workflow.

      defmodule MyApp.OrderFlow do
        use Continuum.Workflow, version: 1, retention: {:days, 30}

        def run(%{order_id: id, items: items}) do
          {:ok, validated} = activity Validation.check(items)
          {:ok, _charge}   = activity Payments.charge(id, validated.total),
                                      retry: [max_attempts: 5, backoff: :exponential]

          case await signal(:fraud_review, timeout: hours(24)) do
            {:ok, :approved} -> activity Fulfillment.ship(id)
            {:ok, :rejected} -> {:error, :rejected}
            :timeout         -> activity Fulfillment.ship(id)
          end
        end
      end

  ## Determinism

  Every public and private function in the module is scanned at compile
  time by `Continuum.AstCheck`. Calls that are known to be non-deterministic
  (`DateTime.utc_now/0`, `:rand.uniform/0`, `IO.puts/1`, ETS access, …) are
  compile errors with a remediation hint.

  See `Continuum.AstCheck.forbidden_calls/0` for the denylist and the
  `:trusted_modules` config knob for extending the allowlist.

  ## Versioning

  The module's AST is hashed at compile time and exposed through
  `__continuum_workflow__/0`. Each Postgres run stores that hash on start so
  drift can be surfaced. As of v0.3, callers may pass `workflow: LogicalModule`
  to `use Continuum.Workflow` to register a concrete module as a hash-specific
  entrypoint for a logical workflow.
  """

  @doc """
  Macro: schedule an activity. The result is journaled on first execution
  and replayed on resume.

      activity Payments.charge(order_id, amount)
      activity Payments.charge(order_id, amount), retry: [max_attempts: 5]
  """
  defmacro activity(call, opts \\ [])

  defmacro activity({{:., _, [{:__aliases__, _, _} = mod_alias, fun]}, _, args}, opts) do
    command =
      command_base(
        __CALLER__,
        :activity,
        {Macro.expand(mod_alias, __CALLER__), fun, length(args || [])}
      )

    quote do
      Continuum.Runtime.Effect.run(
        {:activity, {unquote(mod_alias), unquote(fun), unquote(args)}, unquote(opts)},
        {:command, unquote(Macro.escape(command))}
      )
    end
  end

  defmacro activity({{:., _, [mod, fun]}, _, args}, opts) do
    command = command_base(__CALLER__, :activity, {mod, fun, length(args || [])})

    quote do
      Continuum.Runtime.Effect.run(
        {:activity, {unquote(mod), unquote(fun), unquote(args)}, unquote(opts)},
        {:command, unquote(Macro.escape(command))}
      )
    end
  end

  @doc """
  Macro: wait for an external signal, optionally with a timeout.

      await signal(:approved)
      await signal(:approved, timeout: hours(24))

  Or wait for a child workflow:

      await child MyApp.AuditFlow.run(%{batch_id: id})
  """
  defmacro await({:signal, _, args}) do
    {name, opts} = parse_signal_args(args)
    command = command_base(__CALLER__, :await_signal, name)

    quote do
      Continuum.Runtime.Effect.run(
        {:await_signal, unquote(name), unquote(opts)},
        {:command, unquote(Macro.escape(command))}
      )
    end
  end

  defmacro await({:child, _, [{{:., _, [mod_alias, _fun]}, _, [input]}]}) do
    start_command = command_base(__CALLER__, :start_child, :child)
    await_command = command_base(__CALLER__, :await_child, :child)

    quote do
      ref =
        Continuum.Runtime.Effect.start_child(
          unquote(mod_alias),
          unquote(input),
          [],
          {:command, unquote(Macro.escape(start_command))}
        )

      Continuum.Runtime.Effect.await_child(
        ref,
        {:command, unquote(Macro.escape(await_command))}
      )
    end
  end

  @doc """
  Macro: start a child workflow asynchronously, returning a `%Continuum.ChildRef{}`.

      ref = start_child MyApp.OrderFlow, %{order_id: id}, id: "order-\#{id}"
      # ... do other work ...
      result = await_child(ref)

  `opts` accepts `id:` to tie the child's deterministic run id to a key under
  this parent.
  """
  @doc since: "0.3.0"
  defmacro start_child(workflow, input, opts \\ []) do
    command = command_base(__CALLER__, :start_child, :child)

    quote do
      Continuum.Runtime.Effect.start_child(
        unquote(workflow),
        unquote(input),
        unquote(opts),
        {:command, unquote(Macro.escape(command))}
      )
    end
  end

  @doc """
  Macro: suspend until a previously `start_child`-ed child terminates.

  Returns the child's result (`{:ok, _}`/`{:error, _}` term), the error on child
  failure, or `{:error, :child_cancelled}` if the child was cancelled.
  """
  @doc since: "0.3.0"
  defmacro await_child(ref) do
    command = command_base(__CALLER__, :await_child, :child)

    quote do
      Continuum.Runtime.Effect.await_child(
        unquote(ref),
        {:command, unquote(Macro.escape(command))}
      )
    end
  end

  @doc false
  defmacro child(_call) do
    raise "`child` is only valid in the `await child Mod.run(args)` form"
  end

  @doc """
  Macro: durable timer.

      timer(hours(24))
  """
  defmacro timer(duration) do
    command = command_base(__CALLER__, :timer, :timer)

    quote do
      Continuum.Runtime.Effect.run(
        {:timer, unquote(duration)},
        {:command, unquote(Macro.escape(command))}
      )
    end
  end

  @doc """
  Macro: run the compensation of one successful compensated activity.

      {:ok, charge} = activity Payments.charge(id, total), compensate: {Payments, :refund, [id]}
      # ...
      compensate(charge)

  Takes the `%Continuum.ActivityRef{}` (or `{:ok, ref}`) returned by a compensated
  `activity/2` call, schedules its compensation MFA through the activity worker,
  and removes it from the pending compensation set so a later `compensate_all/0`
  cannot run it twice. Returns `{:ok, result}` or, if the compensation fails
  terminally, `{:error, reason}` — the run continues either way.
  """
  @doc since: "0.3.0"
  defmacro compensate(ref) do
    command = command_base(__CALLER__, :compensate, :compensate)

    quote do
      Continuum.Runtime.Effect.compensate(
        unquote(ref),
        {:command, unquote(Macro.escape(command))}
      )
    end
  end

  @doc """
  Macro: run all pending compensations in LIFO order (most-recent first).

      rescue
        e ->
          compensate_all()
          reraise e, __STACKTRACE__

  Each successful compensated activity that has not already been compensated by
  `compensate/1` is rolled back, newest first. Returns `:ok`.
  """
  @doc since: "0.3.0"
  defmacro compensate_all do
    command = command_base(__CALLER__, :compensate_all, :compensate_all)

    quote do
      Continuum.Runtime.Effect.compensate_all({:command, unquote(Macro.escape(command))})
    end
  end

  @doc "Returns a duration in milliseconds."
  defmacro seconds(n), do: quote(do: unquote(n) * 1_000)
  defmacro minutes(n), do: quote(do: unquote(n) * 60 * 1_000)
  defmacro hours(n), do: quote(do: unquote(n) * 60 * 60 * 1_000)
  defmacro days(n), do: quote(do: unquote(n) * 24 * 60 * 60 * 1_000)

  # ---------------------------------------------------------------------------

  defmacro __using__(opts) do
    version = Keyword.get(opts, :version, 1)
    retention = Keyword.get(opts, :retention, {:days, 30})
    logical_workflow = Keyword.get(opts, :workflow, Keyword.get(opts, :logical_workflow))

    quote do
      require Continuum

      import Continuum.Workflow,
        only: [
          activity: 1,
          activity: 2,
          await: 1,
          timer: 1,
          compensate: 1,
          compensate_all: 0,
          start_child: 2,
          start_child: 3,
          await_child: 1,
          child: 1,
          seconds: 1,
          minutes: 1,
          hours: 1,
          days: 1,
          signal: 1,
          signal: 2
        ]

      @on_definition Continuum.Workflow.OnDef
      @before_compile Continuum.Workflow.BeforeCompile

      @continuum_workflow_version unquote(version)
      @continuum_workflow_retention unquote(retention)
      @continuum_logical_workflow unquote(logical_workflow)
    end
  end

  @doc false
  defmacro signal(name) do
    quote do
      {:signal, unquote(name), []}
    end
  end

  @doc false
  defmacro signal(name, opts) do
    quote do
      {:signal, unquote(name), unquote(opts)}
    end
  end

  defp parse_signal_args([name]), do: {name, []}
  defp parse_signal_args([name, opts]), do: {name, opts}

  defp command_base(env, type, shape) do
    {type, env.module, env.function, env.line, hash_term(shape)}
  end

  defp hash_term(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end

defmodule Continuum.Workflow.OnDef do
  @moduledoc false

  @doc false
  def __on_definition__(env, _kind, name, args, _guards, body) when not is_nil(body) do
    case Continuum.AstCheck.scan(body, env.file) do
      :ok ->
        :ok

      {:error, violations} ->
        arity = length(args || [])

        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "Determinism violation in #{inspect(env.module)}.#{name}/#{arity}:\n\n" <>
              Continuum.AstCheck.format(violations)
    end

    Continuum.AstCheck.check_helper_calls(body, env, name, length(args || []))
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: :ok
end

defmodule Continuum.Workflow.BeforeCompile do
  @moduledoc false

  defmacro __before_compile__(env) do
    version = Module.get_attribute(env.module, :continuum_workflow_version)
    retention = Module.get_attribute(env.module, :continuum_workflow_retention)
    logical_workflow = Module.get_attribute(env.module, :continuum_logical_workflow) || env.module
    hash = compute_version_hash(env.module)

    quote do
      def __continuum_workflow__ do
        %{
          module: unquote(logical_workflow),
          entrypoint: __MODULE__,
          version: unquote(version),
          retention: unquote(Macro.escape(retention)),
          version_hash: unquote(hash)
        }
      end

      def __continuum_entrypoint__, do: __MODULE__
    end
  end

  defp compute_version_hash(module) do
    bodies =
      module
      |> Module.definitions_in()
      |> Enum.sort()
      |> Enum.flat_map(fn {name, arity} ->
        case Module.get_definition(module, {name, arity}) do
          {:v1, kind, _meta, clauses} ->
            Enum.map(clauses, fn {_meta, args, guards, body} ->
              {kind, name, arity, normalize(args), normalize(guards), normalize(body)}
            end)

          _ ->
            []
        end
      end)

    bodies
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  # Strip line metadata, keep structure.
  defp normalize(ast) do
    Macro.prewalk(ast, fn
      {form, meta, args} when is_list(meta) ->
        {form, [], args}

      other ->
        other
    end)
  end
end
