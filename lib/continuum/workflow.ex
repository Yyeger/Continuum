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
  drift can be surfaced. Full content-addressed module dispatch is deliberately
  deferred until workflow versioning becomes load-bearing.
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

  @doc "Returns a duration in milliseconds."
  defmacro seconds(n), do: quote(do: unquote(n) * 1_000)
  defmacro minutes(n), do: quote(do: unquote(n) * 60 * 1_000)
  defmacro hours(n), do: quote(do: unquote(n) * 60 * 60 * 1_000)
  defmacro days(n), do: quote(do: unquote(n) * 24 * 60 * 60 * 1_000)

  # ---------------------------------------------------------------------------

  defmacro __using__(opts) do
    version = Keyword.get(opts, :version, 1)
    retention = Keyword.get(opts, :retention, {:days, 30})

    quote do
      require Continuum

      import Continuum.Workflow,
        only: [
          activity: 1,
          activity: 2,
          await: 1,
          timer: 1,
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
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: :ok
end

defmodule Continuum.Workflow.BeforeCompile do
  @moduledoc false

  defmacro __before_compile__(env) do
    version = Module.get_attribute(env.module, :continuum_workflow_version)
    retention = Module.get_attribute(env.module, :continuum_workflow_retention)
    hash = compute_version_hash(env.module)

    quote do
      def __continuum_workflow__ do
        %{
          module: __MODULE__,
          version: unquote(version),
          retention: unquote(Macro.escape(retention)),
          version_hash: unquote(hash)
        }
      end
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
