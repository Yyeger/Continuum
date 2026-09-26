defmodule Continuum.Pure do
  @moduledoc """
  Mark a module as a pure helper that may be called from workflow code.

  The `Continuum.AstCheck` scanner runs over every function in the module
  as it is defined; non-deterministic calls become compile errors.

      defmodule MyApp.PriceMath do
        use Continuum.Pure

        def total(items), do: Enum.reduce(items, 0, & &1.price + &2)
      end

  Trusted stdlib modules (`Enum`, `Map`, `String`, …) do not need this; see
  `Continuum.AstCheck.trusted_stdlib/0` for the baked-in allowlist.

  Static Pure calls in generated workflow entrypoints are pinned to a
  content-addressed helper implementation, including transitive Pure calls.
  Keep these generated helper BEAMs with historical workflow versions. Define
  same-file helpers before their callers, or place them in separate files.
  """

  defmacro __using__(_opts) do
    quote do
      @on_definition Continuum.Pure
      @before_compile Continuum.Pure

      def __continuum_pure__, do: true
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    definitions = Continuum.CodeIdentity.pin_helpers!(env.module, env)
    hash = Continuum.CodeIdentity.hash(definitions)
    entrypoint = Module.concat(env.module, :"V_#{hash}")

    clauses =
      Enum.flat_map(definitions, fn
        {{name, _arity}, {:v1, kind, _meta, clauses}} when kind in [:def, :defp] ->
          Enum.map(clauses, fn {meta, args, guards, body} ->
            body = pin_self_calls(body, env.module, entrypoint)
            head = {name, meta, args}

            head =
              if guards == [],
                do: head,
                else: {:when, meta, [head | List.flatten(guards)]}

            {kind, meta, [head, [do: body]]}
          end)

        _ ->
          []
      end)

    quote do
      defmodule unquote(entrypoint) do
        @moduledoc false
        unquote_splicing(clauses)

        def __continuum_pure_version__,
          do: %{entrypoint: __MODULE__, version_hash: unquote(hash)}
      end

      @doc false
      def __continuum_pure_version__,
        do: %{entrypoint: unquote(entrypoint), version_hash: unquote(hash)}
    end
  end

  defp pin_self_calls(ast, module, entrypoint) do
    Macro.prewalk(ast, fn
      {{:., dot_meta, [^module, fun]}, meta, args} when is_list(args) ->
        {{:., dot_meta, [entrypoint, fun]}, meta, args}

      node ->
        node
    end)
  end

  @doc false
  def __on_definition__(env, _kind, name, args, guards, body) when not is_nil(body) do
    definition_ast = with_guards(guards, body)

    case Continuum.AstCheck.scan(definition_ast, env) do
      :ok ->
        :ok

      {:error, violations} ->
        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "Continuum.Pure module contains non-deterministic calls:\n\n" <>
              Continuum.AstCheck.format(violations)
    end

    # Pure helpers run inside the workflow process: a `catch` arm around an
    # effect call swallows the engine's suspend throw exactly like one in the
    # workflow module would (the runtime SuspendLeakError stays the backstop,
    # but warn at compile time too).
    Continuum.AstCheck.check_catch_warnings(definition_ast, env, name, length(args || []))

    # A Pure module is wholly trusted from workflow code, so trust must be
    # transitive: calls into unmarked modules and dynamic receivers get the
    # same diagnostics as workflow clauses — otherwise `use Continuum.Pure`
    # launders unscanned calls past the untrusted_call_severity policy.
    Continuum.AstCheck.check_helper_calls(definition_ast, env, name, length(args || []))

    Continuum.AstCheck.check_dynamic_call_warnings(
      definition_ast,
      env,
      name,
      length(args || [])
    )
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: :ok

  defp with_guards([], body), do: body
  defp with_guards(guards, body), do: {:__block__, [], List.wrap(guards) ++ [body]}
end
