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
  """

  defmacro __using__(_opts) do
    quote do
      @on_definition Continuum.Pure

      def __continuum_pure__, do: true
    end
  end

  @doc false
  def __on_definition__(env, _kind, name, args, _guards, body) when not is_nil(body) do
    case Continuum.AstCheck.scan(body, env) do
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
    Continuum.AstCheck.check_catch_warnings(body, env, name, length(args || []))

    # A Pure module is wholly trusted from workflow code, so trust must be
    # transitive: calls into unmarked modules and dynamic receivers get the
    # same diagnostics as workflow clauses — otherwise `use Continuum.Pure`
    # launders unscanned calls past the untrusted_call_severity policy.
    Continuum.AstCheck.check_helper_calls(body, env, name, length(args || []))
    Continuum.AstCheck.check_dynamic_call_warnings(body, env, name, length(args || []))
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: :ok
end
