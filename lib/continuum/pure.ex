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
    end
  end

  @doc false
  def __on_definition__(env, _kind, _name, _args, _guards, body) when not is_nil(body) do
    case Continuum.AstCheck.scan(body, env.file) do
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
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: :ok
end
