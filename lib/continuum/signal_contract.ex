defmodule Continuum.SignalContract do
  @moduledoc """
  Validation helpers for workflow-declared signal contracts.

  Workflows may declare signal names with built-in type validators or an MFA
  validator. A workflow that omits `:signals` remains open for compatibility;
  once the option is present, undeclared names are rejected.
  """

  alias Continuum.Runtime.Instance

  @builtins [:any, :atom, :binary, :boolean, :integer, :list, :map, :number]

  @type validator ::
          :any
          | :atom
          | :binary
          | :boolean
          | :integer
          | :list
          | :map
          | :number
          | {module(), atom()}
  @type contracts :: nil | %{required(atom()) => validator()}

  @doc false
  @spec normalize_declarations!(nil | keyword() | map()) :: contracts()
  def normalize_declarations!(nil), do: nil

  def normalize_declarations!(declarations) when is_list(declarations) do
    unless Keyword.keyword?(declarations) do
      raise ArgumentError, "workflow :signals must be a keyword list or map"
    end

    declarations |> Map.new() |> normalize_declarations!()
  end

  def normalize_declarations!(declarations) when is_map(declarations) do
    Map.new(declarations, fn {name, validator} ->
      unless is_atom(name) do
        raise ArgumentError, "signal names must be atoms, got: #{inspect(name)}"
      end

      {name, normalize_validator!(name, validator)}
    end)
  end

  def normalize_declarations!(declarations) do
    raise ArgumentError,
          "workflow :signals must be a keyword list or map, got: #{inspect(declarations)}"
  end

  @doc false
  def normalize_declarations!(declarations, env) do
    declarations
    |> Macro.prewalk(fn
      {:__aliases__, _meta, _parts} = alias_ast -> Macro.expand(alias_ast, env)
      node -> node
    end)
    |> declarations_literal!()
    |> normalize_declarations!()
  end

  defp declarations_literal!({:%{}, _meta, entries}), do: Map.new(entries)
  defp declarations_literal!(declarations), do: declarations

  @doc false
  @spec validate_delivery(Instance.t(), module(), binary(), term(), term()) ::
          :ok | {:error, term()}
  def validate_delivery(%Instance{} = instance, journal, run_id, name, payload) do
    with {:ok, contracts} <- contracts_for_run(instance, journal, run_id) do
      validate(contracts, name, payload)
    end
  end

  @doc false
  @spec contracts_for_run(Instance.t(), module(), binary()) ::
          {:ok, contracts()} | {:error, term()}
  def contracts_for_run(%Instance{} = instance, journal, run_id) do
    if function_exported?(journal, :get_run, 2) do
      case journal.get_run(instance, run_id) do
        nil ->
          {:error, :not_found}

        %{workflow: workflow, version_hash: version_hash} ->
          with {:ok, %{entrypoint: entrypoint}} <-
                 Continuum.VersionRegistry.resolve(workflow, version_hash) do
            {:ok, Map.get(entrypoint.__continuum_workflow__(), :signals)}
          end
      end
    else
      # Legacy custom journals do not expose enough version metadata for
      # contract lookup. Preserve their pre-contract open delivery behavior.
      {:ok, nil}
    end
  end

  @doc false
  @spec validate(contracts(), term(), term()) :: :ok | {:error, term()}
  def validate(nil, _name, _payload), do: :ok

  def validate(contracts, name, payload) when is_atom(name) do
    case Map.fetch(contracts, name) do
      :error -> {:error, {:undeclared_signal, name}}
      {:ok, validator} -> apply_validator(name, validator, payload)
    end
  end

  def validate(_contracts, name, _payload), do: {:error, {:invalid_signal_name, name}}

  @doc false
  def check_literal_awaits!(ast, nil, _env), do: ast

  def check_literal_awaits!(ast, contracts, env) do
    Macro.prewalk(ast, fn
      {:await, meta, [{:signal, _signal_meta, [name | _rest]}]} = node when is_atom(name) ->
        unless Map.has_key?(contracts, name) do
          raise CompileError,
            file: env.file,
            line: meta[:line] || env.line,
            description:
              "undeclared signal #{inspect(name)} in #{inspect(env.module)}; " <>
                "declare it with `use Continuum.Workflow, signals: [...]`"
        end

        node

      node ->
        node
    end)
  end

  defp normalize_validator!(_name, validator) when validator in @builtins, do: validator

  defp normalize_validator!(_name, {module, function})
       when is_atom(module) and is_atom(function),
       do: {module, function}

  defp normalize_validator!(name, validator) do
    raise ArgumentError,
          "invalid validator for signal #{inspect(name)}: #{inspect(validator)}; " <>
            "expected one of #{inspect(@builtins)} or {Module, :function}"
  end

  defp apply_validator(_name, :any, _payload), do: :ok
  defp apply_validator(name, :atom, payload), do: typed(name, :atom, is_atom(payload))
  defp apply_validator(name, :binary, payload), do: typed(name, :binary, is_binary(payload))
  defp apply_validator(name, :boolean, payload), do: typed(name, :boolean, is_boolean(payload))
  defp apply_validator(name, :integer, payload), do: typed(name, :integer, is_integer(payload))
  defp apply_validator(name, :list, payload), do: typed(name, :list, is_list(payload))
  defp apply_validator(name, :map, payload), do: typed(name, :map, is_map(payload))
  defp apply_validator(name, :number, payload), do: typed(name, :number, is_number(payload))

  defp apply_validator(name, {module, function}, payload) do
    case apply(module, function, [payload]) do
      :ok -> :ok
      true -> :ok
      {:error, reason} -> {:error, {:invalid_signal_payload, name, reason}}
      false -> {:error, {:invalid_signal_payload, name, :validation_failed}}
      other -> {:error, {:invalid_signal_validator_result, name, other}}
    end
  rescue
    error -> {:error, {:signal_validator_failed, name, Exception.message(error)}}
  catch
    kind, reason -> {:error, {:signal_validator_failed, name, {kind, reason}}}
  end

  defp typed(_name, _expected, true), do: :ok

  defp typed(name, expected, false),
    do: {:error, {:invalid_signal_payload, name, {:expected, expected}}}
end
