defmodule Continuum.CodeIdentity do
  @moduledoc false

  # Resolve every static call before hashing. An unresolved call may name a
  # Pure module defined later in the same file; treating it as non-Pure would
  # make identical source hash differently depending on compilation order.
  # Refuse that ambiguity. Modules in separate files are coordinated by the
  # parallel compiler through ensure_compiled; same-file helpers go first.
  def pin_helpers!(module, env) do
    definitions =
      module
      |> Module.definitions_in()
      |> Enum.sort()
      |> Enum.map(fn name_arity -> {name_arity, Module.get_definition(module, name_arity)} end)

    {definitions, _cache} =
      Enum.map_reduce(definitions, %{}, fn {name_arity, definition}, cache ->
        {definition, cache} = pin_definition(definition, env, cache)
        {{name_arity, definition}, cache}
      end)

    definitions
  end

  defp pin_definition({:v1, kind, meta, clauses}, env, cache) do
    {clauses, cache} =
      Enum.map_reduce(clauses, cache, fn {meta, args, guards, body}, cache ->
        {body, cache} = pin_ast(body, env, cache)
        {guards, cache} = pin_ast(guards, env, cache)
        {{meta, args, guards, body}, cache}
      end)

    {{:v1, kind, meta, clauses}, cache}
  end

  defp pin_ast(ast, env, cache) do
    Macro.prewalk(ast, cache, fn
      {{:., dot_meta, [module, fun]}, meta, args}, cache
      when is_atom(module) and is_atom(fun) and is_list(args) ->
        {target, cache} = helper_target(module, env, cache)
        {{{:., dot_meta, [target, fun]}, meta, args}, cache}

      node, cache ->
        {node, cache}
    end)
  end

  defp helper_target(module, %{module: module}, cache), do: {module, cache}

  defp helper_target(module, env, cache) do
    case Map.fetch(cache, module) do
      {:ok, target} ->
        {target, cache}

      :error ->
        target = resolve_helper!(module, env)
        {target, Map.put(cache, module, target)}
    end
  end

  defp resolve_helper!(module, env) do
    case Code.ensure_compiled(module) do
      {:module, ^module} ->
        pin_loaded_helper!(module, env)

      {:error, reason} ->
        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "cannot verify Continuum helper #{inspect(module)} (#{inspect(reason)}); " <>
              "define helpers before their callers in the same file, or move them " <>
              "to separate files so the compiler can resolve their versions"
    end
  end

  defp pin_loaded_helper!(module, env) do
    if function_exported?(module, :__continuum_pure__, 0) do
      unless function_exported?(module, :__continuum_pure_version__, 0) do
        raise CompileError,
          file: env.file,
          line: env.line,
          description: "recompile #{inspect(module)} to capture its Continuum.Pure version"
      end

      %{entrypoint: entrypoint} =
        Macro.compile_apply(module, :__continuum_pure_version__, [], env)

      entrypoint
    else
      module
    end
  end

  def hash(definitions, extra \\ nil) do
    bodies =
      Enum.flat_map(definitions, fn
        {{name, arity}, {:v1, kind, _meta, clauses}} ->
          Enum.map(clauses, fn {_meta, args, guards, body} ->
            {kind, name, arity, normalize(args), normalize(guards), normalize(body)}
          end)
      end)

    input = if is_nil(extra), do: bodies, else: {bodies, extra}

    input
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  # Keep the established workflow normalization byte-for-byte compatible for
  # workflows with no Pure calls, including compiler-generated capture names.
  defp normalize(ast) do
    Macro.prewalk(ast, fn
      {:"_&", _meta, :elixir_fn} -> {:capture, [], nil}
      {form, meta, args} when is_list(meta) -> {form, [], args}
      other -> other
    end)
  end
end
