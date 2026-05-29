defmodule Continuum.AstCheck do
  @moduledoc """
  Compile-time AST scanner that rejects calls known to be non-deterministic
  inside workflow code.

  The scanner is invoked from `Continuum.Workflow` (and `Continuum.Pure`) at
  module compile time. Each forbidden call produces a `CompileError` with a
  remediation hint pointing at the deterministic equivalent.

  See the `:forbidden_calls/0` and `:trusted_stdlib/0` functions for the
  curated denylist and allowlist. Users can extend the allowlist via:

      config :continuum, trusted_modules: [Decimal, Money]

  Calls from workflow code into helper modules that are not stdlib-trusted,
  allowlisted, or marked with `use Continuum.Pure` emit warnings by default.
  Use `config :continuum, untrusted_call_severity: :error` to make those
  diagnostics fail compilation. Error mode raises on the first untrusted
  helper module found in the current definition.
  """

  @typedoc "A `{module, function}` pair."
  @type call :: {module(), atom()}

  @typedoc "A violation found during AST scan."
  @type violation :: %{
          mfa: call(),
          line: pos_integer() | nil,
          file: String.t() | nil,
          hint: String.t()
        }

  @typedoc "An untrusted external helper call found during AST scan."
  @type helper_call :: %{
          module: module(),
          function: atom(),
          arity: non_neg_integer(),
          line: pos_integer() | nil,
          file: String.t() | nil
        }

  @forbidden %{
    {DateTime, :utc_now} => "use Continuum.now/0",
    {DateTime, :now!} => "use Continuum.now/0",
    {DateTime, :now} => "use Continuum.now/0",
    {Date, :utc_today} => "use Continuum.today/0",
    {NaiveDateTime, :utc_now} => "use Continuum.now/0 then NaiveDateTime.from_iso8601!",
    {Time, :utc_now} => "use Continuum.now/0 |> DateTime.to_time/1",
    {:rand, :uniform} => "use Continuum.random/0",
    {:rand, :uniform_real} => "use Continuum.random/0",
    {:rand, :seed} => "rand seeding is non-deterministic on replay",
    {System, :os_time} => "use Continuum.now/0",
    {System, :system_time} => "use Continuum.now/0",
    {System, :monotonic_time} => "monotonic time is non-deterministic on replay",
    {System, :unique_integer} => "use Continuum.uuid4/0 instead",
    {System, :get_env} => "read env at workflow start; pass it as input",
    {System, :fetch_env!} => "read env at workflow start; pass it as input",
    {Node, :list} => "cluster topology is non-deterministic; wrap in an activity",
    {Node, :self} => "use Continuum.workflow_info/0",
    {Process, :send} => "send messages outside the workflow, or wrap in an activity",
    {Process, :send_after} => "use Continuum.timer/1",
    {Process, :sleep} => "use Continuum.timer/1",
    {Process, :spawn} => "spawn forbidden in workflows; use child workflows",
    {Continuum, :start} =>
      "start workflows outside workflow code; child workflows are planned post-v0.1",
    {Continuum, :signal} =>
      "signal/3 is a side effect; wrap it in Continuum.activity/2 or wait for Continuum.signal_child/2 (v0.3+)",
    {Continuum, :__generate_uuid4__} => "use Continuum.uuid4/0",
    {Continuum, :cancel} =>
      "cancel workflows outside workflow code or wrap cancellation in an activity",
    {Continuum, :await} =>
      "await/2 polls workflow state; use the workflow DSL's await signal(...) form",
    {IO, :puts} => "use Continuum.log/1 (journaled)",
    {IO, :inspect} => "use Continuum.log/1 (journaled)",
    {IO, :write} => "use Continuum.log/1 (journaled)",
    {File, :read} => "wrap in Continuum.activity/2",
    {File, :read!} => "wrap in Continuum.activity/2",
    {File, :write} => "wrap in Continuum.activity/2",
    {File, :write!} => "wrap in Continuum.activity/2",
    {:ets, :lookup} => "ETS bypasses the journal; wrap in an activity",
    {:ets, :insert} => "ETS bypasses the journal; wrap in an activity",
    {:persistent_term, :get} => "persistent_term bypasses the journal",
    {:persistent_term, :put} => "persistent_term bypasses the journal",
    {Kernel, :apply} => "dynamic dispatch forbidden in workflows",
    {Kernel, :spawn} => "spawn forbidden in workflows",
    {Kernel, :spawn_link} => "spawn forbidden in workflows",
    {Code, :eval_string} => "code evaluation is non-deterministic",
    {Code, :eval_quoted} => "code evaluation is non-deterministic",
    {:erlang, :now} => "use Continuum.now/0 (and :erlang.now is deprecated)",
    {:erlang, :spawn} => "spawn forbidden in workflows",
    {:erlang, :phash2} => "phash2 salt may change across OTP releases"
  }

  @trusted_stdlib MapSet.new([
                    Enum,
                    Stream,
                    Map,
                    String,
                    Integer,
                    Float,
                    Decimal,
                    Tuple,
                    List,
                    Keyword,
                    MapSet,
                    Range,
                    Base,
                    URI,
                    Regex,
                    Kernel,
                    Bitwise,
                    Access,
                    Function
                  ])

  @doc "The full denylist as a map of `{mod, fun} => hint`."
  def forbidden_calls, do: @forbidden

  @doc "Stdlib modules considered pure-by-construction."
  def trusted_stdlib, do: @trusted_stdlib

  @doc """
  Scan an AST. Returns `:ok` or `{:error, [violation]}`.

  Pass the originating `file` so diagnostics include it.
  """
  @spec scan(Macro.t(), String.t() | nil) :: :ok | {:error, [violation()]}
  def scan(ast, file \\ nil) do
    {_, violations} = Macro.prewalk(ast, [], &check_node(&1, &2, file))

    case Enum.reverse(violations) do
      [] -> :ok
      list -> {:error, list}
    end
  end

  @doc """
  Format a list of violations into a single human-readable string suitable
  for `CompileError`.
  """
  @spec format([violation()]) :: String.t()
  def format(violations) do
    violations
    |> Enum.map(&format_violation/1)
    |> Enum.join("\n\n")
  end

  @doc """
  Emit or raise diagnostics for external helper modules that are not trusted.

  Activity calls are skipped because their side effects are deliberately routed
  through the DSL and journal. Same-module calls are also skipped; their bodies
  are scanned by the workflow compiler hook.
  """
  @spec check_helper_calls(Macro.t(), Macro.Env.t(), atom(), non_neg_integer()) :: :ok
  def check_helper_calls(ast, env, caller_fun, caller_arity) do
    calls =
      ast
      |> external_calls(env)
      |> Enum.reject(&(&1.module == env.module))
      |> Enum.reject(&trusted_helper_module?/1)
      |> Enum.uniq_by(& &1.module)

    case {untrusted_call_severity(), calls} do
      {_, []} ->
        :ok

      {:warn, calls} ->
        Enum.each(calls, &warn_helper_call(&1, env, caller_fun, caller_arity))

      {:error, [call | _]} ->
        raise CompileError,
          file: call.file || env.file,
          line: call.line || env.line,
          description: helper_call_message(call, env, caller_fun, caller_arity)
    end
  end

  # ---------------------------------------------------------------------------

  defp external_calls(ast, env) do
    {_, calls} =
      Macro.prewalk(ast, [], fn
        {:activity, meta, args}, acc when is_list(args) ->
          {{:__continuum_skipped_activity__, meta, []}, acc}

        # `child Mod.run(args)` names a child workflow, not a helper call.
        {:child, meta, args}, acc when is_list(args) ->
          {{:__continuum_skipped_child__, meta, []}, acc}

        node, acc ->
          collect_external_call(node, acc, env)
      end)

    Enum.reverse(calls)
  end

  defp collect_external_call(
         {{:., _, [module_ast, fun]}, meta, args} = node,
         acc,
         env
       )
       when is_atom(fun) and is_list(args) do
    case static_module(module_ast, env) do
      module when is_atom(module) and module not in [nil, true, false] ->
        call = %{
          module: module,
          function: fun,
          arity: length(args),
          line: Keyword.get(meta, :line),
          file: env.file
        }

        {node, [call | acc]}

      _ ->
        {node, acc}
    end
  end

  defp collect_external_call(node, acc, _env), do: {node, acc}

  defp static_module({:__aliases__, _, _} = alias_ast, env), do: Macro.expand(alias_ast, env)
  defp static_module({:__MODULE__, _, _}, env), do: env.module
  defp static_module(module, _env) when is_atom(module), do: module
  defp static_module(_module, _env), do: nil

  defp trusted_helper_module?(%{module: module}) do
    module in [Continuum, Continuum.Workflow] or
      MapSet.member?(@trusted_stdlib, module) or
      module in trusted_modules() or
      pure_module?(module)
  end

  defp trusted_modules do
    :continuum
    |> Application.get_env(:trusted_modules, [])
    |> List.wrap()
  end

  defp pure_module?(module) do
    case Code.ensure_compiled(module) do
      {:module, ^module} -> function_exported?(module, :__continuum_pure__, 0)
      _ -> false
    end
  end

  defp untrusted_call_severity do
    case Application.get_env(:continuum, :untrusted_call_severity, :warn) do
      :error -> :error
      _ -> :warn
    end
  end

  defp warn_helper_call(call, env, caller_fun, caller_arity) do
    IO.warn(
      helper_call_message(call, env, caller_fun, caller_arity),
      [
        {env.module, caller_fun, caller_arity,
         [file: to_charlist(call.file || env.file || "nofile"), line: call.line || env.line || 0]}
      ]
    )
  end

  defp helper_call_message(call, env, caller_fun, caller_arity) do
    location =
      case {call.file || env.file, call.line || env.line} do
        {nil, nil} -> ""
        {nil, line} -> "line #{line}\n\n"
        {file, nil} -> "#{file}\n\n"
        {file, line} -> "#{file}:#{line}\n\n"
      end

    """
    Continuum cannot determine whether #{inspect(call.module)} is deterministic.

    #{location}Called from #{inspect(env.module)}.#{caller_fun}/#{caller_arity}:
        #{inspect(call.module)}.#{call.function}/#{call.arity}

    - If the module is pure helpers, add `use Continuum.Pure` at the top of #{inspect(call.module)}.
    - If the module performs side effects, wrap the call in `activity/2`.
    - If you've audited it externally, list it in `config :continuum, trusted_modules: [#{inspect(call.module)}, ...]`.
    """
    |> String.trim_trailing()
  end

  defp check_node({{:., _, [{:__aliases__, _, alias_parts}, fun]}, meta, args} = node, acc, file)
       when is_atom(fun) and is_list(args) do
    mod = Module.concat(alias_parts)
    {node, maybe_record(mod, fun, meta, file, acc)}
  end

  defp check_node({{:., _, [mod, fun]}, meta, args} = node, acc, file)
       when is_atom(mod) and is_atom(fun) and is_list(args) do
    {node, maybe_record(mod, fun, meta, file, acc)}
  end

  defp check_node(node, acc, _file), do: {node, acc}

  defp maybe_record(mod, fun, meta, file, acc) do
    case Map.fetch(@forbidden, {mod, fun}) do
      {:ok, hint} ->
        violation = %{
          mfa: {mod, fun},
          line: Keyword.get(meta, :line),
          file: file,
          hint: hint
        }

        [violation | acc]

      :error ->
        acc
    end
  end

  defp format_violation(%{mfa: {mod, fun}, line: line, file: file, hint: hint}) do
    location =
      case {file, line} do
        {nil, nil} -> ""
        {nil, l} -> "  line #{l}\n"
        {f, nil} -> "  #{f}\n"
        {f, l} -> "  #{f}:#{l}\n"
      end

    """
    == Determinism violation ==
    #{location}      #{Inspect.Atom.inspect(mod, %Inspect.Opts{})}.#{fun}

      #{hint}
    """
    |> String.trim_trailing()
  end
end
