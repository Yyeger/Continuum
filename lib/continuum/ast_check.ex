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
    {:pg, :join} => "cluster topology is non-deterministic; wrap in an activity",
    {:pg, :leave} => "cluster topology is non-deterministic; wrap in an activity",
    {:pg, :get_members} => "cluster topology is non-deterministic; wrap in an activity",
    {:pg, :get_local_members} => "cluster topology is non-deterministic; wrap in an activity",
    {:rpc, :call} => "remote calls are non-deterministic; wrap in an activity",
    {:rpc, :cast} => "remote calls are non-deterministic; wrap in an activity",
    {:rpc, :multicall} => "remote calls are non-deterministic; wrap in an activity",
    {:erpc, :call} => "remote calls are non-deterministic; wrap in an activity",
    {:erpc, :cast} => "remote calls are non-deterministic; wrap in an activity",
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
    {Kernel, :spawn_monitor} => "spawn forbidden in workflows",
    {Kernel, :send} => "send messages outside the workflow, or wrap in an activity",
    {Kernel, :self} => "pid identity is non-deterministic on replay; wrap in an activity",
    {Kernel, :make_ref} => "use Continuum.uuid4/0",
    {Kernel, :node} => "cluster topology is non-deterministic; wrap in an activity",
    {Function, :capture} =>
      "Function.capture/3 builds dynamic dispatch the scanner cannot follow; call the function directly",
    {Code, :eval_string} => "code evaluation is non-deterministic",
    {Code, :eval_quoted} => "code evaluation is non-deterministic",
    {:erlang, :apply} => "dynamic dispatch forbidden in workflows",
    {:erlang, :now} => "use Continuum.now/0 (and :erlang.now is deprecated)",
    {:erlang, :spawn} => "spawn forbidden in workflows",
    {:erlang, :spawn_link} => "spawn forbidden in workflows",
    {:erlang, :spawn_monitor} => "spawn forbidden in workflows",
    {:erlang, :phash2} => "phash2 salt may change across OTP releases",
    {:erlang, :system_time} => "use Continuum.now/0",
    {:erlang, :monotonic_time} => "monotonic time is non-deterministic on replay",
    {:erlang, :unique_integer} => "use Continuum.uuid4/0",
    {:erlang, :make_ref} => "use Continuum.uuid4/0",
    {:erlang, :self} => "pid identity is non-deterministic on replay; wrap in an activity",
    {:erlang, :send} => "send messages outside the workflow, or wrap in an activity",
    {:os, :system_time} => "use Continuum.now/0",
    {:os, :timestamp} => "use Continuum.now/0"
  }

  # Unqualified spellings of forbidden auto-imported Kernel functions, used
  # when `scan/2` runs without a caller env (the env-aware path resolves any
  # local call through `Macro.Env.lookup_import/2` instead, so it also catches
  # `import DateTime`-style imports and respects user shadowing).
  @forbidden_locals %{
    {:apply, 2} => {Kernel, :apply},
    {:apply, 3} => {Kernel, :apply},
    {:spawn, 1} => {Kernel, :spawn},
    {:spawn, 3} => {Kernel, :spawn},
    {:spawn_link, 1} => {Kernel, :spawn_link},
    {:spawn_link, 3} => {Kernel, :spawn_link},
    {:spawn_monitor, 1} => {Kernel, :spawn_monitor},
    {:spawn_monitor, 3} => {Kernel, :spawn_monitor},
    {:send, 2} => {Kernel, :send},
    {:self, 0} => {Kernel, :self},
    {:make_ref, 0} => {Kernel, :make_ref},
    {:node, 0} => {Kernel, :node},
    {:node, 1} => {Kernel, :node}
  }

  @receive_hint "receive blocks bypass the journal and are non-deterministic on replay; " <>
                  "use `await signal(...)` (or `timer/1`) instead"

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

  Pass the caller's `%Macro.Env{}` (as `Continuum.Workflow` and
  `Continuum.Pure` do) so unqualified calls are resolved through the imports
  in scope — `import DateTime` followed by a bare `utc_now()` is caught the
  same as the qualified spelling. Passing just a `file` string keeps
  diagnostics located but limits local-call detection to the auto-imported
  Kernel denylist.
  """
  @spec scan(Macro.t(), Macro.Env.t() | String.t() | nil) :: :ok | {:error, [violation()]}
  def scan(ast, file_or_env \\ nil)

  def scan(ast, %Macro.Env{} = env), do: do_scan(ast, env.file, env)
  def scan(ast, file), do: do_scan(ast, file, nil)

  defp do_scan(ast, file, env) do
    acc = %{violations: [], imports: [], aliases: %{}}
    ast = normalize_pipes(ast)
    {_, %{violations: violations}} = Macro.prewalk(ast, acc, &check_node(&1, &2, file, env))

    case Enum.reverse(violations) do
      [] -> :ok
      list -> {:error, list}
    end
  end

  # `x |> send(:msg)` parses as `send/1`, so exact-arity denylist lookups miss
  # it. Rewrite pipes into the call they expand to before scanning, so the
  # effective arity is checked.
  defp normalize_pipes(ast) do
    Macro.prewalk(ast, fn
      {:|>, _meta, [lhs, {fun, call_meta, args}]} when is_atom(fun) or is_tuple(fun) ->
        {fun, call_meta, [lhs | List.wrap(args)]}

      node ->
        node
    end)
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

  @doc """
  Warn on `catch` arms inside workflow clauses.

  Continuum suspends a workflow by throwing a control tuple *after* the
  pending effect has been journaled; a `catch` arm (especially `_, _ ->` or
  `:throw, _ ->`) can intercept it. The runtime detects the swallow and
  fails the run with `Continuum.SuspendLeakError`, but the right fix is in
  the code: use `rescue`/`after`, or re-throw the engine's control tuples.
  """
  @spec check_catch_warnings(Macro.t(), Macro.Env.t(), atom(), non_neg_integer()) :: :ok
  def check_catch_warnings(ast, env, caller_fun, caller_arity) do
    {_ast, catches} =
      Macro.prewalk(ast, [], fn
        {:try, meta, [blocks]} = node, acc when is_list(blocks) ->
          if Keyword.has_key?(blocks, :catch) do
            {node, [%{line: Keyword.get(meta, :line)} | acc]}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    catches
    |> Enum.reverse()
    |> Enum.each(&warn_catch_arm(&1, env, caller_fun, caller_arity))

    :ok
  end

  defp warn_catch_arm(found, env, caller_fun, caller_arity) do
    IO.warn(
      """
      Continuum workflow code uses a `catch` arm inside `try`.

      Continuum suspends this workflow by throwing a control tuple after the
      pending effect is journaled; a `catch` arm can swallow that throw, and
      the run then fails with Continuum.SuspendLeakError instead of suspending.

      Use `rescue` (and `after`) instead, or re-throw the engine's control
      tuples from every catch arm:

          catch
            :throw, {:continuum_suspend, _} = signal -> throw(signal)
            :throw, {:continuum_continued_as_new, _} = signal -> throw(signal)
      """,
      [
        {env.module, caller_fun, caller_arity,
         [file: to_charlist(env.file || "nofile"), line: found.line || env.line || 0]}
      ]
    )
  end

  @doc """
  Warn on dynamic-receiver calls (`some_var.fun(...)`) in workflow code.

  A call whose receiver is a runtime value cannot be checked against the
  denylist — `m = DateTime; m.utc_now()` would silently bypass the scanner.
  Plain field access (`input.seed`, no parentheses) is not flagged.
  """
  @spec check_dynamic_call_warnings(Macro.t(), Macro.Env.t(), atom(), non_neg_integer()) :: :ok
  def check_dynamic_call_warnings(ast, env, caller_fun, caller_arity) do
    {_ast, calls} =
      ast
      |> normalize_pipes()
      |> Macro.prewalk([], fn
        # Captures of dynamic modules: &m.f/1, &input.mod.f/2, ...
        {:&, meta, [{:/, _, [{{:., _, [receiver, fun]}, _, _}, arity]}]} = node, acc
        when is_atom(fun) and is_integer(arity) ->
          if static_receiver?(receiver) do
            {node, acc}
          else
            {node, [dynamic_call(receiver, fun, arity, meta) | acc]}
          end

        {{:., _, [receiver, fun]}, meta, args} = node, acc
        when is_atom(fun) and is_list(args) ->
          cond do
            # Plain field access (input.seed) stays silent.
            args == [] and Keyword.get(meta, :no_parens, false) -> {node, acc}
            static_receiver?(receiver) -> {node, acc}
            true -> {node, [dynamic_call(receiver, fun, length(args), meta) | acc]}
          end

        node, acc ->
          {node, acc}
      end)

    calls
    |> Enum.reverse()
    |> Enum.each(&warn_dynamic_call(&1, env, caller_fun, caller_arity))

    :ok
  end

  # Receivers the main scanner can resolve at compile time; everything else is
  # a runtime value the denylist cannot be checked against — single variables,
  # chained field access (input.mod), call results, and so on.
  defp static_receiver?(receiver) do
    case receiver do
      {:__aliases__, _, _} -> true
      module when is_atom(module) -> true
      {:__MODULE__, _, ctx} when is_atom(ctx) -> true
      {:__ENV__, _, ctx} when is_atom(ctx) -> true
      _other -> false
    end
  end

  defp dynamic_call(receiver, fun, arity, meta) do
    %{
      receiver: Macro.to_string(receiver),
      function: fun,
      arity: arity,
      line: Keyword.get(meta, :line)
    }
  end

  defp warn_dynamic_call(call, env, caller_fun, caller_arity) do
    IO.warn(
      """
      Continuum cannot analyze a dynamic-receiver call in workflow code:

          #{call.receiver}.#{call.function}/#{call.arity}

      The receiver is a runtime value, so the determinism scanner cannot
      check it against the denylist. Call the module directly, or move the
      dynamic dispatch into an activity.
      """,
      [
        {env.module, caller_fun, caller_arity,
         [file: to_charlist(env.file || "nofile"), line: call.line || env.line || 0]}
      ]
    )
  end

  @doc false
  @spec collect_compensation_sites(Macro.t(), Macro.Env.t(), atom(), non_neg_integer()) :: :ok
  def collect_compensation_sites(ast, env, caller_fun, caller_arity) do
    Macro.prewalk(ast, fn
      {:activity, meta, args} = node when is_list(args) ->
        Module.put_attribute(env.module, :continuum_activity_sites, %{
          fun: caller_fun,
          arity: caller_arity,
          line: Keyword.get(meta, :line),
          status: activity_compensation_status(args)
        })

        node

      {:compensate_all, _meta, args} = node when is_list(args) ->
        Module.put_attribute(env.module, :continuum_compensate_all_sites, %{
          fun: caller_fun,
          arity: caller_arity
        })

        node

      node ->
        node
    end)

    :ok
  end

  @doc false
  @spec emit_compensation_warnings(Macro.Env.t()) :: :ok
  def emit_compensation_warnings(env) do
    compensate_all_sites = Module.get_attribute(env.module, :continuum_compensate_all_sites) || []
    activity_sites = Module.get_attribute(env.module, :continuum_activity_sites) || []

    if compensate_all_sites != [] do
      activity_sites
      |> Enum.filter(&(&1.status == :uncompensated))
      |> Enum.sort_by(&{&1.fun, &1.line})
      |> Enum.each(&warn_uncompensated_activity(&1, env))
    end

    :ok
  end

  # ---------------------------------------------------------------------------

  # Literal opts are analyzable; a variable/dynamic opts argument is not, and
  # an unanalyzable site must be silent rather than falsely flagged.
  defp activity_compensation_status([_call]), do: :uncompensated

  defp activity_compensation_status([_call, opts]) when is_list(opts) do
    cond do
      not Keyword.keyword?(opts) -> :unanalyzable
      Keyword.has_key?(opts, :compensate) -> :compensated
      true -> :uncompensated
    end
  end

  defp activity_compensation_status([_call, _dynamic_opts]), do: :unanalyzable
  defp activity_compensation_status(_args), do: :unanalyzable

  defp warn_uncompensated_activity(site, env) do
    IO.warn(
      """
      Continuum workflow uses compensate_all but has an activity without `compensate:`.

      Add `compensate: {Mod, :fun, args}` to make rollback explicit, or use
      `compensate: :none` to mark this activity as intentionally not compensated.
      """,
      [
        {env.module, site.fun, site.arity,
         [file: to_charlist(env.file || "nofile"), line: site.line || env.line || 0]}
      ]
    )
  end

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

  # Aliases are resolved before the denylist lookup: an in-body
  # `alias DateTime, as: D` is tracked by the walk; module-level aliases come
  # from the caller env via `Macro.expand/2`. Both directions matter —
  # `D.utc_now()` must be a hard error, and a user's own
  # `MyApp.Legacy.DateTime` aliased as `DateTime` must NOT be.
  defp check_node(
         {{:., _, [{:__aliases__, _, _} = alias_ast, fun]}, meta, args} = node,
         acc,
         file,
         env
       )
       when is_atom(fun) and is_list(args) do
    mod = resolve_alias(alias_ast, acc.aliases, env)
    {node, maybe_record(mod, fun, meta, file, acc)}
  end

  defp check_node({{:., _, [mod, fun]}, meta, args} = node, acc, file, _env)
       when is_atom(mod) and is_atom(fun) and is_list(args) do
    {node, maybe_record(mod, fun, meta, file, acc)}
  end

  # `receive` parks the process outside the journal: on replay nothing sends
  # the message again, so the workflow blocks (or takes the `after` branch)
  # differently than it did originally.
  defp check_node({:receive, meta, args} = node, acc, file, _env)
       when is_list(args) do
    violation = %{
      mfa: {Kernel.SpecialForms, :receive},
      line: Keyword.get(meta, :line),
      file: file,
      hint: @receive_hint
    }

    {node, record(acc, violation)}
  end

  # Track in-body `import` directives so later unqualified calls in the same
  # definition resolve against them even when no caller env is available.
  defp check_node({:import, _meta, [module_ast | rest]} = node, acc, _file, env) do
    case import_spec(module_ast, rest, env) do
      {:ok, spec} -> {node, %{acc | imports: [spec | acc.imports]}}
      :error -> {node, acc}
    end
  end

  # Track in-body `alias` directives (single, `as:`, and `Mod.{A, B}` forms).
  defp check_node({:alias, _meta, [aliased | rest]} = node, acc, _file, env) do
    {node, %{acc | aliases: Map.merge(acc.aliases, alias_entries(aliased, rest, env))}}
  end

  # Local (unqualified) calls: `apply(...)`, `spawn(...)`, `send(self(), ...)`,
  # or a forbidden function pulled in by `import`.
  defp check_node({fun, meta, args} = node, acc, file, env)
       when is_atom(fun) and is_list(meta) and is_list(args) do
    arity = length(args)

    hit =
      walked_import_hit(acc.imports, fun, arity) ||
        env_import_hit(env, fun, arity) ||
        fallback_local_hit(env, fun, arity)

    case hit do
      {mod, canonical_fun, hint} ->
        violation = %{
          mfa: {mod, canonical_fun},
          line: Keyword.get(meta, :line),
          file: file,
          hint: hint
        }

        {node, record(acc, violation)}

      nil ->
        {node, acc}
    end
  end

  defp check_node(node, acc, _file, _env), do: {node, acc}

  defp record(acc, violation), do: %{acc | violations: [violation | acc.violations]}

  defp maybe_record(mod, fun, meta, file, acc) do
    case Map.fetch(@forbidden, {mod, fun}) do
      {:ok, hint} ->
        record(acc, %{
          mfa: {mod, fun},
          line: Keyword.get(meta, :line),
          file: file,
          hint: hint
        })

      :error ->
        acc
    end
  end

  defp resolve_alias({:__aliases__, _, [first | rest]} = alias_ast, aliases, env)
       when is_atom(first) do
    case Map.fetch(aliases, first) do
      {:ok, target} when rest == [] -> target
      {:ok, target} -> Module.concat([target | rest])
      :error -> expand_alias(alias_ast, env)
    end
  end

  defp resolve_alias(alias_ast, _aliases, env), do: expand_alias(alias_ast, env)

  defp expand_alias(alias_ast, %Macro.Env{} = env), do: Macro.expand(alias_ast, env)

  defp expand_alias({:__aliases__, _, parts}, nil) do
    if Enum.all?(parts, &is_atom/1), do: Module.concat(parts), else: nil
  end

  defp alias_entries({:__aliases__, _, parts} = aliased, rest, env) when is_list(parts) do
    target = expand_alias(aliased, env)

    as_name =
      case alias_as_option(rest) do
        nil -> List.last(parts)
        as_name -> as_name
      end

    if is_atom(as_name) and is_atom(target) and not is_nil(target) do
      %{as_name => target}
    else
      %{}
    end
  end

  # alias Mod.{A, B}
  defp alias_entries(
         {{:., _, [{:__aliases__, _, base_parts} = base, :{}]}, _, inner},
         _rest,
         env
       )
       when is_list(base_parts) and is_list(inner) do
    base_mod = expand_alias(base, env)

    inner
    |> Enum.flat_map(fn
      {:__aliases__, _, [_ | _] = parts} = _inner_alias when is_atom(base_mod) ->
        if Enum.all?(parts, &is_atom/1) do
          [{List.last(parts), Module.concat([base_mod | parts])}]
        else
          []
        end

      _other ->
        []
    end)
    |> Map.new()
  end

  defp alias_entries(_aliased, _rest, _env), do: %{}

  defp alias_as_option([opts]) when is_list(opts) do
    case Keyword.get(opts, :as) do
      {:__aliases__, _, [as_name]} when is_atom(as_name) -> as_name
      _ -> nil
    end
  end

  defp alias_as_option(_rest), do: nil

  defp import_spec(module_ast, rest, env) do
    opts =
      case rest do
        [opts] when is_list(opts) -> opts
        _ -> []
      end

    case import_module(module_ast, env) do
      mod when is_atom(mod) and not is_nil(mod) ->
        {:ok,
         %{
           module: mod,
           only: literal_fa_list(opts[:only]),
           except: literal_fa_list(opts[:except])
         }}

      _ ->
        :error
    end
  end

  defp import_module({:__aliases__, _, _} = alias_ast, %Macro.Env{} = env),
    do: Macro.expand(alias_ast, env)

  defp import_module({:__aliases__, _, parts}, nil) do
    if Enum.all?(parts, &is_atom/1), do: Module.concat(parts), else: nil
  end

  defp import_module(mod, _env) when is_atom(mod), do: mod
  defp import_module(_module_ast, _env), do: nil

  # `only:`/`except:` are honored when they are literal `[fun: arity]` lists;
  # anything else (`only: :functions`, computed lists) is treated as a full
  # import — over-approximating keeps detection sound.
  defp literal_fa_list(list) when is_list(list) do
    if Enum.all?(list, fn
         {fun, arity} when is_atom(fun) and is_integer(arity) -> true
         _ -> false
       end),
       do: list,
       else: nil
  end

  defp literal_fa_list(_other), do: nil

  defp walked_import_hit(imports, fun, arity) do
    Enum.find_value(imports, fn %{module: mod, only: only, except: except} ->
      cond do
        is_list(only) and {fun, arity} not in only -> nil
        is_list(except) and {fun, arity} in except -> nil
        true -> forbidden_hit(mod, fun)
      end
    end)
  end

  defp env_import_hit(%Macro.Env{} = env, fun, arity) do
    env
    |> Macro.Env.lookup_import({fun, arity})
    |> Enum.find_value(fn {_kind, mod} -> forbidden_hit(mod, fun) end)
  end

  defp env_import_hit(_env, _fun, _arity), do: nil

  # Without a caller env there is nothing to resolve imports against; fall
  # back to the auto-imported Kernel spellings everyone actually writes.
  defp fallback_local_hit(nil, fun, arity) do
    case Map.fetch(@forbidden_locals, {fun, arity}) do
      {:ok, {mod, canonical_fun}} -> forbidden_hit(mod, canonical_fun)
      :error -> nil
    end
  end

  defp fallback_local_hit(_env, _fun, _arity), do: nil

  defp forbidden_hit(mod, fun) do
    case Map.fetch(@forbidden, {mod, fun}) do
      {:ok, hint} -> {mod, fun, hint}
      :error -> nil
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
