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

  Or wait for a child workflow. The shorthand accepts exactly
  `child Mod.run(input)`; use `start_child/3` for other setup shapes.

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

  defmacro await({:child, _, [{{:., _, [mod_alias, :run]}, _, [input]}]}) do
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

  defmacro await({:child, _, [_other]}) do
    raise ArgumentError,
          "`await child ...` expects exactly `await child Mod.run(input)`; " <>
            "use `start_child/3` and `await_child/1` for other child workflow shapes"
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
  Macro: tail-call continuation — complete this run and start a fresh one on the
  same workflow with new input.

      def run(%{cycles_done: n} = state) do
        activity Billing.charge(state.customer_id)
        timer(days(30))

        if n >= 11 do
          {:ok, :year_complete}
        else
          continue_as_new(%{state | cycles_done: n + 1})
        end
      end

  The current run is marked `completed` with `result: {:continued, next_run_id}`;
  a new run starts with the given input, sharing the chain's `correlation_id`,
  `namespace`, and `attributes`. Use it to keep history bounded for
  long-running / cron-style workflows.

  Children started but not yet awaited when the run continues are re-parented
  to the successor so cancelling the chain still cascades into them. The
  successor cannot *await* them, however — their `child_started` events live in
  the predecessor's history. Await every child you need a result from before
  calling `continue_as_new/1`.
  """
  @doc since: "0.3.0"
  defmacro continue_as_new(input) do
    command = command_base(__CALLER__, :continue_as_new, :continue_as_new)

    quote do
      Continuum.Runtime.Effect.continue_as_new(
        unquote(input),
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
    quote do
      compensate_all([])
    end
  end

  @doc since: "0.4.0"
  defmacro compensate_all(opts) do
    command = command_base(__CALLER__, :compensate_all, :compensate_all)

    quote do
      Continuum.Runtime.Effect.compensate_all(
        {:command, unquote(Macro.escape(command))},
        unquote(opts)
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
    logical_workflow = Keyword.get(opts, :workflow, Keyword.get(opts, :logical_workflow))

    snapshot_threshold =
      case Keyword.fetch(opts, :snapshot_threshold) do
        {:ok, threshold} -> Continuum.Runtime.Snapshotter.normalize_threshold!(threshold)
        :error -> nil
      end

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
          compensate_all: 1,
          start_child: 2,
          start_child: 3,
          await_child: 1,
          child: 1,
          continue_as_new: 1,
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
      @continuum_snapshot_threshold unquote(snapshot_threshold)
      Module.register_attribute(__MODULE__, :continuum_patch_sites, accumulate: true)
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
    case Continuum.AstCheck.scan(body, env) do
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
    Continuum.AstCheck.check_compensation_warnings(body, env, name, length(args || []))
    Continuum.AstCheck.check_catch_warnings(body, env, name, length(args || []))
    Continuum.AstCheck.check_dynamic_call_warnings(body, env, name, length(args || []))
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: :ok
end

defmodule Continuum.Workflow.BeforeCompile do
  @moduledoc false

  defmacro __before_compile__(env) do
    version = Module.get_attribute(env.module, :continuum_workflow_version)
    retention = Module.get_attribute(env.module, :continuum_workflow_retention)
    logical_workflow = Module.get_attribute(env.module, :continuum_logical_workflow) || env.module
    snapshot_threshold = Module.get_attribute(env.module, :continuum_snapshot_threshold)
    patch_sites = Module.get_attribute(env.module, :continuum_patch_sites) |> Enum.reverse()
    hash = compute_version_hash(env.module)
    generated_module = Module.concat(env.module, :"V_#{hash}")
    generated_definitions = generated_definitions(env.module, generated_module)

    metadata = %{
      module: logical_workflow,
      source_module: env.module,
      version: version,
      retention: retention,
      snapshot_threshold: snapshot_threshold,
      patch_sites: patch_sites,
      version_hash: hash
    }

    quote do
      defmodule unquote(generated_module) do
        @moduledoc false
        unquote_splicing(generated_definitions)

        @doc false
        def __continuum_workflow__ do
          %{
            module: unquote(metadata.module),
            source_module: unquote(metadata.source_module),
            entrypoint: __MODULE__,
            version: unquote(metadata.version),
            retention: unquote(Macro.escape(metadata.retention)),
            snapshot_threshold: unquote(metadata.snapshot_threshold),
            patch_sites: unquote(Macro.escape(metadata.patch_sites)),
            version_hash: unquote(metadata.version_hash)
          }
        end

        @doc false
        def __continuum_entrypoint__, do: __MODULE__
      end

      def __continuum_workflow__ do
        %{
          module: unquote(metadata.module),
          source_module: unquote(metadata.source_module),
          entrypoint: unquote(generated_module),
          version: unquote(metadata.version),
          retention: unquote(Macro.escape(metadata.retention)),
          snapshot_threshold: unquote(metadata.snapshot_threshold),
          patch_sites: unquote(Macro.escape(metadata.patch_sites)),
          version_hash: unquote(metadata.version_hash)
        }
      end

      def __continuum_entrypoint__, do: unquote(generated_module)
    end
  end

  defp generated_definitions(module, generated_module) do
    module
    |> Module.definitions_in()
    |> Enum.sort()
    |> Enum.flat_map(fn {name, arity} ->
      case Module.get_definition(module, {name, arity}) do
        {:v1, kind, _meta, clauses} when kind in [:def, :defp] ->
          Enum.map(clauses, &definition_ast(kind, name, &1, module, generated_module))

        _ ->
          []
      end
    end)
  end

  defp definition_ast(kind, name, {meta, args, guards, body}, module, generated_module) do
    args = rewrite_self_references(args, module, generated_module)
    guards = rewrite_self_references(guards, module, generated_module)
    body = rewrite_self_references(body, module, generated_module)

    head =
      case List.wrap(guards) do
        [] -> {name, meta, args}
        guards -> {:when, meta, [{name, meta, args} | List.flatten(guards)]}
      end

    {kind, meta, [head, [do: body]]}
  end

  defp rewrite_self_references(ast, module, generated_module) do
    Macro.prewalk(ast, fn
      ^module -> generated_module
      other -> other
    end)
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
