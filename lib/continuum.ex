defmodule Continuum do
  @moduledoc """
  OTP-native durable execution engine for Elixir.

  Continuum lets you write a multi-step business process as straight-line
  Elixir code. The process survives crashes, node restarts, and partitions:
  the engine journals each effect to Postgres and replays the workflow's
  history through the same orchestration code on resume.

  See `Continuum.Workflow` for the workflow DSL and `Continuum.Activity` for
  activities (the only place side effects are allowed inside a workflow).

  ## Public API

    * `children/1` — Postgres runtime child specs for host supervision trees
    * `start/3` — start a new workflow run
    * `signal/3` — deliver an external signal to a running workflow
    * `cancel/2` — cancel a running workflow
    * `await/2` — block until a workflow completes (test/synchronous use)
    * `query/1` and `get_run/2` — inspect durable runs
    * `set_attributes/3` — externally update run search attributes
    * `now/0`, `uuid4/0`, `random/0`, `side_effect/1` — deterministic primitives
      callable from workflow code
    * `patched?/1` — journaled patch marker for compatible workflow changes
  """

  alias Continuum.Runtime.{Context, Effect}

  @type run_id :: binary()
  @type workflow_module :: module()
  @type input :: term()

  @doc """
  Returns runtime child specs for a named, non-default Continuum instance.

      children =
        [
          MyApp.Repo,
          Continuum.children(name: :billing_continuum, repo: MyApp.Repo)
        ]

  The default `Continuum` instance is owned by `Continuum.Application` and
  `Continuum.children()` returns `[]` to avoid duplicate process names.

  A named instance given a `:repo` uses the Postgres journal for every run
  started, signalled, cancelled, or awaited through it; pass `:journal` to
  override. The default instance follows `config :continuum, :journal`.

  Child-specific options may be passed with `:workflow_modules`,
  `:activity_executor`, `:heartbeater`, `:run_supervisor`,
  `:activity_supervisor`, `:recovery`, `:dispatcher`, `:activity_dispatcher`,
  `:timer_wheel`, `:signal_router`, and `:snapshotter`.
  Passing `false` for a child omits it from the returned list.
  """
  @spec children(keyword()) :: [Supervisor.child_spec()]
  def children(opts \\ []) do
    name = Keyword.get(opts, :name, Continuum)

    if name == Continuum do
      []
    else
      instance =
        Continuum.Runtime.Instance.new(
          name: name,
          repo: opts[:repo],
          journal: opts[:journal],
          activity_executor: Keyword.get(opts, :activity_executor, :builtin),
          workflow_modules: opts[:workflow_modules]
        )
        |> Continuum.Runtime.Instance.register()

      [
        Supervisor.child_spec({Phoenix.PubSub, name: instance.pubsub},
          id: {Phoenix.PubSub, instance.name}
        ),
        Supervisor.child_spec({Registry, keys: :unique, name: instance.registry},
          id: {Registry, instance.name}
        ),
        child(
          Continuum.Runtime.Lease.Heartbeater,
          Keyword.get(opts, :heartbeater, []),
          instance
        ),
        child(
          Continuum.Runtime.RunSupervisor,
          Keyword.get(opts, :run_supervisor, []),
          instance
        ),
        activity_supervisor_child(opts, instance),
        child(Continuum.Runtime.Recovery, Keyword.get(opts, :recovery, []), instance),
        child(Continuum.Runtime.Dispatcher, Keyword.get(opts, :dispatcher, []), instance),
        child(
          Continuum.Runtime.ActivityWorker.Dispatcher,
          Keyword.get(opts, :activity_dispatcher, []),
          instance
        ),
        child(Continuum.Runtime.Snapshotter, Keyword.get(opts, :snapshotter, []), instance),
        child(Continuum.Runtime.TimerWheel, Keyword.get(opts, :timer_wheel, []), instance),
        child(Continuum.Runtime.SignalRouter, Keyword.get(opts, :signal_router, []), instance),
        child(Continuum.VersionRegistry, Keyword.get(opts, :version_registry, []), instance)
      ]
      |> Enum.reject(&is_nil/1)
    end
  end

  @doc """
  Start a new workflow run.

  Options include `:instance` for selecting a named Continuum instance,
  `:namespace` for soft tenant scoping of list/query paths, `:trace_context` for
  persisting an opaque W3C traceparent binary that
  observability integrations can use to link resumed run attempts, and
  `:attributes` for JSON-encodable search metadata stored on the run row.
  """
  @spec start(workflow_module(), input(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def start(workflow_module, input, opts \\ []) do
    Continuum.Runtime.Engine.start_run(workflow_module, input, opts)
  end

  @doc """
  Deliver a signal to a running workflow.
  """
  @spec signal(run_id(), atom(), term()) :: :ok | {:error, term()}
  def signal(run_id, name, payload) do
    signal(run_id, name, payload, [])
  end

  @doc """
  Deliver a signal to a running workflow, selecting a Continuum instance with
  `:instance`.
  """
  @spec signal(run_id(), atom(), term(), keyword()) :: :ok | {:error, term()}
  def signal(run_id, name, payload, opts) do
    Continuum.Runtime.SignalRouter.deliver(run_id, name, payload, opts)
  end

  @doc """
  Cancel a running workflow.
  """
  @spec cancel(run_id(), keyword()) :: :ok | {:error, term()}
  def cancel(run_id, opts \\ []) do
    Continuum.Runtime.Engine.cancel(run_id, opts)
  end

  @doc """
  Block until the run completes. Test/synchronous use only.
  """
  @spec await(run_id(), timeout(), keyword()) :: {:ok, map()} | {:error, term()}
  def await(run_id, timeout \\ 5_000, opts \\ []) do
    Continuum.Runtime.Engine.await(run_id, timeout, opts)
  end

  @doc """
  Query durable runs with a closed, structured query spec.

  See `Continuum.Query` for supported `:where`, `:order_by`, and pagination
  options. Querying requires a Postgres-backed Continuum instance.
  """
  @spec query(keyword()) :: {:ok, map()} | {:error, term()}
  def query(opts \\ []) do
    Continuum.Query.list(opts)
  end

  @doc """
  Query durable runs for a named Continuum instance.
  """
  @spec query(atom() | Continuum.Runtime.Instance.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def query(instance, opts) do
    Continuum.Query.list(Keyword.put(opts, :instance, instance))
  end

  @doc """
  Load one durable run by id.
  """
  @spec get_run(run_id(), keyword()) :: {:ok, map()} | {:error, :not_found | term()}
  def get_run(run_id, opts \\ []) do
    Continuum.Query.get_run(run_id, opts)
  end

  @doc """
  Merge JSON-encodable search attributes into a durable run row.

  This is external metadata. It is not journaled and workflow code cannot read
  it during replay.
  """
  @spec set_attributes(run_id(), map(), keyword()) :: :ok | {:error, term()}
  def set_attributes(run_id, attributes, opts \\ []) do
    Continuum.Query.set_attributes(run_id, attributes, opts)
  end

  # ---------------------------------------------------------------------------
  # Deterministic primitives — callable only from inside a workflow process.
  # Each consults the journal first; on first execution journals the value;
  # on replay returns the journaled value.
  # ---------------------------------------------------------------------------

  @doc """
  The current wall-clock time, journaled and replayed deterministically.
  """
  defmacro now do
    command = command_base(__CALLER__, :now)

    quote do
      Continuum.Runtime.Effect.run(
        {:side_effect, :now},
        {:command, unquote(Macro.escape(command)), &DateTime.utc_now/0}
      )
    end
  end

  @doc """
  The current UTC date, journaled and replayed deterministically.
  """
  defmacro today do
    command = command_base(__CALLER__, :today)

    quote do
      Continuum.Runtime.Effect.run(
        {:side_effect, :today},
        {:command, unquote(Macro.escape(command)), &Date.utc_today/0}
      )
    end
  end

  @doc """
  A v4 UUID, journaled and replayed deterministically.
  """
  defmacro uuid4 do
    command = command_base(__CALLER__, :uuid4)

    quote do
      Continuum.Runtime.Effect.run(
        {:side_effect, :uuid4},
        {:command, unquote(Macro.escape(command)), &Continuum.__generate_uuid4__/0}
      )
    end
  end

  @doc """
  A pseudo-random float in [0, 1), journaled and replayed deterministically.
  """
  defmacro random do
    command = command_base(__CALLER__, :random)

    quote do
      Continuum.Runtime.Effect.run(
        {:side_effect, :random},
        {:command, unquote(Macro.escape(command)), &:rand.uniform_real/0}
      )
    end
  end

  @doc """
  General-purpose escape hatch for an impure read whose result must be
  journaled and replayed.

  The function is called once on first execution; its return value is
  journaled and returned on every subsequent replay. Return values must be
  serializable via `:erlang.term_to_binary/1` — pids, refs, ports, and
  similar local-only terms are rejected.

  This is a macro so Continuum can capture the source call site for a stable
  command identity. Workflow modules that `use Continuum.Workflow` already
  require `Continuum`; other modules must `require Continuum` before calling it.

  > #### Helper-module caveat {: .warning}
  >
  > Command identity includes the call site's module and line. Inside a
  > *workflow* module that is safe: any edit changes the version hash and
  > in-flight runs keep resuming through the old version's entrypoint. A
  > `Continuum.Pure` helper module has no such protection — editing a helper
  > so that a `side_effect` call moves to a different line changes its command
  > identity and in-flight runs replaying through it raise
  > `Continuum.ReplayDriftError` on the next deploy. Prefer keeping
  > `side_effect` calls in the workflow module itself.
  """
  defmacro side_effect(fun) do
    command = command_base(__CALLER__, :user)

    quote do
      Continuum.__side_effect__(
        unquote(fun),
        unquote(Macro.escape(command))
      )
    end
  end

  @doc false
  @spec __side_effect__((-> term()), term()) :: term()
  def __side_effect__(fun, command_base) when is_function(fun, 0) do
    Effect.run({:side_effect, :user}, {:command, command_base, fun})
  end

  @doc """
  Recover an activity's raw return value from a compensation handle.

  When an `activity/2` call uses `compensate:`, a success is returned as
  `{:ok, %Continuum.ActivityRef{}}` rather than a bare term. `unwrap/1` peels the
  ref back to the activity's raw return:

    * `unwrap(%Continuum.ActivityRef{raw_result: raw})` → `raw`
    * `unwrap({:ok, %Continuum.ActivityRef{} = ref})` → `ref.raw_result`
    * `unwrap(other)` → `other` (activities without `compensate:` are unchanged)
  """
  @doc since: "0.3.0"
  @spec unwrap(term()) :: term()
  def unwrap(%Continuum.ActivityRef{raw_result: raw}), do: raw
  def unwrap({:ok, %Continuum.ActivityRef{raw_result: raw}}), do: raw
  def unwrap(other), do: other

  @doc """
  Whether we are currently executing inside a workflow process. Useful
  in helper modules that branch on context.
  """
  @spec in_workflow?() :: boolean()
  def in_workflow? do
    Context.active?()
  end

  @doc """
  Journaled patch marker for in-place, backward-compatible workflow changes.

      def run(input) do
        if Continuum.patched?(:add_fraud_check_v2) do
          activity FraudCheck.v2(input)
        else
          activity FraudCheck.v1(input)
        end
      end

  Inside a workflow the first call to `patched?(name)` at a given source line
  journals a `patched` event with `value: true` and returns `true`; the value
  is then replayed on resume so the run never changes branch mid-flight. A run
  that is replaying history recorded *before* the patch line existed returns
  `false` without consuming any event, keeping in-flight runs on the old path.

  Outside a workflow process (test setup, ordinary code) it returns `false`.

  Like `now/0` and `uuid4/0`, this is a macro so it captures `__CALLER__` for a
  stable command identity; modules that call it must `require Continuum`
  (`use Continuum.Workflow` does this for you).
  """
  @doc since: "0.3.0"
  defmacro patched?(patch_name) do
    command = command_base(__CALLER__, :patched)
    register_patch_site(__CALLER__, patch_name, command)

    quote do
      Continuum.__patched__(unquote(patch_name), unquote(Macro.escape(command)))
    end
  end

  @doc false
  def __patched__(patch_name, command_base) do
    if Context.active?() do
      Effect.run({:patched, patch_name}, {:command, command_base})
    else
      false
    end
  end

  # ---------------------------------------------------------------------------

  defp child(_module, false, _instance), do: nil
  defp child(module, true, instance), do: child(module, [], instance)

  defp child(module, opts, instance) do
    opts = opts |> List.wrap() |> Keyword.put(:instance, instance)
    Supervisor.child_spec({module, opts}, id: {module, instance.name})
  end

  defp activity_supervisor_child(opts, %{activity_executor: :builtin} = instance) do
    child(
      Continuum.Runtime.ActivityWorker.Supervisor,
      Keyword.get(opts, :activity_supervisor, []),
      instance
    )
  end

  defp activity_supervisor_child(_opts, _instance), do: nil

  @doc false
  def __generate_uuid4__ do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)

    <<a::32, b::16, c::16, d::16, e::48>> =
      <<u0::48, 4::4, u1::12, 2::2, u2::62>>

    :io_lib.format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [a, b, c, d, e])
    |> IO.iodata_to_binary()
  end

  defp command_base(env, kind) do
    {:side_effect, env.module, env.function, env.line, hash_term(kind)}
  end

  defp register_patch_site(%Macro.Env{module: nil}, _patch_name, _command), do: :ok

  defp register_patch_site(env, patch_name, command) do
    Module.put_attribute(env.module, :continuum_patch_sites, %{
      name: patch_name,
      command_id: command,
      file: env.file,
      line: env.line
    })
  rescue
    _ -> :ok
  end

  defp hash_term(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
