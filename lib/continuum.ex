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
    * `now/0`, `uuid4/0`, `random/0`, `side_effect/1` — deterministic primitives
      callable from workflow code
    * `patched?/1` — v0.1 compatibility stub for future workflow patches
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

  Child-specific options may be passed with `:heartbeater`, `:run_supervisor`,
  `:activity_supervisor`, `:recovery`, `:dispatcher`, `:activity_dispatcher`,
  `:timer_wheel`, and `:signal_router`.
  Passing `false` for a child omits it from the returned list.
  """
  @spec children(keyword()) :: [Supervisor.child_spec()]
  def children(opts \\ []) do
    name = Keyword.get(opts, :name, Continuum)

    if name == Continuum do
      []
    else
      instance =
        Continuum.Runtime.Instance.new(name: name, repo: opts[:repo])
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
        child(
          Continuum.Runtime.ActivityWorker.Supervisor,
          Keyword.get(opts, :activity_supervisor, []),
          instance
        ),
        child(Continuum.Runtime.Recovery, Keyword.get(opts, :recovery, []), instance),
        child(Continuum.Runtime.Dispatcher, Keyword.get(opts, :dispatcher, []), instance),
        child(
          Continuum.Runtime.ActivityWorker.Dispatcher,
          Keyword.get(opts, :activity_dispatcher, []),
          instance
        ),
        child(Continuum.Runtime.TimerWheel, Keyword.get(opts, :timer_wheel, []), instance),
        child(Continuum.Runtime.SignalRouter, Keyword.get(opts, :signal_router, []), instance)
      ]
      |> Enum.reject(&is_nil/1)
    end
  end

  @doc """
  Start a new workflow run.

  Options include `:instance` for selecting a named Continuum instance and
  `:trace_context` for persisting an opaque W3C traceparent binary that
  observability integrations can use to link resumed run attempts.
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
  """
  @spec side_effect((-> term())) :: term()
  def side_effect(fun) when is_function(fun, 0) do
    Effect.run({:side_effect, :user}, fun)
  end

  @doc """
  Whether we are currently executing inside a workflow process. Useful
  in helper modules that branch on context.
  """
  @spec in_workflow?() :: boolean()
  def in_workflow? do
    Context.active?()
  end

  @doc """
  Compatibility marker for future workflow patches.

  v0.1 ships this as a deterministic stub returning `false`. Real patch
  markers are planned for a later release; until then, use explicit workflow
  versions for incompatible changes.
  """
  @spec patched?(atom() | binary()) :: false
  def patched?(_patch_name), do: false

  # ---------------------------------------------------------------------------

  defp child(_module, false, _instance), do: nil
  defp child(module, true, instance), do: child(module, [], instance)

  defp child(module, opts, instance) do
    opts = opts |> List.wrap() |> Keyword.put(:instance, instance)
    Supervisor.child_spec({module, opts}, id: {module, instance.name})
  end

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

  defp hash_term(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
