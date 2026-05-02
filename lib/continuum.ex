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

    * `start/3` — start a new workflow run
    * `signal/3` — deliver an external signal to a running workflow
    * `cancel/2` — cancel a running workflow
    * `await/2` — block until a workflow completes (test/synchronous use)
    * `now/0`, `uuid4/0`, `random/0`, `side_effect/1` — deterministic primitives
      callable from workflow code
  """

  alias Continuum.Runtime.{Context, Effect}

  @type run_id :: binary()
  @type workflow_module :: module()
  @type input :: term()

  @doc """
  Start a new workflow run.
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
    Continuum.Runtime.SignalRouter.deliver(run_id, name, payload)
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
  @spec await(run_id(), timeout()) :: {:ok, map()} | {:error, term()}
  def await(run_id, timeout \\ 5_000) do
    Continuum.Runtime.Engine.await(run_id, timeout)
  end

  # ---------------------------------------------------------------------------
  # Deterministic primitives — callable only from inside a workflow process.
  # Each consults the journal first; on first execution journals the value;
  # on replay returns the journaled value.
  # ---------------------------------------------------------------------------

  @doc """
  The current wall-clock time, journaled and replayed deterministically.
  """
  @spec now() :: DateTime.t()
  def now do
    Effect.run({:side_effect, :now}, fn -> DateTime.utc_now() end)
  end

  @doc """
  The current UTC date, journaled and replayed deterministically.
  """
  @spec today() :: Date.t()
  def today do
    Effect.run({:side_effect, :today}, fn -> Date.utc_today() end)
  end

  @doc """
  A v4 UUID, journaled and replayed deterministically.
  """
  @spec uuid4() :: binary()
  def uuid4 do
    Effect.run({:side_effect, :uuid4}, &generate_uuid4/0)
  end

  @doc """
  A pseudo-random float in [0, 1), journaled and replayed deterministically.
  """
  @spec random() :: float()
  def random do
    Effect.run({:side_effect, :random}, fn -> :rand.uniform_real() end)
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

  # ---------------------------------------------------------------------------

  defp generate_uuid4 do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)

    <<a::32, b::16, c::16, d::16, e::48>> =
      <<u0::48, 4::4, u1::12, 2::2, u2::62>>

    :io_lib.format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [a, b, c, d, e])
    |> IO.iodata_to_binary()
  end
end
