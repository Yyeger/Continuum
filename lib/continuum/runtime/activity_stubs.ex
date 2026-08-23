defmodule Continuum.Runtime.ActivityStubs do
  @moduledoc """
  Activity stubs for in-memory test runs.

  The user-facing surface is the `:activities` option on
  `Continuum.Test.start_synchronous/3`; this module validates it and resolves a
  stub at the point the in-memory journal would otherwise invoke the real
  activity body.

  Stubs live in the `Continuum.Runtime.Context`, not in a registry keyed on the
  calling process: the workflow body runs inside the Engine GenServer, not in
  the test process, so anything stored in the test process is invisible where it
  is needed.

  A stub cannot influence a `command_id`. Command identity is computed at macro
  expansion from the call site and assigned before the effect is dispatched, so
  a stubbed run journals byte-identical command identity to a real one — which
  is what makes a stubbed unit test's history meaningful.
  """
  @moduledoc since: "0.8.0"

  alias Continuum.{ActivityStubError, DurableTerm}

  @type key :: {module(), atom()} | {module(), atom(), arity()}
  @type t :: %{optional(key()) => term()}

  @doc """
  Validate and normalize the `:activities` option.

  Refuses stubs on the Postgres journal: a durable activity runs in an activity
  worker process, out of a claimed task row, with retry, timeout, and
  idempotency semantics of its own. A stub cannot reach that path, and silently
  ignoring one would make a "passing" durable test meaningless.
  """
  @spec validate!(term(), module()) :: t()
  def validate!(stubs, _journal) when stubs == %{}, do: %{}

  def validate!(_stubs, Continuum.Runtime.Journal.Postgres) do
    raise ArgumentError,
          "activity stubs are only supported on the in-memory journal. Durable " <>
            "activities execute in an activity worker out of a claimed task row, " <>
            "which a stub cannot reach. Use Continuum.Test.start_synchronous/3, or " <>
            "drop :activities to exercise the real durable path"
  end

  def validate!(stubs, _journal) when is_map(stubs) do
    Map.new(stubs, fn {key, value} -> {normalize_key!(key), value} end)
  end

  def validate!(other, _journal) do
    raise ArgumentError,
          ":activities must be a map of {Module, :function} or {Module, :function, arity} " <>
            "to a stub, got: #{inspect(other)}"
  end

  @doc """
  Resolve a stub for an activity call, if one is registered.

  A more specific `{module, function, arity}` key wins over `{module, function}`.
  """
  @spec fetch(t(), module(), atom(), list()) :: {:ok, term()} | :error
  def fetch(stubs, _module, _function, _args) when stubs == %{}, do: :error

  def fetch(stubs, module, function, args) do
    case Map.fetch(stubs, {module, function, length(args)}) do
      {:ok, stub} -> {:ok, stub}
      :error -> Map.fetch(stubs, {module, function})
    end
  end

  @doc """
  Produce a stub's value for one call.

  A function of the activity's arity is invoked with the activity's arguments;
  any other value is returned as-is. The result is validated the way the
  Postgres adapter validates a real activity result — in-memory writes skip
  `DurableTerm.validate!`, so without this a stub returning a PID would pass the
  unit test and be rejected in production.
  """
  @spec invoke!(term(), list()) :: term()
  def invoke!(stub, args) when is_function(stub) do
    {:arity, arity} = :erlang.fun_info(stub, :arity)

    unless arity == length(args) do
      raise ActivityStubError,
        message:
          "activity stub takes #{arity} argument(s) but the activity was called " <>
            "with #{length(args)}: #{inspect(args)}"
    end

    stub |> apply(args) |> durable!()
  end

  def invoke!(stub, _args), do: durable!(stub)

  defp durable!(value) do
    case DurableTerm.validate(value, :activity_stub) do
      :ok ->
        value

      {:error, error} ->
        raise ActivityStubError,
          message:
            "activity stub returned a value the journal would reject: " <>
              Exception.message(error)
    end
  end

  defp normalize_key!({module, function} = key) when is_atom(module) and is_atom(function),
    do: key

  defp normalize_key!({module, function, arity} = key)
       when is_atom(module) and is_atom(function) and is_integer(arity) and arity >= 0,
       do: key

  defp normalize_key!(other) do
    raise ArgumentError,
          "invalid activity stub key #{inspect(other)}; expected {Module, :function} or " <>
            "{Module, :function, arity}"
  end
end
