defmodule Continuum.Runtime.Engine do
  @moduledoc """
  GenServer-per-run. The heart of replay.

  Each run is owned by exactly one Engine GenServer process; the process is
  started by `start_run/3` under `Continuum.Runtime.RunSupervisor`.

  The same replay loop runs against both the in-memory journal and the
  Postgres journal. Postgres durability, scheduling, and fencing are provided
  by the journal adapter and runtime pollers around this engine.
  """

  use GenServer
  require Logger

  alias Continuum.{Runtime.Context, Runtime.Instance, Runtime.Lease, Telemetry}

  defstruct [
    :run_id,
    :workflow_module,
    :input,
    :instance,
    :journal,
    :lease_owner,
    :lease_token,
    :trace_context,
    :status,
    :result,
    :error
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Start a fresh workflow run.
  """
  def start_run(workflow_module, input, opts \\ []) do
    run_id = Keyword.get(opts, :run_id, Continuum.Runtime.IdGen.run_id())

    start_child(workflow_module, input, run_id, opts)
  end

  @doc false
  def resume_run(workflow_module, input, run_id, opts \\ []) do
    opts = Keyword.put(opts, :resume, true)
    start_child(workflow_module, input, run_id, opts)
  end

  defp start_child(workflow_module, input, run_id, opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    opts = Keyword.put(opts, :instance, instance)

    case DynamicSupervisor.start_child(
           instance.run_supervisor,
           {__MODULE__, {workflow_module, input, run_id, opts}}
         ) do
      {:ok, _pid} -> {:ok, run_id}
      {:error, _} = err -> err
    end
  end

  @doc false
  def start_link({workflow_module, input, run_id, opts}) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    GenServer.start_link(__MODULE__, {workflow_module, input, run_id, opts},
      name: via(instance, run_id)
    )
  end

  @doc false
  def child_spec({_workflow_module, _input, _run_id, _opts} = args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary,
      type: :worker
    }
  end

  @doc """
  Cancel a running workflow.
  """
  def cancel(run_id, opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case GenServer.whereis(via(instance, run_id)) do
      nil -> durable_cancel(instance, run_id, opts)
      pid -> GenServer.call(pid, :cancel)
    end
  end

  @doc """
  Block the caller until the run completes (or `timeout` ms elapses).

  Polls the journal at 5 ms intervals. Source of truth is the journal —
  works even after the engine process has exited.

  Accepts `journal:` in opts to override which journal adapter to poll.
  """
  def await(run_id, timeout, opts \\ []) do
    deadline = System.monotonic_time(:millisecond) + timeout
    journal = Keyword.get(opts, :journal, Continuum.Runtime.Journal.default())
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case subscribe_run(instance, run_id) do
      :ok ->
        try do
          case poll_once(instance, run_id, journal) do
            :pending -> await_run_finished(instance, run_id, deadline, journal)
            result -> result
          end
        after
          unsubscribe_run(instance, run_id)
        end

      :error ->
        poll_until(instance, run_id, deadline, journal)
    end
  end

  defp poll_until(instance, run_id, deadline, journal) do
    case poll_once(instance, run_id, journal) do
      :pending -> poll_pending(instance, run_id, deadline, journal)
      result -> result
    end
  end

  defp poll_once(instance, run_id, journal) do
    case journal.get_run(instance, run_id) do
      nil ->
        {:error, :not_found}

      %{state: :completed, result: result} ->
        {:ok, %{run_id: run_id, state: :completed, result: result}}

      %{state: :failed, error: err} ->
        {:error, %{run_id: run_id, state: :failed, error: err}}

      %{state: :cancelled} ->
        {:error, %{run_id: run_id, state: :cancelled}}

      %{state: :running} ->
        :pending

      %{state: :suspended} ->
        :pending
    end
  end

  defp await_run_finished(instance, run_id, deadline, journal) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:run_finished, ^run_id, state, payload} ->
        await_result(run_id, state, payload)
    after
      timeout ->
        poll_until(instance, run_id, deadline, journal)
    end
  end

  defp poll_pending(instance, run_id, deadline, journal) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      Process.sleep(5)
      poll_until(instance, run_id, deadline, journal)
    end
  end

  defp await_result(run_id, :completed, result) do
    {:ok, %{run_id: run_id, state: :completed, result: result}}
  end

  defp await_result(run_id, :failed, error) do
    {:error, %{run_id: run_id, state: :failed, error: error}}
  end

  defp await_result(run_id, :cancelled, payload) do
    {:error, %{run_id: run_id, state: :cancelled, error: payload}}
  end

  defp run_topic(run_id), do: "continuum:run:#{run_id}"

  defp subscribe_run(instance, run_id) do
    if Process.whereis(instance.pubsub) do
      Phoenix.PubSub.subscribe(instance.pubsub, run_topic(run_id))
    else
      :error
    end
  end

  defp unsubscribe_run(instance, run_id) do
    if Process.whereis(instance.pubsub) do
      Phoenix.PubSub.unsubscribe(instance.pubsub, run_topic(run_id))
    end
  end

  def broadcast_run_finished(instance, run_id, state, payload) do
    if Process.whereis(instance.pubsub) do
      Phoenix.PubSub.broadcast(
        instance.pubsub,
        run_topic(run_id),
        {:run_finished, run_id, state, payload}
      )
    end

    :ok
  end

  @doc false
  def wake(run_id), do: wake(Instance.default(), run_id)

  def wake(instance, run_id) do
    case GenServer.whereis(via(instance, run_id)) do
      nil -> {:error, :not_found}
      pid -> GenServer.cast(pid, :wake)
    end
  end

  defp via(instance, run_id), do: {:via, Registry, {instance.registry, run_id}}

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init({workflow_module, input, run_id, opts}) do
    journal = Keyword.get(opts, :journal, Continuum.Runtime.Journal.InMemory)
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    trace_context = initial_trace_context(opts)

    unless Keyword.get(opts, :resume, false) do
      :ok = start_run(journal, instance, run_id, workflow_module, input, trace_context)
    end

    {lease_owner, lease_token} = acquire_lease(instance, journal, run_id, opts)
    trace_context = resume_trace_context(journal, instance, run_id, trace_context, opts)

    state = %__MODULE__{
      run_id: run_id,
      workflow_module: workflow_module,
      input: input,
      instance: instance,
      journal: journal,
      lease_owner: lease_owner,
      lease_token: lease_token,
      trace_context: trace_context,
      status: :running,
      result: nil,
      error: nil
    }

    Telemetry.execute(
      [:continuum, :run, :started],
      %{},
      run_metadata(state, %{
        resumed?: Keyword.get(opts, :resume, false),
        lease_owner: lease_owner
      })
    )

    {:ok, state, {:continue, :run}}
  end

  @impl true
  def handle_continue(:run, state) do
    state |> attempt_run() |> finalize()
  end

  @impl true
  def handle_cast(:wake, state) do
    {:noreply, state, {:continue, :run}}
  end

  @impl true
  def handle_call(:cancel, _from, state) do
    :ok = cancel_run(state)
    state = %{state | status: :cancelled, error: :cancelled}

    Telemetry.execute([:continuum, :run, :cancelled], %{}, run_metadata(state))

    :ok = broadcast_run_finished(state.instance, state.run_id, :cancelled, :cancelled)
    untrack_lease(state)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_info(
        {:continuum_lease_lost, run_id, token},
        %{run_id: run_id, lease_token: token} = state
      ) do
    Logger.warning("Workflow #{run_id} lost its Postgres lease; stopping stale engine")
    {:stop, :normal, state}
  end

  def handle_info({:continuum_lease_lost, _run_id, _token}, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Replay loop
  # ---------------------------------------------------------------------------

  defp attempt_run(state) do
    history = state.journal.load(state.instance, state.run_id)

    ctx = %Context{
      run_id: state.run_id,
      history: history,
      cursor: 0,
      workflow_module: state.workflow_module,
      lease_token: state.lease_token,
      trace_context: state.trace_context,
      instance: state.instance,
      journal: state.journal
    }

    Context.put(ctx)

    try do
      result = state.workflow_module.run(state.input)
      complete_run(state, result)
    catch
      {:continuum_suspend, reason} ->
        suspend_run(state, reason)

      kind, reason ->
        stacktrace = __STACKTRACE__

        if lease_lost?(kind, reason) do
          lease_lost(state)
        else
          fail_run(state, {kind, reason, stacktrace}, {kind, reason})
        end
    after
      Context.clear()
    end
  end

  defp complete_run(state, result) do
    :ok = state.journal.complete!(state.instance, state.run_id, result, state.lease_token)
    :ok = broadcast_run_finished(state.instance, state.run_id, :completed, result)

    Telemetry.execute([:continuum, :run, :completed], %{}, run_metadata(state))

    %{state | status: :completed, result: result}
  rescue
    error ->
      if lease_lost?(:error, error), do: lease_lost(state), else: reraise(error, __STACKTRACE__)
  end

  defp suspend_run(state, reason) do
    Logger.debug("Workflow #{state.run_id} suspended: #{inspect(reason)}")
    :ok = state.journal.suspend!(state.instance, state.run_id, state.lease_token)

    Telemetry.execute([:continuum, :run, :suspended], %{}, run_metadata(state, %{reason: reason}))

    %{state | status: :suspended}
  rescue
    error ->
      if lease_lost?(:error, error), do: lease_lost(state), else: reraise(error, __STACKTRACE__)
  end

  defp fail_run(state, journal_error, state_error) do
    :ok = state.journal.fail!(state.instance, state.run_id, journal_error, state.lease_token)
    :ok = broadcast_run_finished(state.instance, state.run_id, :failed, state_error)

    Telemetry.execute(
      [:continuum, :run, :failed],
      %{},
      run_metadata(state, %{error: state_error})
    )

    %{state | status: :failed, error: state_error}
  rescue
    error ->
      if lease_lost?(:error, error), do: lease_lost(state), else: reraise(error, __STACKTRACE__)
  end

  defp cancel_run(%{journal: Continuum.Runtime.Journal.Postgres} = state) do
    Continuum.Runtime.Journal.Postgres.cancel_run!(
      state.instance,
      state.run_id,
      state.lease_token
    )
  end

  defp cancel_run(state) do
    state.journal.fail!(state.instance, state.run_id, :cancelled, state.lease_token)
  end

  defp durable_cancel(%Instance{repo: nil}, _run_id, _opts), do: {:error, :not_found}

  defp durable_cancel(instance, run_id, opts) do
    journal = Keyword.get(opts, :journal, Continuum.Runtime.Journal.default())

    case journal do
      Continuum.Runtime.Journal.Postgres ->
        with {:ok, lease} <-
               Lease.acquire(run_id,
                 owner:
                   Keyword.get_lazy(opts, :lease_owner, fn -> Lease.owner(instance.name) end),
                 repo: instance.repo,
                 ttl_seconds: Keyword.get(opts, :lease_ttl_seconds, 30)
               ) do
          Continuum.Runtime.Journal.Postgres.cancel_run!(instance, run_id, lease.token)
        else
          {:error, :not_acquired} -> {:error, :not_found}
          {:error, reason} -> {:error, reason}
        end

      _other ->
        {:error, :not_found}
    end
  rescue
    error -> {:error, error}
  end

  defp finalize(%{status: :suspended} = state), do: {:noreply, state}

  defp finalize(%{status: :lease_lost} = state) do
    untrack_lease(state)
    {:stop, :normal, state}
  end

  defp finalize(state) do
    untrack_lease(state)
    {:stop, :normal, state}
  end

  defp acquire_lease(instance, Continuum.Runtime.Journal.Postgres, run_id, opts) do
    case {Keyword.get(opts, :lease_owner), Keyword.get(opts, :lease_token)} do
      {owner, token} when is_binary(owner) and is_integer(token) ->
        track_lease(instance, %Lease{run_id: run_id, owner: owner, token: token})
        {owner, token}

      _ ->
        lease =
          Lease.acquire!(run_id,
            owner: Keyword.get_lazy(opts, :lease_owner, fn -> Lease.owner(instance.name) end),
            repo: instance.repo,
            ttl_seconds: Keyword.get(opts, :lease_ttl_seconds, 30)
          )

        track_lease(instance, lease)
        {lease.owner, lease.token}
    end
  end

  defp acquire_lease(_instance, _journal, _run_id, _opts), do: {nil, nil}

  defp track_lease(instance, %Lease{} = lease) do
    if Process.whereis(instance.heartbeater) do
      Continuum.Runtime.Lease.Heartbeater.track(instance, lease, self())
    end
  end

  defp untrack_lease(%{instance: instance, run_id: run_id, lease_token: token})
       when is_integer(token) do
    if Process.whereis(instance.heartbeater) do
      Continuum.Runtime.Lease.Heartbeater.untrack(instance, run_id)
    end
  end

  defp untrack_lease(_state), do: :ok

  defp lease_lost(state) do
    Logger.warning("Workflow #{state.run_id} lost its Postgres lease; stopping stale engine")

    Telemetry.execute([:continuum, :run, :lease_lost], %{}, run_metadata(state))

    %{state | status: :lease_lost}
  end

  defp start_run(journal, instance, run_id, workflow_module, input, trace_context) do
    if function_exported?(journal, :start_run, 5) do
      journal.start_run(instance, run_id, workflow_module, input, trace_context: trace_context)
    else
      journal.start_run(instance, run_id, workflow_module, input)
    end
  end

  defp initial_trace_context(opts) do
    case Keyword.fetch(opts, :trace_context) do
      {:ok, trace_context} -> normalize_trace_context(trace_context)
      :error -> current_trace_context()
    end
  end

  defp resume_trace_context(journal, instance, run_id, trace_context, opts) do
    if Keyword.get(opts, :resume, false) and is_nil(trace_context) and
         function_exported?(journal, :get_run, 2) do
      case journal.get_run(instance, run_id) do
        %{trace_context: loaded_trace_context} -> loaded_trace_context
        _ -> nil
      end
    else
      trace_context
    end
  end

  defp normalize_trace_context(nil), do: nil
  defp normalize_trace_context(trace_context) when is_binary(trace_context), do: trace_context

  defp normalize_trace_context(other) do
    raise ArgumentError, "expected :trace_context to be a binary or nil, got: #{inspect(other)}"
  end

  defp current_trace_context do
    Process.get(:continuum_trace_context) || otel_traceparent()
  end

  defp otel_traceparent do
    if Code.ensure_loaded?(:otel_propagator_text_map) and
         function_exported?(:otel_propagator_text_map, :inject, 1) do
      :otel_propagator_text_map
      |> apply(:inject, [%{}])
      |> extract_traceparent()
    end
  rescue
    _ -> nil
  end

  defp extract_traceparent(%{} = carrier) do
    carrier["traceparent"] || carrier[:traceparent]
  end

  defp extract_traceparent(carrier) when is_list(carrier) do
    Enum.find_value(carrier, fn
      {"traceparent", value} -> value
      {:traceparent, value} -> value
      _ -> nil
    end)
  end

  defp extract_traceparent(_carrier), do: nil

  defp run_metadata(state, extra \\ %{}) do
    %{
      instance: state.instance.name,
      run_id: state.run_id,
      workflow: state.workflow_module,
      lease_token: state.lease_token,
      trace_context: state.trace_context
    }
    |> Map.merge(extra)
  end

  defp lease_lost?(:error, %RuntimeError{message: message}) do
    String.contains?(message, "lease token mismatch") or
      String.contains?(message, "lease_mismatch")
  end

  defp lease_lost?(_kind, _reason), do: false
end
