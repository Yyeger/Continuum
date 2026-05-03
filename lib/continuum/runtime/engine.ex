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

  alias Continuum.{Runtime.Context, Runtime.Lease, Telemetry}

  defstruct [
    :run_id,
    :workflow_module,
    :input,
    :journal,
    :lease_owner,
    :lease_token,
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
    case DynamicSupervisor.start_child(
           Continuum.Runtime.RunSupervisor,
           {__MODULE__, {workflow_module, input, run_id, opts}}
         ) do
      {:ok, _pid} -> {:ok, run_id}
      {:error, _} = err -> err
    end
  end

  @doc false
  def start_link({workflow_module, input, run_id, opts}) do
    GenServer.start_link(__MODULE__, {workflow_module, input, run_id, opts}, name: via(run_id))
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
  def cancel(run_id, _opts \\ []) do
    case GenServer.whereis(via(run_id)) do
      nil -> {:error, :not_found}
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

    case subscribe_run(run_id) do
      :ok ->
        try do
          case poll_once(run_id, journal) do
            :pending -> await_run_finished(run_id, deadline, journal)
            result -> result
          end
        after
          unsubscribe_run(run_id)
        end

      :error ->
        poll_until(run_id, deadline, journal)
    end
  end

  defp poll_until(run_id, deadline, journal) do
    case poll_once(run_id, journal) do
      :pending -> poll_pending(run_id, deadline, journal)
      result -> result
    end
  end

  defp poll_once(run_id, journal) do
    case journal.get_run(run_id) do
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

  defp await_run_finished(run_id, deadline, journal) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:run_finished, ^run_id, state, payload} ->
        await_result(run_id, state, payload)
    after
      timeout ->
        poll_until(run_id, deadline, journal)
    end
  end

  defp poll_pending(run_id, deadline, journal) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      Process.sleep(5)
      poll_until(run_id, deadline, journal)
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

  defp subscribe_run(run_id) do
    if Process.whereis(Continuum.PubSub) do
      Phoenix.PubSub.subscribe(Continuum.PubSub, run_topic(run_id))
    else
      :error
    end
  end

  defp unsubscribe_run(run_id) do
    if Process.whereis(Continuum.PubSub) do
      Phoenix.PubSub.unsubscribe(Continuum.PubSub, run_topic(run_id))
    end
  end

  def broadcast_run_finished(run_id, state, payload) do
    if Process.whereis(Continuum.PubSub) do
      Phoenix.PubSub.broadcast(
        Continuum.PubSub,
        run_topic(run_id),
        {:run_finished, run_id, state, payload}
      )
    end

    :ok
  end

  @doc false
  def wake(run_id) do
    case GenServer.whereis(via(run_id)) do
      nil -> {:error, :not_found}
      pid -> GenServer.cast(pid, :wake)
    end
  end

  defp via(run_id), do: {:via, Registry, {Continuum.Runtime.Registry, run_id}}

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init({workflow_module, input, run_id, opts}) do
    journal = Keyword.get(opts, :journal, Continuum.Runtime.Journal.InMemory)

    unless Keyword.get(opts, :resume, false) do
      :ok = journal.start_run(run_id, workflow_module, input)
    end

    {lease_owner, lease_token} = acquire_lease(journal, run_id, opts)

    state = %__MODULE__{
      run_id: run_id,
      workflow_module: workflow_module,
      input: input,
      journal: journal,
      lease_owner: lease_owner,
      lease_token: lease_token,
      status: :running,
      result: nil,
      error: nil
    }

    Telemetry.execute([:continuum, :run, :started], %{}, %{
      run_id: run_id,
      workflow: workflow_module,
      resumed?: Keyword.get(opts, :resume, false),
      lease_owner: lease_owner,
      lease_token: lease_token
    })

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

    Telemetry.execute([:continuum, :run, :cancelled], %{}, %{
      run_id: state.run_id,
      workflow: state.workflow_module,
      lease_token: state.lease_token
    })

    :ok = broadcast_run_finished(state.run_id, :cancelled, :cancelled)
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
    history = state.journal.load(state.run_id)

    ctx = %Context{
      run_id: state.run_id,
      history: history,
      cursor: 0,
      workflow_module: state.workflow_module,
      lease_token: state.lease_token,
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
    :ok = state.journal.complete!(state.run_id, result, state.lease_token)
    :ok = broadcast_run_finished(state.run_id, :completed, result)

    Telemetry.execute([:continuum, :run, :completed], %{}, %{
      run_id: state.run_id,
      workflow: state.workflow_module,
      lease_token: state.lease_token
    })

    %{state | status: :completed, result: result}
  rescue
    error ->
      if lease_lost?(:error, error), do: lease_lost(state), else: reraise(error, __STACKTRACE__)
  end

  defp suspend_run(state, reason) do
    Logger.debug("Workflow #{state.run_id} suspended: #{inspect(reason)}")
    :ok = state.journal.suspend!(state.run_id, state.lease_token)

    Telemetry.execute([:continuum, :run, :suspended], %{}, %{
      run_id: state.run_id,
      workflow: state.workflow_module,
      reason: reason,
      lease_token: state.lease_token
    })

    %{state | status: :suspended}
  rescue
    error ->
      if lease_lost?(:error, error), do: lease_lost(state), else: reraise(error, __STACKTRACE__)
  end

  defp fail_run(state, journal_error, state_error) do
    :ok = state.journal.fail!(state.run_id, journal_error, state.lease_token)
    :ok = broadcast_run_finished(state.run_id, :failed, state_error)

    Telemetry.execute([:continuum, :run, :failed], %{}, %{
      run_id: state.run_id,
      workflow: state.workflow_module,
      error: state_error,
      lease_token: state.lease_token
    })

    %{state | status: :failed, error: state_error}
  rescue
    error ->
      if lease_lost?(:error, error), do: lease_lost(state), else: reraise(error, __STACKTRACE__)
  end

  defp cancel_run(%{journal: Continuum.Runtime.Journal.Postgres} = state) do
    Continuum.Runtime.Journal.Postgres.cancel_run!(state.run_id, state.lease_token)
  end

  defp cancel_run(state) do
    state.journal.fail!(state.run_id, :cancelled, state.lease_token)
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

  defp acquire_lease(Continuum.Runtime.Journal.Postgres, run_id, opts) do
    case {Keyword.get(opts, :lease_owner), Keyword.get(opts, :lease_token)} do
      {owner, token} when is_binary(owner) and is_integer(token) ->
        track_lease(%Lease{run_id: run_id, owner: owner, token: token})
        {owner, token}

      _ ->
        lease =
          Lease.acquire!(run_id,
            owner: Keyword.get_lazy(opts, :lease_owner, &Lease.owner/0),
            ttl_seconds: Keyword.get(opts, :lease_ttl_seconds, 30)
          )

        track_lease(lease)
        {lease.owner, lease.token}
    end
  end

  defp acquire_lease(_journal, _run_id, _opts), do: {nil, nil}

  defp track_lease(%Lease{} = lease) do
    if Process.whereis(Continuum.Runtime.Lease.Heartbeater) do
      Continuum.Runtime.Lease.Heartbeater.track(lease, self())
    end
  end

  defp untrack_lease(%{run_id: run_id, lease_token: token}) when is_integer(token) do
    if Process.whereis(Continuum.Runtime.Lease.Heartbeater) do
      Continuum.Runtime.Lease.Heartbeater.untrack(run_id)
    end
  end

  defp untrack_lease(_state), do: :ok

  defp lease_lost(state) do
    Logger.warning("Workflow #{state.run_id} lost its Postgres lease; stopping stale engine")

    Telemetry.execute([:continuum, :run, :lease_lost], %{}, %{
      run_id: state.run_id,
      workflow: state.workflow_module,
      lease_token: state.lease_token
    })

    %{state | status: :lease_lost}
  end

  defp lease_lost?(:error, %RuntimeError{message: message}) do
    String.contains?(message, "lease token mismatch") or
      String.contains?(message, "lease_mismatch")
  end

  defp lease_lost?(_kind, _reason), do: false
end
