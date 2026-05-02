defmodule Continuum.Runtime.Engine do
  @moduledoc """
  GenServer-per-run. The heart of replay.

  v0.1 in-memory, single-node implementation. Each run is owned by exactly
  one Engine GenServer process; the process is started by `start_run/3`
  under `Continuum.Runtime.RunSupervisor`.

  The Postgres-backed engine (planned for v0.1 final) replaces the
  in-memory journal calls with transactional writes guarded by a lease
  token; the replay loop and effect protocol stay identical.
  """

  use GenServer
  require Logger

  alias Continuum.Runtime.{Context, Lease}

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
      restart: :transient,
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
    poll_until(run_id, deadline, journal)
  end

  defp poll_until(run_id, deadline, journal) do
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
        poll_pending(run_id, deadline, journal)

      %{state: :suspended} ->
        poll_pending(run_id, deadline, journal)
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

  @doc """
  Deliver a signal to a running workflow's mailbox. Used by the
  signal router.
  """
  def deliver_signal(run_id, name, payload) do
    case GenServer.whereis(via(run_id)) do
      nil -> {:error, :not_found}
      pid -> GenServer.cast(pid, {:signal, name, payload})
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

    {:ok, state, {:continue, :run}}
  end

  @impl true
  def handle_continue(:run, state) do
    state |> attempt_run() |> finalize()
  end

  @impl true
  def handle_cast({:signal, name, payload}, state) do
    # In v0.1 in-memory, signals immediately journal a `signal_received`
    # event and resume the workflow.
    event = %{
      type: :signal_received,
      name: name,
      payload: payload,
      seq: nil
    }

    :ok = state.journal.append!(state.run_id, event, state.lease_token)
    {:noreply, state, {:continue, :run}}
  end

  @impl true
  def handle_call(:cancel, _from, state) do
    :ok = state.journal.fail!(state.run_id, :cancelled, state.lease_token)
    state = %{state | status: :cancelled, error: :cancelled}
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
      :ok = state.journal.complete!(state.run_id, result, state.lease_token)
      %{state | status: :completed, result: result}
    catch
      {:continuum_suspend, reason} ->
        Logger.debug("Workflow #{state.run_id} suspended: #{inspect(reason)}")
        :ok = state.journal.suspend!(state.run_id, state.lease_token)
        %{state | status: :suspended}

      kind, reason ->
        stacktrace = __STACKTRACE__
        :ok = state.journal.fail!(state.run_id, {kind, reason, stacktrace}, state.lease_token)
        %{state | status: :failed, error: {kind, reason}}
    after
      Context.clear()
    end
  end

  defp finalize(%{status: :suspended} = state), do: {:noreply, state}

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
end
