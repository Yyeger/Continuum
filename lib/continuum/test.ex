defmodule Continuum.Test do
  @moduledoc """
  Public helpers for testing Continuum workflows.

  The helpers in this module are deliberately small and stable. They cover the
  v0.1 testing loop:

    * run workflows against the in-memory journal
    * load or persist event histories
    * replay a committed golden history
    * inject signals and timers in deterministic tests
    * check out an Ecto SQL Sandbox connection for Postgres-backed tests

  The in-memory journal is process-local and not durable. Use unique run IDs
  or call `reset_in_memory!/0` between tests that need a clean journal.
  """

  import Ecto.Query
  import ExUnit.Assertions

  alias Continuum.Runtime.{Context, Engine, Instance, Journal}
  alias Continuum.Schema.{Run, Timer}

  @type replay_result ::
          {:ok, term()}
          | {:suspended, term()}
          | {:continued, binary()}
          | {:error, term()}

  @doc """
  Start a workflow run synchronously against the in-memory journal.
  """
  @spec start_synchronous(module(), term(), keyword()) :: {:ok, binary()} | {:error, term()}
  def start_synchronous(workflow_module, input, opts \\ []) do
    opts = Keyword.put(opts, :journal, Journal.InMemory)
    Continuum.start(workflow_module, input, opts)
  end

  @doc """
  Start a workflow run against the Postgres journal.
  """
  @spec start_postgres(module(), term(), keyword()) :: {:ok, binary()} | {:error, term()}
  def start_postgres(workflow_module, input, opts \\ []) do
    opts = Keyword.put(opts, :journal, Journal.Postgres)
    Continuum.start(workflow_module, input, opts)
  end

  @doc """
  Reset the in-memory journal.
  """
  @spec reset_in_memory!() :: :ok
  def reset_in_memory! do
    Journal.InMemory.reset()
  end

  @doc """
  Load a run's event history from a journal.
  """
  @spec history(binary(), keyword()) :: [map()]
  def history(run_id, opts \\ []) do
    journal = Keyword.get(opts, :journal, Journal.InMemory)
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    journal.load(instance, run_id)
  end

  @doc """
  Persist a run history to `path` as an Erlang external term.

  The resulting file is intended for golden-history tests committed to the
  repository.
  """
  @spec dump_history!(binary(), Path.t(), keyword()) :: :ok
  def dump_history!(run_id, path, opts \\ []) do
    path
    |> Path.dirname()
    |> File.mkdir_p!()

    File.write!(path, :erlang.term_to_binary(history(run_id, opts)))
  end

  @doc """
  Load a history previously written by `dump_history!/3`.
  """
  @spec load_history!(Path.t()) :: [map()]
  def load_history!(path) do
    path
    |> File.read!()
    |> :erlang.binary_to_term()
  end

  @doc """
  Replay a workflow from an existing history.

  Returns `{:ok, result}` when the workflow completes from history, or
  `{:suspended, reason}` if the history ends at a pending effect.
  """
  @spec replay(module(), term(), [map()], keyword()) :: replay_result()
  def replay(workflow_module, input, history, opts \\ []) do
    workflow_module
    |> do_replay(input, history, opts)
    |> maybe_replay_generated_entrypoint(workflow_module, input, history, opts)
  end

  defp do_replay(workflow_module, input, history, opts) do
    run_id = Keyword.get(opts, :run_id, "continuum-replay")
    journal = Keyword.get(opts, :journal, Journal.InMemory)
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    snapshot = compatible_snapshot(Keyword.get(opts, :snapshot), workflow_module)

    ctx = %Context{
      run_id: run_id,
      history: history,
      history_offset: history_offset(snapshot),
      snapshot_steps: snapshot_steps(snapshot),
      cursor: 0,
      workflow_module: workflow_module,
      lease_token: Keyword.get(opts, :lease_token),
      instance: instance,
      journal: journal
    }

    Context.put(ctx)

    try do
      result = workflow_module.run(input)
      assert_all_history_consumed!(history)
      {:ok, result}
    catch
      {token, reason} when token == :continuum_suspend ->
        {:suspended, reason}

      {token, next_run_id} when token == :continuum_continued_as_new ->
        {:continued, next_run_id}

      kind, reason ->
        {:error, {kind, reason, __STACKTRACE__}}
    after
      Context.clear()
    end
  end

  defp maybe_replay_generated_entrypoint(
         {:error, {:error, %Continuum.ReplayDriftError{expected: expected}, _stacktrace}} =
           result,
         workflow_module,
         input,
         history,
         opts
       ) do
    entrypoint = expected_entrypoint(expected)

    if entrypoint && entrypoint != workflow_module do
      do_replay(entrypoint, input, history, opts)
    else
      result
    end
  end

  defp maybe_replay_generated_entrypoint(result, _workflow_module, _input, _history, _opts) do
    result
  end

  defp expected_entrypoint(expected) when is_map(expected) do
    expected
    |> Map.get(:command_id, Map.get(expected, "command_id"))
    |> command_entrypoint()
  end

  defp expected_entrypoint(_expected), do: nil

  defp command_entrypoint(module) when is_atom(module) do
    if generated_workflow_entrypoint?(module) and function_exported?(module, :run, 1) do
      module
    end
  end

  defp command_entrypoint(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.find_value(&command_entrypoint/1)
  end

  defp command_entrypoint(list) when is_list(list),
    do: Enum.find_value(list, &command_entrypoint/1)

  defp command_entrypoint(_other), do: nil

  defp generated_workflow_entrypoint?(module) do
    metadata = module.__continuum_workflow__()
    metadata.entrypoint == module and Map.get(metadata, :source_module, module) != module
  rescue
    UndefinedFunctionError -> false
  end

  @doc """
  Assert that a workflow replays from history to `expected`.
  """
  @spec assert_replays(module(), term(), [map()], term()) :: term()
  def assert_replays(workflow_module, input, history, expected) do
    assert {:ok, ^expected} = replay(workflow_module, input, history)
    expected
  end

  @doc """
  Assert that a workflow replays from history without drift.

  Returns the replayed result.
  """
  @spec assert_replays(module(), term(), [map()]) :: term()
  def assert_replays(workflow_module, input, history) do
    assert {:ok, result} = replay(workflow_module, input, history)
    result
  end

  @doc """
  Inject a signal into a run and wake its local engine when one exists.

  Delivery goes through the same `Continuum.Runtime.SignalRouter` path as
  `Continuum.signal/4`: in-memory signals are buffered in the run's mailbox
  and consumed by the matching `await signal`, journaling `signal_received`
  with the await's command identity — injected signals exercise the same
  command-identity drift detection as production deliveries.
  """
  @spec inject_signal(binary(), atom(), term(), keyword()) :: :ok | {:error, term()}
  def inject_signal(run_id, name, payload, opts \\ []) do
    journal = Keyword.get(opts, :journal, Journal.InMemory)

    case journal do
      adapter when adapter in [Journal.Postgres, Journal.InMemory] ->
        Continuum.Runtime.SignalRouter.deliver(
          run_id,
          name,
          payload,
          Keyword.put(opts, :journal, adapter)
        )

      other ->
        {:error, {:unsupported_signal_injection_journal, other}}
    end
  end

  @doc """
  Inject a fired timer event for the latest pending timer in a run's history.
  """
  @spec fire_timer(binary(), keyword()) :: :ok | {:error, term()}
  def fire_timer(run_id, opts \\ []) do
    journal = Keyword.get(opts, :journal, Journal.InMemory)
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case journal do
      Journal.Postgres -> fire_postgres_timer(run_id, opts)
      _ -> fire_journal_timer(instance, run_id, journal)
    end
  end

  @doc """
  Check out an Ecto SQL Sandbox connection.

  Pass `shared: true` when workflow engines or workers need to use the test
  process' checked-out connection.
  """
  @spec checkout_sandbox(module() | nil, keyword()) :: :ok
  def checkout_sandbox(repo \\ Application.get_env(:continuum, :repo), opts \\ []) do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(repo)

    if Keyword.get(opts, :shared, false) do
      Ecto.Adapters.SQL.Sandbox.mode(repo, {:shared, self()})
    end

    :ok
  end

  defp assert_all_history_consumed!(history) do
    ctx = Context.get()
    consumed = ctx.cursor
    expected = (ctx.history_offset || 0) + length(history)

    assert consumed == expected,
           "replay consumed #{consumed} events but history covers through cursor #{expected}"
  end

  defp compatible_snapshot(nil, _workflow_module), do: nil

  defp compatible_snapshot(
         %Continuum.Snapshot{version_hash: version_hash} = snapshot,
         workflow_module
       ) do
    if version_hash == workflow_version_hash(workflow_module), do: snapshot, else: nil
  end

  defp workflow_version_hash(workflow_module) do
    workflow_module.__continuum_workflow__().version_hash
  rescue
    UndefinedFunctionError -> <<0::256>>
  end

  defp history_offset(nil), do: 0
  defp history_offset(%Continuum.Snapshot{through_seq: through_seq}), do: through_seq + 1

  defp snapshot_steps(nil), do: %{}
  defp snapshot_steps(%Continuum.Snapshot{steps_by_seq: steps}), do: steps || %{}

  defp fire_journal_timer(instance, run_id, journal) do
    with {:ok, timer_event} <- latest_pending_timer(journal.load(instance, run_id)) do
      :ok =
        journal.append!(
          instance,
          run_id,
          %{
            type: :timer_fired,
            timer_id: Map.fetch!(timer_event, :timer_id),
            command_id: Map.get(timer_event, :command_id),
            seq: nil
          },
          nil
        )

      Engine.wake(instance, run_id)
      :ok
    end
  end

  defp fire_postgres_timer(run_id, opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    timer_id = Keyword.get(opts, :timer_id)

    timer =
      instance.repo.one(
        from(t in Timer,
          where: t.run_id == ^run_id and t.fired == false,
          where: is_nil(^timer_id) or t.id == ^timer_id,
          order_by: [desc: t.fires_at],
          limit: 1
        )
      )

    case timer do
      nil ->
        {:error, :no_pending_timer}

      %Timer{} = timer ->
        lease_token =
          instance.repo.one(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

        :ok = Journal.Postgres.fire_timer!(instance, run_id, timer.id, lease_token)
        Engine.wake(instance, run_id)
        :ok
    end
  end

  defp latest_pending_timer(history) do
    fired =
      history
      |> Enum.filter(&(&1.type == :timer_fired))
      |> MapSet.new(&Map.get(&1, :timer_id))

    history
    |> Enum.reverse()
    |> Enum.find(fn event ->
      event.type == :timer_started and not MapSet.member?(fired, Map.get(event, :timer_id))
    end)
    |> case do
      nil -> {:error, :no_pending_timer}
      event -> {:ok, event}
    end
  end
end
