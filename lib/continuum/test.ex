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

  alias Continuum.Runtime.{Context, Engine, Journal}
  alias Continuum.Schema.{Run, Timer}

  @type replay_result ::
          {:ok, term()}
          | {:suspended, term()}
          | {:error, term()}

  @doc """
  Start a workflow run against the in-memory journal.
  """
  @spec start_in_memory(module(), term(), keyword()) :: {:ok, binary()} | {:error, term()}
  def start_in_memory(workflow_module, input, opts \\ []) do
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
    journal.load(run_id)
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
    run_id = Keyword.get(opts, :run_id, "continuum-replay")
    journal = Keyword.get(opts, :journal, Journal.InMemory)

    ctx = %Context{
      run_id: run_id,
      history: history,
      cursor: 0,
      workflow_module: workflow_module,
      lease_token: Keyword.get(opts, :lease_token),
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

      kind, reason ->
        {:error, {kind, reason, __STACKTRACE__}}
    after
      Context.clear()
    end
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
  """
  @spec inject_signal(binary(), atom(), term(), keyword()) :: :ok | {:error, term()}
  def inject_signal(run_id, name, payload, opts \\ []) do
    journal = Keyword.get(opts, :journal, Journal.InMemory)

    case journal do
      Journal.Postgres ->
        :ok = Journal.Postgres.deliver_signal!(run_id, name, payload)
        Engine.wake(run_id)
        :ok

      _ ->
        case Engine.deliver_signal(run_id, name, payload) do
          :ok ->
            :ok

          {:error, :not_found} ->
            journal.append!(
              run_id,
              %{type: :signal_received, name: name, payload: payload, seq: nil},
              nil
            )

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Inject a fired timer event for the latest pending timer in a run's history.
  """
  @spec fire_timer(binary(), keyword()) :: :ok | {:error, term()}
  def fire_timer(run_id, opts \\ []) do
    journal = Keyword.get(opts, :journal, Journal.InMemory)

    case journal do
      Journal.Postgres -> fire_postgres_timer(run_id, opts)
      _ -> fire_journal_timer(run_id, journal)
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
    consumed = Context.get().cursor

    assert consumed == length(history),
           "replay consumed #{consumed} events but history has #{length(history)} events"
  end

  defp fire_journal_timer(run_id, journal) do
    with {:ok, timer_id} <- latest_pending_timer(journal.load(run_id)) do
      :ok = journal.append!(run_id, %{type: :timer_fired, timer_id: timer_id, seq: nil}, nil)
      Engine.wake(run_id)
      :ok
    end
  end

  defp fire_postgres_timer(run_id, opts) do
    timer_id = Keyword.get(opts, :timer_id)

    timer =
      repo().one(
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
        lease_token = repo().one(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

        :ok = Journal.Postgres.fire_timer!(run_id, timer.id, lease_token)
        Engine.wake(run_id)
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
      event -> {:ok, Map.fetch!(event, :timer_id)}
    end
  end

  defp repo do
    Application.fetch_env!(:continuum, :repo)
  end
end
