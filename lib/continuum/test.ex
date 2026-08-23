defmodule Continuum.Test do
  @moduledoc """
  Public helpers for testing Continuum workflows.

  The helpers in this module are deliberately small and stable. They cover the
  v0.1 testing loop:

    * run workflows against the in-memory journal
    * load or persist event histories
    * replay a committed golden history (through `Continuum.Replay`)
    * inject signals and timers in deterministic tests
    * check out an Ecto SQL Sandbox connection for Postgres-backed tests

  The in-memory journal is process-local and not durable. Use unique run IDs
  or call `reset_in_memory!/0` between tests that need a clean journal.
  """

  import Ecto.Query
  import ExUnit.Assertions

  alias Continuum.Runtime.{Engine, Instance, Journal}
  alias Continuum.Schema.{Run, Timer}

  @type replay_result ::
          {:ok, term()}
          | {:suspended, term()}
          | {:continued, binary()}
          | {:error, term()}

  @doc """
  Start a workflow run synchronously against the in-memory journal.

  Activities run inline in the engine process — no worker pool, no retry, no
  timeout. Child workflows run inline too, as their own in-memory engines.

  ## Stubbing activities

  Pass `:activities` to stand in for activity bodies, so a unit test can drive
  a workflow's branches without the activity's real dependencies:

      Continuum.Test.start_synchronous(Checkout, order,
        activities: %{
          {Payments, :charge} => fn _order -> {:ok, "ch_test"} end,
          {Shipping, :book} => {:error, :out_of_stock}
        }
      )

  Keys are `{Module, :function}`, or `{Module, :function, arity}` when one
  module exports the same activity name at several arities; the more specific
  key wins. A value that is a function of the activity's arity is called with
  the activity's arguments, and any other value is returned as-is.

  Stub returns are validated with `Continuum.DurableTerm`, because in-memory
  writes otherwise skip that check — a stub returning a PID would pass the unit
  test and be rejected in production.

  Stubs are refused on the Postgres journal: a durable activity runs in a
  worker process out of a claimed task row, which a stub cannot reach.
  They also cannot influence command identity, which is computed at macro
  expansion from the call site, so a stubbed run journals the same command ids
  as a real one.
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

  `:journal` accepts the shorthand `:postgres` or `:in_memory` as well as an
  adapter module, so a test never has to name a `Continuum.Runtime.*` module.
  Defaults to the in-memory journal.
  """
  @spec history(binary(), keyword()) :: [map()]
  def history(run_id, opts \\ []) do
    journal = resolve_journal(opts)
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
    |> Continuum.DurableTerm.decode!()
  end

  @doc """
  Replay a workflow from an existing history.

  Delegates to `Continuum.Replay.run/4`, which is read-only by default:
  stepping past the journaled tail reports `{:suspended, {:history_exhausted, _}}`
  instead of executing the next activity for real. Pass `journal:` explicitly to
  replay through a live adapter.

  Returns `{:ok, result}` when the workflow completes from history, or
  `{:suspended, reason}` if the history ends at a pending effect.
  """
  @spec replay(module(), term(), [map()], keyword()) :: replay_result()
  def replay(workflow_module, input, history, opts \\ []) do
    case Continuum.Replay.run(workflow_module, input, history, opts) do
      {:error, {:history_not_consumed, %{consumed: consumed, expected: expected}}} ->
        flunk("replay consumed #{consumed} events but history covers through cursor #{expected}")

      result ->
        result
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

  Delivery goes through the same `Continuum.Runtime.SignalRouter` path as
  `Continuum.signal/4`: in-memory signals are buffered in the run's mailbox
  and consumed by the matching `await signal`, journaling `signal_received`
  with the await's command identity — injected signals exercise the same
  command-identity drift detection as production deliveries.
  """
  @spec inject_signal(binary(), atom(), term(), keyword()) :: :ok | {:error, term()}
  def inject_signal(run_id, name, payload, opts \\ []) do
    journal = resolve_journal(opts)
    opts = Keyword.put(opts, :journal, journal)

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
    journal = resolve_journal(opts)
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case journal do
      Journal.Postgres -> fire_postgres_timer(run_id, opts)
      _ -> fire_journal_timer(instance, run_id, journal)
    end
  end

  # ---------------------------------------------------------------------------
  # Durable test driver
  # ---------------------------------------------------------------------------

  @doc """
  Drive the durable runtime until `run_id` reaches a terminal state.

  Test suites normally start Continuum with its pollers disabled so nothing
  moves behind a test's back. This turns the crank instead: on each tick it
  rescues expired leases, dispatches runnable runs, runs due activity tasks,
  and fires due timers, until the run completes, fails, is cancelled, or the
  deadline passes.

  Returns whatever `Continuum.await/3` returns for the finished run.

      {:ok, run_id} = Continuum.Test.start_postgres(Checkout, order)
      assert {:ok, %{state: :completed}} = Continuum.Test.drive(run_id)

  Options: `:instance`, `:timeout` (default 5_000 ms), `:batch_size`
  (default 10).
  """
  @doc since: "0.8.0"
  @spec drive(binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def drive(run_id, opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    deadline = System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, 5_000)

    drive_to_result(instance, run_id, deadline, Keyword.get(opts, :batch_size, 10))
  end

  @doc """
  Drive the durable runtime until `run_id` is in one of `states`.

  The same crank as `drive/2`, stopped earlier. Useful for getting a run to the
  point you want to crash it at.

      Continuum.Test.drive_until_state(run_id, [:suspended])
  """
  @doc since: "0.8.0"
  @spec drive_until_state(binary(), [atom()], keyword()) :: :ok | {:error, :timeout}
  def drive_until_state(run_id, states, opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    deadline = System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, 5_000)
    wanted = Enum.map(states, &to_string/1)

    drive_until(instance, run_id, deadline, Keyword.get(opts, :batch_size, 10), wanted)
  end

  @doc """
  Kill a run's engine process the way a node loss would.

  Returns once the engine is dead and has left the instance registry, so the
  next `drive/2` starts a genuinely fresh engine rather than racing the old
  one. Pair with `expire_lease!/2`: a killed engine's lease is still valid
  until it expires, and rescuing it earlier would be lease theft.

  Drive the run to a resting state first (`drive_until_state/3`). Killing an
  engine that is mid-statement is fine in production but takes the SQL Sandbox
  shared connection down with it, which fails the rest of the test for a reason
  that has nothing to do with the workflow.
  """
  @doc since: "0.8.0"
  @spec crash!(binary(), keyword()) :: :ok | {:error, :no_engine}
  def crash!(run_id, opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case Registry.lookup(instance.registry, run_id) do
      [] ->
        {:error, :no_engine}

      [{pid, _value} | _rest] ->
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
        after
          1_000 -> :ok
        end

        await_deregistration(instance, run_id, pid, 200)
    end
  end

  @doc """
  Expire a run's lease so recovery may rescue it.

  Recovery deliberately refuses to touch a run whose lease is still live, so a
  crash-resume test has to move the clock rather than wait out a real TTL.
  """
  @doc since: "0.8.0"
  @spec expire_lease!(binary(), keyword()) :: :ok
  def expire_lease!(run_id, opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    instance.repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [lease_expires_at: past()]
    )

    :ok
  end

  @doc """
  Make every unfired timer on a run due now.

  A multi-day `timer/1` is the thing you least want to wait for in a test; this
  moves its `fires_at` into the past so the next `drive/2` fires it.
  """
  @doc since: "0.8.0"
  @spec elapse_timers!(binary(), keyword()) :: :ok
  def elapse_timers!(run_id, opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    instance.repo.update_all(
      from(t in Timer, where: t.run_id == ^run_id and t.fired == false),
      set: [fires_at: past()]
    )

    :ok
  end

  defp drive_until(instance, run_id, deadline, batch_size, states) do
    cond do
      run_state(instance, run_id) in states and settled?(instance, run_id, states) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :timeout}

      true ->
        tick(instance, batch_size)
        Process.sleep(5)
        drive_until(instance, run_id, deadline, batch_size, states)
    end
  end

  # Poll through the public await surface so `continue_as_new` chains remain
  # one logical run to the driver. Looking only at the root row stops as soon
  # as it stores `{:continued, next_run_id}`, leaving the successor undispatched
  # in the disabled-poller test setup this helper exists for.
  defp drive_to_result(instance, run_id, deadline, batch_size) do
    case Continuum.await(run_id, 0, journal: Journal.Postgres, instance: instance.name) do
      {:error, :timeout} ->
        if System.monotonic_time(:millisecond) > deadline do
          {:error, :timeout}
        else
          tick(instance, batch_size)
          Process.sleep(5)
          drive_to_result(instance, run_id, deadline, batch_size)
        end

      result ->
        result
    end
  end

  # Return only once the engine has finished the callback that wrote the
  # requested state. A run reads "suspended" as soon as the journal transaction
  # commits, while its engine may still be unwinding the statement — and
  # `crash!/2` would then kill a process still using the SQL Sandbox connection.
  # `:sys.get_state/2` serializes behind that callback instead of guessing with
  # a sleep.
  defp settled?(instance, run_id, states) do
    run_state(instance, run_id) in states and local_engine_settled?(instance, run_id, states)
  end

  defp local_engine_settled?(instance, run_id, states) do
    case Registry.lookup(instance.registry, run_id) do
      [] ->
        true

      [{pid, _value} | _rest] ->
        pid
        |> :sys.get_state(1_000)
        |> Map.fetch!(:status)
        |> to_string()
        |> Kernel.in(states)
    end
  catch
    :exit, {:noproc, _} -> true
    :exit, {:timeout, _} -> false
    :exit, _reason -> true
  end

  defp tick(instance, batch_size) do
    opts = [instance: instance.name, batch_size: batch_size]

    _ = Continuum.Runtime.Recovery.recover_once(instance: instance.name)
    _ = Continuum.Runtime.Dispatcher.dispatch_once(opts)
    _ = Continuum.Runtime.ActivityWorker.Dispatcher.dispatch_once(opts)
    _ = Continuum.Runtime.TimerWheel.fire_due_once(opts)
    :ok
  end

  defp run_state(instance, run_id) do
    instance.repo.one(from(r in Run, where: r.id == ^run_id, select: r.state))
  end

  defp await_deregistration(_instance, _run_id, _pid, 0), do: :ok

  defp await_deregistration(instance, run_id, pid, attempts) do
    if Enum.any?(Registry.lookup(instance.registry, run_id), &(elem(&1, 0) == pid)) do
      Process.sleep(5)
      await_deregistration(instance, run_id, pid, attempts - 1)
    else
      :ok
    end
  end

  @doc false
  @spec resolve_journal(keyword()) :: module()
  def resolve_journal(opts) do
    case Keyword.get(opts, :journal, Journal.InMemory) do
      :postgres -> Journal.Postgres
      :in_memory -> Journal.InMemory
      module when is_atom(module) -> module
    end
  end

  defp past do
    DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)
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
