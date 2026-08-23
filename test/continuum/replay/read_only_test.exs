defmodule Continuum.Replay.ReadOnlyTest do
  @moduledoc """
  The two documented routes by which "read-only" replay used to mutate durable
  state, closed by `Continuum.Runtime.Journal.ReadOnly`.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.{Postgres, ReadOnly}
  alias Continuum.Schema.{Event, Run, Signal}

  setup do
    previous_journal = Application.get_env(:continuum, :journal)
    Application.put_env(:continuum, :journal, Postgres)

    on_exit(fn ->
      case previous_journal do
        nil -> Application.delete_env(:continuum, :journal)
        journal -> Application.put_env(:continuum, :journal, journal)
      end
    end)

    :ok
  end

  defmodule AwaitingFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      {:ok, await(signal(:decision))}
    end
  end

  defmodule ChargingFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(Continuum.Replay.ReadOnlyTest.Charger.charge(input))
    end
  end

  defmodule Charger do
    use Continuum.Activity

    def charge(input) do
      # Inline in-memory activities run in the calling process, so the process
      # dictionary is enough to witness whether the body executed.
      Process.put(:charges, [input | Process.get(:charges, [])])
      :charged
    end
  end

  describe "a tail signal_awaited with a pending signal row" do
    test "replays read-only without consuming the signal or touching the run" do
      run_id = start_and_suspend()

      {:ok, ^run_id, :delivered} =
        Postgres.deliver_signal!(Instance.default(), run_id, :decision, :approved, [])

      history = Postgres.load(Instance.default(), run_id)
      assert List.last(history).type == :signal_awaited

      before = snapshot_state(run_id)

      assert {:suspended, reason} =
               Continuum.Test.replay(AwaitingFlow, %{}, history,
                 journal: ReadOnly,
                 run_id: run_id
               )

      assert reason == {:awaiting_signal, :decision}
      assert snapshot_state(run_id) == before
    end

    test "the Postgres journal does consume it, which is why the guard exists" do
      run_id = start_and_suspend()

      {:ok, ^run_id, :delivered} =
        Postgres.deliver_signal!(Instance.default(), run_id, :decision, :approved, [])

      history = Postgres.load(Instance.default(), run_id)
      before = snapshot_state(run_id)

      # The Postgres adapter resolves a tail await by consuming the mailbox row
      # inside a transaction, so "replaying" a history through it is a write.
      Continuum.Replay.run(AwaitingFlow, %{}, history,
        journal: Postgres,
        run_id: run_id,
        lease_token: Repo.get(Run, run_id).lease_token
      )

      after_replay = snapshot_state(run_id)
      assert before.pending_signals == 1
      assert after_replay.pending_signals == 0
      assert after_replay.events > before.events
    end
  end

  describe "a tail the workflow steps past" do
    test "does not execute the activity body" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Instance.default(), run_id, ChargingFlow, %{amount: 100})

      before = snapshot_state(run_id)
      Process.delete(:charges)

      assert {:suspended, {:history_exhausted, detail}} =
               Continuum.Test.replay(ChargingFlow, %{amount: 100}, [], journal: ReadOnly)

      assert detail.cursor == 0
      assert {:activity, {Charger, :charge, [%{amount: 100}]}, _opts} = detail.effect

      assert Process.get(:charges) == nil
      assert snapshot_state(run_id) == before
    end

    test "the in-memory journal does execute it, which is why the guard exists" do
      Process.delete(:charges)

      # The append that follows fails (no in-memory run row), which is exactly
      # the point: the activity body already ran by then.
      Continuum.Replay.run(ChargingFlow, %{amount: 100}, [],
        journal: Continuum.Runtime.Journal.InMemory
      )

      assert Process.get(:charges) == [%{amount: 100}]
    end
  end

  describe "the adapter itself" do
    test "raises on every callback, reads included" do
      for {fun, args} <- [
            {:append!, [Instance.default(), "run", %{}, nil]},
            {:load, [Instance.default(), "run"]},
            {:load_with_snapshot, [Instance.default(), "run", nil]},
            {:suspend!, [Instance.default(), "run", nil]},
            {:complete!, [Instance.default(), "run", :ok, nil]},
            {:fail!, [Instance.default(), "run", :boom, nil]},
            {:deliver_signal!, [Instance.default(), "run", :name, :payload]},
            {:get_run, [Instance.default(), "run"]}
          ] do
        assert_raise Continuum.ReadOnlyJournalError, ~r/read-only replay attempted/, fn ->
          apply(ReadOnly, fun, args)
        end
      end
    end
  end

  defp start_and_suspend do
    {:ok, run_id} = Continuum.Runtime.Engine.start_run(AwaitingFlow, %{}, journal: Postgres)

    wait_for_suspension(run_id)
    run_id
  end

  defp wait_for_suspension(run_id, attempts \\ 200) do
    run = Repo.get(Run, run_id)

    cond do
      run && run.state == "suspended" -> :ok
      attempts == 0 -> flunk("run #{run_id} never suspended")
      true -> wait_for_suspension(run_id, attempts - 1)
    end
  end

  defp snapshot_state(run_id) do
    run = Repo.get(Run, run_id)

    %{
      state: run.state,
      lease_token: run.lease_token,
      result: run.result,
      error: run.error,
      next_wakeup_at: run.next_wakeup_at,
      events: Repo.aggregate(from(e in Event, where: e.run_id == ^run_id), :count),
      pending_signals:
        Repo.aggregate(
          from(s in Signal, where: s.run_id == ^run_id and s.delivered == false),
          :count
        )
    }
  end

  defp generate_uuid, do: Ecto.UUID.generate()
end
