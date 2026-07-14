defmodule Continuum.Runtime.SignalRouterTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.TimerWheel
  alias Continuum.Runtime.Instance
  alias Continuum.Schema.{Event, Run, Signal, Timer}

  setup do
    previous_journal = Application.get_env(:continuum, :journal)
    Application.put_env(:continuum, :journal, Postgres)

    on_exit(fn ->
      restore_env(:journal, previous_journal)
    end)

    unless Process.whereis(Instance.default().signal_router) do
      start_supervised!({Continuum.Runtime.SignalRouter, listen?: false})
    end

    :ok
  end

  defmodule DurableSignalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      case await(signal(:decision)) do
        :go -> {:ok, :went}
        other -> {:ok, other}
      end
    end
  end

  defmodule SignalTimeoutFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      case await(signal(:decision, timeout: input.timeout_ms)) do
        :timeout -> {:ok, :timed_out}
        payload -> {:ok, payload}
      end
    end
  end

  defmodule CustomSignalJournal do
    def deliver_signal!(_instance, run_id, name, %{test_pid: test_pid} = payload) do
      send(test_pid, {:custom_signal_delivered, run_id, name, payload})
      :ok
    end
  end

  defmodule UnsupportedSignalJournal do
  end

  test "delivers signals through the durable Postgres mailbox" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      event_types(run_id) == ["signal_awaited"]
    end)

    # The signal_awaited event is journaled before the engine commits the
    # suspended state — poll for the transition instead of racing it.
    assert_eventually(fn ->
      Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
    end)

    :ok = Continuum.signal(run_id, :decision, :go)

    assert {:ok, %{state: :completed, result: {:ok, :went}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert ["signal_awaited", "signal_received"] = event_types(run_id)

    signal = Repo.one!(from(s in Signal, where: s.run_id == ^run_id))
    assert signal.name == "decision"
    assert signal.delivered == true
  end

  test "signaling a missing run returns {:error, :not_found}" do
    assert {:error, :not_found} = Continuum.signal(Ecto.UUID.generate(), :decision, :go)
  end

  test "rejects node-local identities in durable signal payloads" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
    end)

    assert_raise Continuum.DurableTermError, ~r/signal.approver/, fn ->
      Continuum.signal(run_id, :decision, %{approver: self()})
    end

    assert Repo.aggregate(Signal, :count) == 0
  end

  test "signaling a terminal run returns {:error, :run_terminal}" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      event_types(run_id) == ["signal_awaited"]
    end)

    :ok = Continuum.signal(run_id, :decision, :go)

    assert {:ok, %{state: :completed, result: {:ok, :went}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert {:error, :run_terminal} = Continuum.signal(run_id, :decision, :go)
  end

  test "custom journals receive signals through their adapter callback" do
    run_id = Ecto.UUID.generate()
    payload = %{test_pid: self(), decision: :go}

    assert :ok =
             Continuum.Runtime.SignalRouter.deliver(run_id, :decision, payload,
               journal: CustomSignalJournal
             )

    assert_receive {:custom_signal_delivered, ^run_id, :decision, ^payload}
  end

  test "custom journals without signal delivery report unsupported" do
    assert {:error, :unsupported} =
             Continuum.Runtime.SignalRouter.deliver(
               Ecto.UUID.generate(),
               :decision,
               :go,
               journal: UnsupportedSignalJournal
             )
  end

  test "catch_up_once wakes a local engine with an undelivered mailbox row" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      event_types(run_id) == ["signal_awaited"]
    end)

    # Insert the signal row directly — no pg_notify, no wake — simulating a
    # dropped notification while the engine is parked on a live lease.
    Repo.insert!(%Signal{
      run_id: run_id,
      name: "decision",
      payload: :erlang.term_to_binary(:go),
      delivered: false,
      inserted_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
    })

    assert :ok = Continuum.Runtime.SignalRouter.catch_up_once()

    assert {:ok, %{state: :completed, result: {:ok, :went}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "catch_up_once pages only local live-leased runs and reports scan metrics" do
    {:ok, first_run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    {:ok, second_run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      Enum.all?([first_run_id, second_run_id], fn run_id ->
        Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
      end)
    end)

    global_run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Instance.default(), global_run_id, DurableSignalFlow, %{})

    inserted_at =
      DateTime.utc_now()
      |> DateTime.add(-2, :second)
      |> DateTime.truncate(:microsecond)

    Enum.each([first_run_id, second_run_id, global_run_id], fn run_id ->
      Repo.insert!(%Signal{
        run_id: run_id,
        name: "decision",
        payload: :erlang.term_to_binary(:go),
        delivered: false,
        inserted_at: inserted_at
      })
    end)

    handler_id = "signal-router-catch-up-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :signal_router, :catch_up],
        fn event, measurements, metadata, test_pid ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert :ok = Continuum.Runtime.SignalRouter.catch_up_once(batch_size: 1)

    assert_receive {:telemetry, [:continuum, :signal_router, :catch_up], measurements,
                    %{batch_size: 1, status: :ok}}

    assert measurements.scanned_count >= 2
    assert measurements.matched_count == 2
    assert measurements.woken_count == 2
    assert measurements.page_count == measurements.scanned_count
    assert measurements.oldest_wake_age_ms >= 1_000

    for run_id <- [first_run_id, second_run_id] do
      assert {:ok, %{state: :completed, result: {:ok, :went}}} =
               Continuum.await(run_id, 1_000, journal: Postgres)
    end

    assert Repo.one!(from(s in Signal, where: s.run_id == ^global_run_id)).delivered == false
  end

  test "catch_up_once rejects an invalid batch size" do
    assert_raise ArgumentError, ~r/positive integer/, fn ->
      Continuum.Runtime.SignalRouter.catch_up_once(batch_size: 0)
    end
  end

  test "active wake catch-up has a partial index" do
    %{rows: [[definition]]} =
      Repo.query!("""
      SELECT indexdef
      FROM pg_indexes
      WHERE schemaname = current_schema()
        AND indexname = 'continuum_runs_catch_up_idx'
      """)

    assert definition =~ "next_wakeup_at"
    assert definition =~ "lease_expires_at"
    assert definition =~ "lease_owner IS NOT NULL"
    assert definition =~ "lease_token IS NOT NULL"
  end

  test "catch_up_once wakes a local engine with durable wake evidence" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"
    end)

    [{engine_pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run_id)
    :erlang.trace(engine_pid, true, [:receive])

    on_exit(fn ->
      if Process.alive?(engine_pid), do: :erlang.trace(engine_pid, false, [:receive])
    end)

    past = DateTime.utc_now() |> DateTime.add(-1, :second) |> DateTime.truncate(:microsecond)

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [next_wakeup_at: past]
    )

    assert :ok = Continuum.Runtime.SignalRouter.catch_up_once()
    assert_receive {:trace, ^engine_pid, :receive, {:"$gen_cast", :wake}}
    assert %{run_id: ^run_id} = :sys.get_state(engine_pid)
    assert Repo.one!(from(r in Run, where: r.id == ^run_id)).next_wakeup_at == nil
  end

  test "already-pending signal journals signal_received without signal_awaited" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Instance.default(), run_id, DurableSignalFlow, %{})
    {:ok, _} = Postgres.deliver_signal!(Instance.default(), run_id, :decision, :go)

    assert {:ok, 1} =
             Continuum.Runtime.Dispatcher.dispatch_once(owner: "pending-signal", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, :went}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert ["signal_received"] = event_types(run_id)

    signal = Repo.one!(from(s in Signal, where: s.run_id == ^run_id))
    assert signal.delivered == true
  end

  test "already-pending signal with timeout skips timeout timer creation" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Instance.default(), run_id, SignalTimeoutFlow, %{timeout_ms: 60_000})
    {:ok, _} = Postgres.deliver_signal!(Instance.default(), run_id, :decision, :go)

    assert {:ok, 1} =
             Continuum.Runtime.Dispatcher.dispatch_once(
               owner: "pending-signal-timeout",
               batch_size: 1
             )

    assert {:ok, %{state: :completed, result: {:ok, :go}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert ["signal_received"] = event_types(run_id)
    # The fast-path armed no timeout timer (next_wakeup_at is set by the
    # delivery itself, so the timer table is the meaningful assertion).
    assert Repo.aggregate(from(t in Timer, where: t.run_id == ^run_id), :count) == 0
  end

  test "already-pending signal fast-path replays without drift" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Instance.default(), run_id, DurableSignalFlow, %{})
    {:ok, _} = Postgres.deliver_signal!(Instance.default(), run_id, :decision, :go)

    assert {:ok, 1} =
             Continuum.Runtime.Dispatcher.dispatch_once(
               owner: "pending-signal-replay",
               batch_size: 1
             )

    assert {:ok, %{state: :completed, result: {:ok, :went}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    history = Continuum.Test.history(run_id, journal: Postgres)
    assert [%{type: :signal_received}] = history
    assert {:ok, {:ok, :went}} = Continuum.Test.replay(DurableSignalFlow, %{}, history)
  end

  test "wakes a suspended local engine from a Postgres notification" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      event_types(run_id) == ["signal_awaited"]
    end)

    {:ok, _} =
      Postgres.deliver_signal!(Continuum.Runtime.Instance.default(), run_id, :decision, :go)

    assert {:error, :timeout} = Continuum.await(run_id, 25, journal: Postgres)

    send(
      Process.whereis(Continuum.Runtime.SignalRouter),
      {:notification, self(), make_ref(), "continuum_signal", run_id}
    )

    assert {:ok, %{state: :completed, result: {:ok, :went}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "signal await timeout completes with :timeout when the timeout timer wins" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SignalTimeoutFlow, %{timeout_ms: 60_000},
        journal: Postgres
      )

    assert_eventually(fn ->
      event_types(run_id) == ["signal_awaited"]
    end)

    timer = Repo.one!(from(t in Timer, where: t.run_id == ^run_id))
    assert Repo.one!(from(r in Run, where: r.id == ^run_id)).next_wakeup_at != nil

    force_due(timer.id)
    assert {:ok, 1} = TimerWheel.fire_due_once(batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, :timed_out}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert ["signal_awaited", "timer_fired"] = event_types(run_id)
    assert Repo.one!(from(t in Timer, where: t.id == ^timer.id)).fired == true
  end

  test "signal await timeout is cancelled when the signal wins" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SignalTimeoutFlow, %{timeout_ms: 60_000},
        journal: Postgres
      )

    assert_eventually(fn ->
      event_types(run_id) == ["signal_awaited"]
    end)

    timer = Repo.one!(from(t in Timer, where: t.run_id == ^run_id))

    :ok = Continuum.signal(run_id, :decision, :go)

    assert {:ok, %{state: :completed, result: {:ok, :go}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert ["signal_awaited", "signal_received"] = event_types(run_id)
    assert Repo.one!(from(t in Timer, where: t.id == ^timer.id)).fired == true
    assert Repo.one!(from(r in Run, where: r.id == ^run_id)).next_wakeup_at == nil
  end

  defp event_types(run_id) do
    Repo.all(
      from(e in Event,
        where: e.run_id == ^run_id,
        order_by: [asc: e.seq],
        select: e.event_type
      )
    )
  end

  defp force_due(timer_id) do
    due_at =
      DateTime.utc_now()
      |> DateTime.add(-1, :second)
      |> DateTime.truncate(:microsecond)

    Repo.update_all(
      from(t in Timer, where: t.id == ^timer_id),
      set: [fires_at: due_at]
    )
  end

  defp assert_eventually(fun, attempts \\ 20)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp restore_env(key, nil), do: Application.delete_env(:continuum, key)
  defp restore_env(key, value), do: Application.put_env(:continuum, key, value)
end
