defmodule Continuum.Runtime.TimerWheelTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.TimerWheel
  alias Continuum.Schema.{Run, Timer}

  setup do
    TimerWheel.reset_cache()

    on_exit(fn ->
      TimerWheel.reset_cache()
    end)

    :ok
  end

  defmodule TimerFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      timer(input.ms)
      {:ok, :fired}
    end
  end

  test "timer schedules durable row and completes after TimerWheel fires it" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 5}, journal: Postgres)

    assert_eventually(fn ->
      case Repo.one(from(r in Run, where: r.id == ^run_id)) do
        %Run{state: "suspended", next_wakeup_at: next_wakeup_at}
        when not is_nil(next_wakeup_at) ->
          Repo.aggregate(Timer, :count) == 1

        _ ->
          false
      end
    end)

    run = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert run.state == "suspended"
    assert run.next_wakeup_at != nil

    timer = Repo.one!(Timer)
    assert timer.fired == false

    force_due(timer.id)

    assert {:ok, 1} = TimerWheel.fire_due_once(batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, :fired}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert Repo.one!(Timer).fired == true
    assert Repo.one!(from(r in Run, where: r.id == ^run_id)).next_wakeup_at == nil
  end

  test "TimerWheel skips future timers" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(Timer, :count) == 1
    end)

    assert {:ok, 0} = TimerWheel.fire_due_once(batch_size: 1)
    assert {:error, :timeout} = Continuum.await(run_id, 25, journal: Postgres)
  end

  test "catch-up recovers a fired timer when the immediate wake is lost" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(Timer, :count) == 1 end)
    timer = Repo.one!(Timer)
    force_due(timer.id)

    lease_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

    assert :ok =
             Postgres.fire_timer!(
               Continuum.Runtime.Instance.default(),
               run_id,
               timer.id,
               lease_token
             )

    next_wakeup_at =
      Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.next_wakeup_at))

    assert %DateTime{} = next_wakeup_at
    %{rows: [[db_now]]} = Repo.query!("SELECT clock_timestamp()")
    assert DateTime.compare(next_wakeup_at, db_now) in [:lt, :eq]
    assert :ok = Continuum.Runtime.SignalRouter.catch_up_once()

    assert {:ok, %{state: :completed, result: {:ok, :fired}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "TimerWheel fires a past-due timer loaded during cache hydrate" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(Timer, :count) == 1
    end)

    timer = Repo.one!(Timer)
    force_due(timer.id)

    pid = ensure_timer_wheel!()
    send(pid, :refresh)

    assert {:ok, %{state: :completed, result: {:ok, :fired}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert Repo.one!(from(t in Timer, where: t.id == ^timer.id)).fired == true
  end

  test "concurrent timer wheels claim a due timer only once" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(Timer, :count) == 1 end)
    timer = Repo.one!(Timer)
    force_due(timer.id)

    results =
      1..2
      |> Enum.map(fn _ -> Task.async(fn -> TimerWheel.fire_due_once(batch_size: 1) end) end)
      |> Enum.map(&Task.await(&1, 1_000))

    assert Enum.sort(results) == [{:ok, 0}, {:ok, 1}]

    assert {:ok, %{state: :completed, result: {:ok, :fired}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert Enum.count(event_types(run_id), &(&1 == "timer_fired")) == 1
  end

  test "TimerWheel notification caches a newly armed timer without waiting for refresh" do
    pid = ensure_timer_wheel!()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(Timer, :count) == 1
    end)

    timer = Repo.one!(Timer)
    force_due(timer.id)

    send(
      pid,
      {:notification, self(), make_ref(), "continuum_timer_armed", timer_payload(run_id, timer)}
    )

    assert {:ok, %{state: :completed, result: {:ok, :fired}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert Repo.one!(from(t in Timer, where: t.id == ^timer.id)).fired == true
  end

  test "timer fire rejects stale run authority before appending an event" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(Timer, :count) == 1
    end)

    timer = Repo.one!(Timer)
    run = Repo.one!(from(r in Run, where: r.id == ^run_id))

    assert_raise Continuum.Runtime.JournalError, ~r/lease_mismatch/, fn ->
      Postgres.fire_timer!(
        Continuum.Runtime.Instance.default(),
        run_id,
        timer.id,
        run.lease_token + 1
      )
    end

    assert event_types(run_id) == ["timer_started"]
    assert Repo.one!(Timer).fired == false
  end

  test "cached due timers back off when their run cannot be claimed" do
    pid = ensure_timer_wheel!()

    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn -> Repo.aggregate(Timer, :count) == 1 end)
    timer = Repo.one!(Timer)

    [{engine_pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run_id)
    ref = Process.monitor(engine_pid)
    Process.exit(engine_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^engine_pid, :killed}

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [lease_owner: nil, lease_token: nil, lease_expires_at: nil]
    )

    force_due(timer.id)
    send(pid, :refresh)

    assert_eventually(fn ->
      state = :sys.get_state(pid)

      case :ets.lookup(state.table, timer.id) do
        [{_timer_id, _run_id, retry_at_ms}] ->
          retry_at_ms > System.system_time(:millisecond) + 20_000

        [] ->
          false
      end
    end)

    state = :sys.get_state(pid)
    assert Process.read_timer(state.tick_ref) > 20_000
    assert Repo.one!(from(t in Timer, where: t.id == ^timer.id)).fired == false
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

  defp timer_payload(run_id, %Timer{fires_at: fires_at}) do
    "#{run_id}|#{DateTime.to_iso8601(fires_at)}"
  end

  defp ensure_timer_wheel! do
    case Process.whereis(TimerWheel) do
      nil ->
        start_supervised!(
          {TimerWheel, enabled?: true, listen?: false, refresh_ms: 30_000, window_ms: 60_000}
        )

      pid ->
        pid
    end
  end

  defp event_types(run_id) do
    Repo.all(
      from(e in Continuum.Schema.Event,
        where: e.run_id == ^run_id,
        order_by: [asc: e.seq],
        select: e.event_type
      )
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
end
