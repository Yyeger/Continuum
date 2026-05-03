defmodule Continuum.Runtime.SignalRouterTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.TimerWheel
  alias Continuum.Schema.{Event, Run, Signal, Timer}

  setup do
    previous_journal = Application.get_env(:continuum, :journal)
    Application.put_env(:continuum, :journal, Postgres)

    on_exit(fn ->
      restore_env(:journal, previous_journal)
    end)

    start_supervised!({Continuum.Runtime.SignalRouter, listen?: false})
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

  test "delivers signals through the durable Postgres mailbox" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      event_types(run_id) == ["signal_awaited"]
    end)

    assert Repo.one!(from(r in Run, where: r.id == ^run_id)).state == "suspended"

    :ok = Continuum.signal(run_id, :decision, :go)

    assert {:ok, %{state: :completed, result: {:ok, :went}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    assert ["signal_awaited", "signal_received"] = event_types(run_id)

    signal = Repo.one!(from(s in Signal, where: s.run_id == ^run_id))
    assert signal.name == "decision"
    assert signal.delivered == true
  end

  test "wakes a suspended local engine from a Postgres notification" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(DurableSignalFlow, %{}, journal: Postgres)

    assert_eventually(fn ->
      event_types(run_id) == ["signal_awaited"]
    end)

    :ok = Postgres.deliver_signal!(run_id, :decision, :go)
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
