defmodule Continuum.Runtime.TimerWheelTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.TimerWheel
  alias Continuum.Schema.{Run, Timer}

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
      Repo.aggregate(Timer, :count) == 1
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
end
