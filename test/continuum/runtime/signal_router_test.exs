defmodule Continuum.Runtime.SignalRouterTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run, Signal}

  defmodule DurableSignalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      case await(signal(:decision)) do
        :go -> {:ok, :went}
        other -> {:ok, other}
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

  defp event_types(run_id) do
    Repo.all(
      from(e in Event,
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
