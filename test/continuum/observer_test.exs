defmodule Continuum.ObserverTest do
  use Continuum.Test.DataCase, async: false

  defmodule SideEffectFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      Continuum.side_effect(fn -> {:ok, input.value} end)
    end
  end

  defmodule SignalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      await(signal(:approve))
    end
  end

  defmodule TimerFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      timer(seconds(60))
      :done
    end
  end

  test "lists runs and decoded events from the configured instance repo" do
    {:ok, run_id} = Continuum.Test.start_postgres(SideEffectFlow, %{value: 42})
    assert {:ok, %{state: :completed, result: {:ok, 42}}} = await_postgres(run_id)

    assert {:ok, page} = Continuum.Observer.list_runs(search: run_id, state: :completed)
    assert page.total == 1
    assert [%{run_id: ^run_id, state: :completed, result: {:ok, 42}}] = page.entries

    assert {:ok, run} = Continuum.Observer.get_run(run_id)
    assert run.workflow =~ "SideEffectFlow"
    assert run.input == %{value: 42}

    assert {:ok, events} = Continuum.Observer.list_events(run_id)
    assert Enum.any?(events, &match?(%{type: :side_effect, payload: %{payload: {:ok, 42}}}, &1))
  end

  test "runs-index topic receives coarse state updates" do
    Continuum.Test.reset_in_memory!()
    assert :ok = Continuum.Observer.subscribe_runs()

    {:ok, run_id} = Continuum.Test.start_synchronous(SideEffectFlow, %{value: 7})
    assert {:ok, %{state: :completed, result: {:ok, 7}}} = Continuum.await(run_id, 1_000)

    assert_receive {:run_state_changed, ^run_id, :running}
    assert_receive {:run_state_changed, ^run_id, :completed}
  end

  test "send_signal uses the observer instance and advances a suspended run" do
    {:ok, run_id} = Continuum.Test.start_postgres(SignalFlow, %{})

    assert_eventually(fn ->
      Repo.exists?(
        from(e in Continuum.Schema.Event,
          where: e.run_id == ^run_id and e.event_type == "signal_awaited"
        )
      )
    end)

    assert {:ok, payload} = Continuum.Observer.decode_signal_payload(~s({"ok":true}))
    assert :ok = Continuum.Observer.send_signal(run_id, "approve", payload)

    assert {:ok, %{state: :completed, result: %{"ok" => true}}} = await_postgres(run_id)
  end

  test "send_signal rejects unknown signal names without creating atoms" do
    assert {:error, {:unknown_signal, "definitely_unknown_signal_name"}} =
             Continuum.Observer.send_signal(
               Ecto.UUID.generate(),
               "definitely_unknown_signal_name",
               %{}
             )
  end

  test "cancel_run uses the observer instance" do
    {:ok, run_id} = Continuum.Test.start_postgres(TimerFlow, %{})

    assert {:error, :timeout} =
             Continuum.await(run_id, 20, journal: Continuum.Runtime.Journal.Postgres)

    assert :ok = Continuum.Observer.cancel_run(run_id)

    assert {:error, %{state: :failed, error: :cancelled}} =
             Continuum.await(run_id, 1_000, journal: Continuum.Runtime.Journal.Postgres)

    assert {:ok, %{entries: [%{run_id: ^run_id, state: :cancelled}]}} =
             Continuum.Observer.list_runs(state: :cancelled)

    assert {:ok, %{entries: []}} = Continuum.Observer.list_runs(state: :failed, search: run_id)
  end

  test "runs-index topic receives suspended updates for Postgres runs" do
    assert :ok = Continuum.Observer.subscribe_runs()

    {:ok, run_id} = Continuum.Test.start_postgres(TimerFlow, %{})
    wait_for_event(run_id, "timer_started")

    assert_receive {:run_state_changed, ^run_id, :running}
    assert_receive {:run_state_changed, ^run_id, :suspended}
  end

  defp await_postgres(run_id) do
    Continuum.await(run_id, 1_000, journal: Continuum.Runtime.Journal.Postgres)
  end

  defp assert_eventually(fun, attempts \\ 50)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp wait_for_event(run_id, event_type) do
    assert_eventually(fn ->
      Repo.exists?(
        from(e in Continuum.Schema.Event,
          where: e.run_id == ^run_id and e.event_type == ^event_type
        )
      )
    end)
  end
end
