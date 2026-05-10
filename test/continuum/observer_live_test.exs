defmodule Continuum.ObserverLiveTest do
  use Continuum.Test.ConnCase, async: false

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

  test "runs index renders and updates after a run starts" do
    {:ok, view, html} = live(build_conn(), "/continuum")
    assert html =~ "No runs found"

    {:ok, run_id} = Continuum.Test.start_postgres(SideEffectFlow, %{value: 42})
    assert {:ok, %{state: :completed, result: {:ok, 42}}} = await_postgres(run_id)

    assert_eventually(fn ->
      html = render(view)
      html =~ run_id and html =~ "completed" and html =~ "SideEffectFlow"
    end)

    html =
      view
      |> form("#co-runs-filter", filters: %{search: "", state: "running", workflow: ""})
      |> render_change()

    refute html =~ run_id

    html =
      view
      |> form("#co-runs-filter", filters: %{search: "", state: "completed", workflow: ""})
      |> render_change()

    assert html =~ run_id
  end

  test "runs index shows cancelled runs in the cancelled filter" do
    {:ok, run_id} = Continuum.Test.start_postgres(TimerFlow, %{})
    wait_for_event(run_id, "timer_started")

    assert :ok = Continuum.Observer.cancel_run(run_id)

    assert {:error, %{state: :failed, error: :cancelled}} =
             Continuum.await(run_id, 1_000, journal: Continuum.Runtime.Journal.Postgres)

    {:ok, view, _html} = live(build_conn(), "/continuum")

    html =
      view
      |> form("#co-runs-filter", filters: %{search: "", state: "cancelled", workflow: ""})
      |> render_change()

    assert html =~ run_id
    assert html =~ "cancelled"

    html =
      view
      |> form("#co-runs-filter", filters: %{search: run_id, state: "failed", workflow: ""})
      |> render_change()

    assert html =~ "No runs found"
  end

  test "run detail renders the event timeline" do
    {:ok, run_id} = Continuum.Test.start_postgres(SideEffectFlow, %{value: 5})
    assert {:ok, %{state: :completed, result: {:ok, 5}}} = await_postgres(run_id)

    {:ok, _view, html} = live(build_conn(), "/continuum/runs/#{run_id}")

    assert html =~ run_id
    assert html =~ "SideEffectFlow"
    assert html =~ "side_effect"
    assert html =~ "payload"
  end

  test "run detail sends a signal and refreshes the timeline" do
    {:ok, run_id} = Continuum.Test.start_postgres(SignalFlow, %{})
    wait_for_event(run_id, "signal_awaited")

    {:ok, view, html} = live(build_conn(), "/continuum/runs/#{run_id}")
    assert html =~ "signal_awaited"

    view
    |> form("#co-signal-form", signal: %{name: "approve", payload: ~s({"ok":true})})
    |> render_submit()

    assert {:ok, %{state: :completed, result: %{"ok" => true}}} = await_postgres(run_id)

    assert_eventually(fn ->
      html = render(view)
      html =~ "completed" and html =~ "signal_received"
    end)
  end

  test "run detail shows suspended transition through runs-topic subscription" do
    {:ok, run_id} = Continuum.Test.start_postgres(TimerFlow, %{})
    {:ok, view, _html} = live(build_conn(), "/continuum/runs/#{run_id}")

    wait_for_event(run_id, "timer_started")

    assert_eventually(fn ->
      render(view) =~ "suspended"
    end)
  end

  test "run detail cancels a suspended run" do
    {:ok, run_id} = Continuum.Test.start_postgres(TimerFlow, %{})
    wait_for_event(run_id, "timer_started")

    {:ok, view, html} = live(build_conn(), "/continuum/runs/#{run_id}")
    assert html =~ "timer_started"

    view
    |> element("#co-cancel-run")
    |> render_click()

    assert {:error, %{state: :failed, error: :cancelled}} =
             Continuum.await(run_id, 1_000, journal: Continuum.Runtime.Journal.Postgres)

    assert_eventually(fn ->
      render(view) =~ "cancelled"
    end)
  end

  test "mounted observer can target a named instance" do
    name = :observer_named_instance

    children =
      Continuum.children(
        name: name,
        repo: Continuum.Test.Repo,
        recovery: false,
        dispatcher: false,
        activity_dispatcher: false,
        timer_wheel: false,
        signal_router: false
      )

    start_supervised!(%{
      id: {Supervisor, name},
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
      type: :supervisor
    })

    {:ok, run_id} =
      Continuum.Test.start_postgres(SideEffectFlow, %{value: 9}, instance: name)

    assert {:ok, %{state: :completed, result: {:ok, 9}}} =
             Continuum.await(run_id, 1_000,
               journal: Continuum.Runtime.Journal.Postgres,
               instance: name
             )

    {:ok, _view, html} = live(build_conn(), "/named-continuum")

    assert html =~ run_id
    assert html =~ "observer_named_instance"
  end

  defp await_postgres(run_id) do
    Continuum.await(run_id, 1_000, journal: Continuum.Runtime.Journal.Postgres)
  end

  defp wait_for_event(run_id, event_type) do
    assert_eventually(fn ->
      Repo.exists?(
        from(e in Continuum.Schema.Event,
          where: e.run_id == ^run_id and e.event_type == ^event_type
        )
      )
    end)
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
end
