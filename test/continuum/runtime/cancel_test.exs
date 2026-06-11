defmodule Continuum.Runtime.CancelTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.ActivityWorker.Dispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.TimerWheel
  alias Continuum.Schema.{ActivityTask, Event, Run, Timer}

  defmodule CancelActivity do
    use Continuum.Activity, retry: [max_attempts: 2, backoff: :constant, base_ms: 1]

    def run(value), do: {:ok, value}
  end

  defmodule ActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(CancelActivity.run(input.value))
    end
  end

  defmodule TimerFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      timer(input.ms)
      {:ok, :fired}
    end
  end

  test "cancel discards pending activity tasks" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{value: 10}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    assert :ok = Continuum.cancel(run_id)

    assert {:error, %{state: :cancelled, error: :cancelled}} =
             Continuum.await(run_id, 25, journal: Postgres)

    task = Repo.one!(ActivityTask)
    assert task.state == "discarded"
    assert task.lease_owner == nil
    assert task.lease_expires_at == nil

    assert {:ok, 0} = Dispatcher.dispatch_once(owner: "cancel-test", batch_size: 1)
    assert event_types(run_id) == ["activity_scheduled"]
  end

  test "cancel marks pending timers fired so TimerWheel cannot complete the run later" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(Timer, :count) == 1
    end)

    assert :ok = Continuum.cancel(run_id)

    assert {:error, %{state: :cancelled, error: :cancelled}} =
             Continuum.await(run_id, 25, journal: Postgres)

    timer = Repo.one!(Timer)
    assert timer.fired == true

    run = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert run.state == "cancelled"
    assert run.next_wakeup_at == nil

    force_due(timer.id)
    assert {:ok, 0} = TimerWheel.fire_due_once(batch_size: 1)
    assert event_types(run_id) == ["timer_started"]
  end

  test "fire_timer! refuses to append timer_fired to a cancelled run" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(TimerFlow, %{ms: 60_000}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(Timer, :count) == 1
    end)

    timer = Repo.one!(Timer)
    lease_token = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.lease_token))

    assert :ok = Continuum.cancel(run_id)

    # Simulate a TimerWheel that claimed before the cancel committed and fires
    # after: the journal must reject by run state, never append to the
    # terminal run's history.
    assert_raise Continuum.Runtime.JournalError, ~r/run_not_active/, fn ->
      Postgres.fire_timer!(Continuum.Runtime.Instance.default(), run_id, timer.id, lease_token)
    end

    assert event_types(run_id) == ["timer_started"]
  end

  test "cancel can complete durable suspended run without a local engine" do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(
        Continuum.Runtime.Instance.default(),
        run_id,
        TimerFlow,
        %{ms: 60_000}
      )

    :ok = Postgres.suspend!(Continuum.Runtime.Instance.default(), run_id, nil)

    assert Registry.lookup(Continuum.Runtime.Registry, run_id) == []
    assert :ok = Continuum.cancel(run_id, journal: Postgres)

    assert {:error, %{state: :cancelled, error: :cancelled}} =
             Continuum.await(run_id, 25, journal: Postgres)

    run = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert run.state == "cancelled"
    assert run.error == :erlang.term_to_binary(:cancelled)
  end

  test "cancel maps unknown durable run to not_found" do
    assert {:error, :not_found} =
             Continuum.cancel(Ecto.UUID.generate(), journal: Postgres)
  end

  test "cancel distinguishes a live lease held elsewhere from a missing run" do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(
        Continuum.Runtime.Instance.default(),
        run_id,
        TimerFlow,
        %{ms: 60_000}
      )

    future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.truncate(:microsecond)

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [
        state: "suspended",
        lease_owner: "othernode@nohost/Elixir.Continuum/1",
        lease_token: 999_999_999,
        lease_expires_at: future
      ]
    )

    assert {:error, :owned_elsewhere} = Continuum.cancel(run_id, journal: Postgres)
  end

  test "cancel of a terminal durable run reports run_not_active" do
    run_id = Ecto.UUID.generate()

    :ok =
      Postgres.start_run(
        Continuum.Runtime.Instance.default(),
        run_id,
        TimerFlow,
        %{ms: 60_000}
      )

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [state: "failed", error: :erlang.term_to_binary(:boom)]
    )

    assert {:error, {:run_not_active, :failed}} = Continuum.cancel(run_id, journal: Postgres)
  end

  test "cancelled leased activity tasks cannot be retried back to available" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(ActivityFlow, %{value: 10}, journal: Postgres)

    assert_eventually(fn ->
      Repo.aggregate(ActivityTask, :count) == 1
    end)

    task = Repo.one!(ActivityTask)
    run = Repo.one!(from(r in Run, where: r.id == ^run_id))

    Repo.update_all(
      from(t in ActivityTask, where: t.id == ^task.id),
      set: [state: "leased", lease_owner: "worker-a", lease_expires_at: future_time()]
    )

    claimed_task =
      task.mfa
      |> decode_term()
      |> Map.merge(%{
        id: task.id,
        run_id: task.run_id,
        seq: task.seq,
        attempt: task.attempt,
        lease_owner: "worker-a"
      })

    assert :ok = Continuum.cancel(run_id)

    assert_raise Continuum.Runtime.JournalError, ~r/run_not_active/, fn ->
      Postgres.retry_activity_task!(
        Continuum.Runtime.Instance.default(),
        claimed_task,
        :boom,
        1_000,
        run.lease_token
      )
    end

    assert Repo.one!(ActivityTask).state == "discarded"
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

  defp future_time do
    DateTime.utc_now()
    |> DateTime.add(60, :second)
    |> DateTime.truncate(:microsecond)
  end

  defp decode_term(binary) when is_binary(binary), do: :erlang.binary_to_term(binary)

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
