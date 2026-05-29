defmodule Continuum.Runtime.ContinueAsNewTest do
  @moduledoc """
  `continue_as_new` cron-style continuation chains (V0.3 PR 6, §3.3).
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{ActivityWorker, Dispatcher, Recovery}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}

  defmodule CycleActivity do
    use Continuum.Activity, retry: [max_attempts: 1]
    def run(n), do: {:ok, n}
  end

  defmodule CycleFlow do
    use Continuum.Workflow, version: 1

    def run(%{n: n, max: max}) do
      {:ok, _} = activity(CycleActivity.run(n))

      if n >= max do
        {:ok, {:done, n}}
      else
        continue_as_new(%{n: n + 1, max: max})
      end
    end
  end

  defmodule ParentOfCycleFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      result = await(child(CycleFlow.run(%{n: 1, max: input.max})))
      {:ok, {:parent_saw, result}}
    end
  end

  setup do
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "a three-cycle chain shares one correlation_id and links continued_from" do
    {:ok, root} = Continuum.start(CycleFlow, %{n: 1, max: 3}, journal: Postgres)

    pump(root)

    runs = chain_runs(root)
    assert length(runs) == 3
    assert runs |> Enum.map(& &1.correlation) |> Enum.uniq() == [root]

    by_pred = Map.new(runs, &{&1.continued_from, &1})
    r1 = by_pred[nil]
    r2 = by_pred[r1.id]
    r3 = by_pred[r2.id]

    assert by_pred[r3.id] == nil
    assert r3.state == "completed"

    assert {:ok, %{state: :completed, result: {:ok, {:done, 3}}}} =
             Continuum.await(r3.id, 2_000, journal: Postgres)

    assert {:ok, %{result: {:continued, _next}}} =
             Continuum.await(r1.id, 2_000, journal: Postgres)
  end

  test "replaying a single iteration reaches the continue_as_new sentinel cleanly" do
    {:ok, root} = Continuum.start(CycleFlow, %{n: 1, max: 3}, journal: Postgres)
    pump(root)

    runs = chain_runs(root)
    by_pred = Map.new(runs, &{&1.continued_from, &1})
    r1 = by_pred[nil]
    r2 = by_pred[r1.id]

    history = Postgres.load(Continuum.Runtime.Instance.default(), r2.id)
    assert Enum.any?(history, &(&1.type == :run_continued_as_new))

    input =
      Repo.one(from(r in Run, where: r.id == ^r2.id, select: r.input)) |> :erlang.binary_to_term()

    assert {:continued, _next_run_id} = Continuum.Test.replay(CycleFlow, input, history)
  end

  test "crash mid-iteration resumes and starts the next cycle exactly once" do
    {:ok, root} = Continuum.start(CycleFlow, %{n: 1, max: 3}, journal: Postgres)

    # Drive cycle 1 to completion so cycle 2 (run2) exists and suspends on its
    # own activity.
    drive_activities()
    assert_eventually(fn -> successor_of(root) != nil end)
    run2 = successor_of(root)

    Dispatcher.dispatch_once(owner: "cycle-crash", batch_size: 10)
    assert_eventually(fn -> run_state(run2) == "suspended" end)

    # Kill cycle 2 mid-flight, before it continues.
    [{pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run2)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000

    expire_lease(run2)
    assert {:ok, _} = Recovery.recover_once()

    pump(root)

    runs = chain_runs(root)
    assert length(runs) == 3
    # Exactly one successor of run2 (cycle 3) — no duplicate from the crash.
    assert Repo.aggregate(from(r in Run, where: r.continued_from_run_id == ^run2), :count) == 1

    terminal = Enum.find(runs, &(&1.state == "completed" and not continued?(&1.result)))

    assert {:ok, %{result: {:ok, {:done, 3}}}} =
             Continuum.await(terminal.id, 2_000, journal: Postgres)
  end

  test "a continued child stays a child and the parent awaits the chain's final result" do
    {:ok, parent_id} = Continuum.start(ParentOfCycleFlow, %{max: 3}, journal: Postgres)

    pump_until(fn -> run_state(parent_id) in ["completed", "failed"] end)

    assert {:ok, %{state: :completed, result: {:ok, {:parent_saw, {:ok, {:done, 3}}}}}} =
             Continuum.await(parent_id, 3_000, journal: Postgres)

    # Every run in the child's chain carries the parent linkage.
    child_runs = Repo.all(from(r in Run, where: r.parent_run_id == ^parent_id, select: r.id))
    assert length(child_runs) == 3
  end

  # --- driving helpers -------------------------------------------------------

  defp pump(root, attempts \\ 400)

  defp pump(root, attempts) when attempts > 0 do
    if chain_done?(root) do
      :ok
    else
      Dispatcher.dispatch_once(owner: "cycle-run", batch_size: 10)
      ActivityWorker.Dispatcher.dispatch_once(owner: "cycle-act", batch_size: 10)
      Process.sleep(5)
      pump(root, attempts - 1)
    end
  end

  defp pump(_root, 0), do: flunk("chain did not terminate")

  defp pump_until(fun, attempts \\ 400)

  defp pump_until(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Dispatcher.dispatch_once(owner: "cycle-run", batch_size: 10)
      ActivityWorker.Dispatcher.dispatch_once(owner: "cycle-act", batch_size: 10)
      Process.sleep(5)
      pump_until(fun, attempts - 1)
    end
  end

  defp pump_until(_fun, 0), do: flunk("condition not met")

  defp drive_activities do
    assert_eventually(fn ->
      Repo.aggregate(from(t in ActivityTask, where: t.state == "available"), :count) >= 1
    end)

    ActivityWorker.Dispatcher.dispatch_once(owner: "cycle-act", batch_size: 10)
  end

  defp chain_done?(root) do
    Enum.any?(chain_runs(root), &(&1.state == "completed" and not continued?(&1.result)))
  end

  defp chain_runs(root) do
    Repo.all(
      from(r in Run,
        where: r.id == ^root or r.correlation_id == ^root,
        select: %{
          id: r.id,
          state: r.state,
          result: r.result,
          correlation: r.correlation_id,
          continued_from: r.continued_from_run_id
        }
      )
    )
  end

  defp successor_of(run_id) do
    Repo.one(from(r in Run, where: r.continued_from_run_id == ^run_id, select: r.id))
  end

  defp run_state(run_id), do: Repo.one(from(r in Run, where: r.id == ^run_id, select: r.state))

  defp continued?(nil), do: false
  defp continued?(binary), do: match?({:continued, _}, :erlang.binary_to_term(binary))

  defp expire_lease(run_id) do
    past = DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)
    Repo.update_all(from(r in Run, where: r.id == ^run_id), set: [lease_expires_at: past])
  end

  defp assert_eventually(fun, attempts \\ 200)

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
