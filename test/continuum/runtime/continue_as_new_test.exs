defmodule Continuum.Runtime.ContinueAsNewTest do
  @moduledoc """
  `continue_as_new` cron-style continuation chains (V0.3 PR 6, §3.3).
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{ActivityWorker, Dispatcher, Recovery}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run, Signal}

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

  defmodule SignalCycleFlow do
    use Continuum.Workflow, version: 1

    def run(%{phase: 1}) do
      continue_as_new(%{phase: 2})
    end

    def run(%{phase: 2}) do
      payload = await(signal(:go))
      {:ok, {:finished, payload}}
    end
  end

  defmodule SlowChildFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: {:ok, await(signal(:never))}
  end

  defmodule OrphaningFlow do
    use Continuum.Workflow, version: 1

    def run(%{phase: 1}) do
      start_child(SlowChildFlow, %{}, id: "bg")
      continue_as_new(%{phase: 2})
    end

    def run(%{phase: 2}) do
      payload = await(signal(:go))
      {:ok, payload}
    end
  end

  setup do
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Signal)
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

    # Awaiting the chain root follows the chain to the final terminal result;
    # the internal {:continued, _} sentinel is never exposed.
    assert {:ok, %{state: :completed, result: {:ok, {:done, 3}}}} =
             Continuum.await(r1.id, 2_000, journal: Postgres)
  end

  test "a single-iteration run uses its own id as correlation_id" do
    {:ok, run_id} = Continuum.start(CycleFlow, %{n: 1, max: 1}, journal: Postgres)

    pump_until(fn -> run_state(run_id) == "completed" end)

    assert Repo.one(from(r in Run, where: r.id == ^run_id, select: r.correlation_id)) == run_id
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

  test "paranoid verify_run! re-replays a continued run to its sentinel" do
    {:ok, root} = Continuum.start(CycleFlow, %{n: 1, max: 2}, journal: Postgres)
    pump(root)

    input =
      Repo.one(from(r in Run, where: r.id == ^root, select: r.input))
      |> :erlang.binary_to_term()

    assert :ok = Continuum.Test.Paranoid.verify_run!(CycleFlow, input, root, journal: Postgres)
  end

  test "the successor inherits the predecessor's namespace and attributes" do
    {:ok, root} =
      Continuum.start(CycleFlow, %{n: 1, max: 2},
        journal: Postgres,
        namespace: "tenant-b",
        attributes: %{tenant: "globex"}
      )

    pump(root)

    successor = successor_of(root)
    run = Repo.one!(from(r in Run, where: r.id == ^successor))
    assert run.namespace == "tenant-b"
    assert run.attributes == %{"tenant" => "globex"}
  end

  test "continue_as_new stamps the successor with the currently loaded version" do
    metadata = CycleFlow.__continuum_workflow__()
    current_hash = metadata.version_hash
    old_hash = "superseded-version-hash"

    # Simulate a run started under a previous deploy of CycleFlow: its row is
    # pinned to old_hash, which still resolves to a loaded entrypoint.
    :ok = Continuum.VersionRegistry.register(CycleFlow, 1, old_hash, metadata.entrypoint)

    root = Ecto.UUID.generate()

    %Run{}
    |> Ecto.Changeset.change(%{
      id: root,
      workflow: inspect(CycleFlow),
      version_hash: old_hash,
      state: "suspended",
      input: :erlang.term_to_binary(%{n: 1, max: 2}),
      correlation_id: root,
      next_wakeup_at:
        DateTime.utc_now() |> DateTime.add(-1, :second) |> DateTime.truncate(:microsecond)
    })
    |> Repo.insert!()

    pump(root)

    successor = successor_of(root)

    # The successor starts with empty history, so it picks up the latest
    # loaded version instead of inheriting the superseded pin.
    assert Repo.one(from(r in Run, where: r.id == ^successor, select: r.version_hash)) ==
             current_hash

    assert {:ok, %{state: :completed, result: {:ok, {:done, 2}}}} =
             Continuum.await(root, 2_000, journal: Postgres)
  end

  test "continue_as_new re-parents unawaited live children to the successor" do
    {:ok, root} = Continuum.start(OrphaningFlow, %{phase: 1}, journal: Postgres)

    pump_until(fn -> successor_of(root) != nil end)
    tip = successor_of(root)
    pump_until(fn -> run_state(tip) == "suspended" end)

    # The unawaited child now hangs off the successor, not the dead root.
    child =
      Repo.one(
        from(r in Run,
          where: r.parent_run_id == ^tip and is_nil(r.continued_from_run_id),
          select: r.id
        )
      )

    assert child != nil
    pump_until(fn -> run_state(child) == "suspended" end)

    # Cancelling through the chain root reaches the tip and cascades into the
    # re-parented child.
    assert :ok = Continuum.cancel(root, journal: Postgres)

    assert run_state(child) == "failed"

    assert Repo.one(from(r in Run, where: r.id == ^child, select: r.error)) |> decoded() ==
             :parent_cancelled
  end

  test "a signal sent to the chain root is delivered to the live tip" do
    {:ok, root} = Continuum.start(SignalCycleFlow, %{phase: 1}, journal: Postgres)

    pump_until(fn -> successor_of(root) != nil end)
    tip = successor_of(root)
    pump_until(fn -> run_state(tip) == "suspended" end)

    :ok = Continuum.signal(root, :go, :payload, journal: Postgres)
    pump_until(fn -> run_state(tip) == "completed" end)

    # The signal landed in the tip's mailbox, not the dead root's.
    assert Repo.aggregate(from(s in Signal, where: s.run_id == ^tip), :count) == 1
    assert Repo.aggregate(from(s in Signal, where: s.run_id == ^root), :count) == 0

    assert {:ok, %{state: :completed, result: {:ok, {:finished, :payload}}}} =
             Continuum.await(root, 2_000, journal: Postgres)
  end

  test "cancelling the chain root cancels the live tip" do
    {:ok, root} = Continuum.start(SignalCycleFlow, %{phase: 1}, journal: Postgres)

    pump_until(fn -> successor_of(root) != nil end)
    tip = successor_of(root)
    pump_until(fn -> run_state(tip) == "suspended" end)

    assert :ok = Continuum.cancel(root, journal: Postgres)

    assert run_state(tip) == "failed"

    assert Repo.one(from(r in Run, where: r.id == ^tip, select: r.error)) |> decoded() ==
             :cancelled

    # The root itself stays a completed {:continued, _} run.
    assert run_state(root) == "completed"
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

  defp decoded(nil), do: nil
  defp decoded(binary), do: :erlang.binary_to_term(binary)

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
