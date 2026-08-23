defmodule Continuum.ActivityAllTest do
  @moduledoc """
  `activity_all/1`: parallel fan-out with results in declared order.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}
  alias Continuum.Test

  defmodule Pricing do
    use Continuum.Activity, retry: [max_attempts: 1]

    def quote_for(%{id: id, cents: cents}), do: {:ok, %{id: id, cents: cents}}
    def fail(%{id: id}), do: raise("no quote for #{id}")
  end

  defmodule Inventory do
    use Continuum.Activity, retry: [max_attempts: 1]

    def reserve(sku), do: {:ok, "hold_#{sku}"}
  end

  defmodule FanOutFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      {:ok,
       activity_all([
         Pricing.quote_for(%{id: :a, cents: input.a}),
         Pricing.quote_for(%{id: :b, cents: input.b}),
         Inventory.reserve(input.sku)
       ])}
    end
  end

  defmodule PartialFailureFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      {:ok,
       activity_all([
         Pricing.quote_for(%{id: :ok1, cents: 1}),
         Pricing.fail(%{id: :boom}),
         Pricing.quote_for(%{id: :ok2, cents: 2})
       ])}
    end
  end

  defmodule SameMfaFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      {:ok,
       activity_all([
         Pricing.quote_for(%{id: :first, cents: 1}),
         Pricing.quote_for(%{id: :second, cents: 2}),
         Pricing.quote_for(%{id: :third, cents: 3})
       ])}
    end
  end

  setup do
    Test.reset_in_memory!()
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  describe "in memory" do
    test "returns results in declared order" do
      {:ok, run_id} = Test.start_synchronous(FanOutFlow, %{a: 100, b: 200, sku: "s-1"})

      assert {:ok, %{state: :completed, result: {:ok, results}}} = Continuum.await(run_id, 1_000)

      assert results == [
               {:ok, %{id: :a, cents: 100}},
               {:ok, %{id: :b, cents: 200}},
               {:ok, "hold_s-1"}
             ]
    end

    test "a failing member contributes an error entry and the rest still land" do
      {:ok, run_id} = Test.start_synchronous(PartialFailureFlow, %{})

      assert {:ok, %{state: :completed, result: {:ok, results}}} = Continuum.await(run_id, 1_000)

      assert [{:ok, %{id: :ok1}}, {:error, _}, {:ok, %{id: :ok2}}] = results
    end

    test "journals a schedule per member and then a terminal per member" do
      {:ok, run_id} = Test.start_synchronous(FanOutFlow, %{a: 1, b: 2, sku: "s"})
      {:ok, _} = Continuum.await(run_id, 1_000)

      assert Enum.map(Test.history(run_id), & &1.type) == [
               :activity_batch_scheduled,
               :activity_batch_scheduled,
               :activity_batch_scheduled,
               :activity_completed,
               :activity_completed,
               :activity_completed
             ]
    end
  end

  describe "durable" do
    test "schedules the whole batch under one lock and completes out of order" do
      {:ok, run_id} =
        Continuum.start(FanOutFlow, %{a: 100, b: 200, sku: "s-1"}, journal: Postgres)

      assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 3 end)

      assert Enum.map(events(run_id), & &1.event_type) == [
               "activity_batch_scheduled",
               "activity_batch_scheduled",
               "activity_batch_scheduled"
             ]

      assert {:ok, %{state: :completed, result: {:ok, results}}} = Test.drive(run_id)

      assert results == [
               {:ok, %{id: :a, cents: 100}},
               {:ok, %{id: :b, cents: 200}},
               {:ok, "hold_s-1"}
             ]
    end

    test "three identical-MFA members keep their own results" do
      {:ok, run_id} = Continuum.start(SameMfaFlow, %{}, journal: Postgres)

      assert {:ok, %{state: :completed, result: {:ok, results}}} = Test.drive(run_id)

      assert results == [
               {:ok, %{id: :first, cents: 1}},
               {:ok, %{id: :second, cents: 2}},
               {:ok, %{id: :third, cents: 3}}
             ]
    end

    test "replays the journaled history to the same results" do
      {:ok, run_id} = Continuum.start(SameMfaFlow, %{}, journal: Postgres)
      {:ok, %{result: expected}} = Test.drive(run_id)

      history = Postgres.load(Instance.default(), run_id)

      assert {:ok, ^expected} = Continuum.Test.replay(SameMfaFlow, %{}, history)
    end

    test "a partly completed batch suspends without advancing the cursor" do
      {:ok, run_id} = Continuum.start(SameMfaFlow, %{}, journal: Postgres)
      assert_eventually(fn -> Repo.aggregate(ActivityTask, :count) == 3 end)

      history = Postgres.load(Instance.default(), run_id)
      partial = history ++ [terminal_for(history, 1)]

      assert {:suspended, {:activity_batch_pending, _}} =
               Continuum.Test.replay(SameMfaFlow, %{}, partial)
    end
  end

  describe "snapshot replay" do
    test "replays a batch out of a forged snapshot step" do
      {:ok, run_id} = Test.start_synchronous(SameMfaFlow, %{})
      {:ok, %{result: expected}} = Continuum.await(run_id, 1_000)

      history = Test.history(run_id)
      hash = SameMfaFlow.__continuum_workflow__().version_hash

      assert {:ok, snapshot} = Continuum.Snapshot.compact(run_id, hash, history)
      assert map_size(snapshot.steps_by_seq) == 1

      remaining = Enum.drop(history, snapshot.through_seq + 1)

      assert {:ok, ^expected} =
               Continuum.Test.replay(SameMfaFlow, %{}, remaining, snapshot: snapshot)
    end

    # The event path and the snapshot path must agree, which is the rule
    # `CLAUDE.md` records from the v0.3 `patched?/1` review. Here the step's
    # command ids no longer describe the batch the code asks for, so the
    # snapshot path must raise rather than hand back plausible results.
    test "raises drift when the snapshot step does not describe this batch" do
      {:ok, run_id} = Test.start_synchronous(SameMfaFlow, %{})
      {:ok, _} = Continuum.await(run_id, 1_000)

      history = Test.history(run_id)
      hash = SameMfaFlow.__continuum_workflow__().version_hash
      {:ok, snapshot} = Continuum.Snapshot.compact(run_id, hash, history)

      forged = update_in(snapshot.steps_by_seq[0].command_ids, &Enum.reverse/1)
      remaining = Enum.drop(history, snapshot.through_seq + 1)

      assert {:error, {:error, %Continuum.ReplayDriftError{}, _stack}} =
               Continuum.Replay.run(SameMfaFlow, %{}, remaining, snapshot: forged)
    end

    test "raises drift when batch arguments change behind a snapshot" do
      original = %{a: 100, b: 200, sku: "original"}
      changed = %{a: 999, b: 200, sku: "changed"}

      {:ok, run_id} = Test.start_synchronous(FanOutFlow, original)
      {:ok, _} = Continuum.await(run_id, 1_000)

      history = Test.history(run_id)
      hash = FanOutFlow.__continuum_workflow__().version_hash
      {:ok, snapshot} = Continuum.Snapshot.compact(run_id, hash, history)
      remaining = Enum.drop(history, snapshot.through_seq + 1)

      assert {:error, {:error, %Continuum.ReplayDriftError{}, _stack}} =
               Continuum.Replay.run(FanOutFlow, changed, remaining, snapshot: snapshot)
    end

    test "raises drift when a non-batch step sits where the batch does" do
      {:ok, run_id} = Test.start_synchronous(SameMfaFlow, %{})
      {:ok, _} = Continuum.await(run_id, 1_000)

      history = Test.history(run_id)
      hash = SameMfaFlow.__continuum_workflow__().version_hash
      {:ok, snapshot} = Continuum.Snapshot.compact(run_id, hash, history)

      forged = put_in(snapshot.steps_by_seq[0].effect_type, :activity)
      remaining = Enum.drop(history, snapshot.through_seq + 1)

      assert {:error, {:error, %Continuum.ReplayDriftError{}, _stack}} =
               Continuum.Replay.run(SameMfaFlow, %{}, remaining, snapshot: forged)
    end
  end

  describe "the macro" do
    test "refuses a non-literal list, because identity is computed at expansion" do
      assert_raise ArgumentError, ~r/literal list of activity calls/, fn ->
        defmodule DynamicFlow do
          use Continuum.Workflow, version: 1

          def run(calls), do: activity_all(calls)
        end
      end
    end

    test "refuses an element that is not an activity call" do
      assert_raise ArgumentError, ~r/each element to be an activity call/, fn ->
        defmodule BadElementFlow do
          use Continuum.Workflow, version: 1

          def run(_input), do: activity_all([:not_a_call])
        end
      end
    end

    test "refuses an empty batch" do
      assert_raise ArgumentError, ~r/at least one activity call/, fn ->
        defmodule EmptyFlow do
          use Continuum.Workflow, version: 1

          def run(_input), do: activity_all([])
        end
      end
    end
  end

  defp terminal_for(history, index) do
    schedule = Enum.at(Enum.filter(history, &(&1.type == :activity_batch_scheduled)), index)

    %{
      type: :activity_completed,
      mfa: schedule.mfa,
      payload: {:ok, :partial},
      command_id: schedule.command_id,
      seq: length(history)
    }
  end

  defp events(run_id) do
    Repo.all(from(e in Event, where: e.run_id == ^run_id, order_by: [asc: e.seq]))
  end

  defp assert_eventually(fun, attempts \\ 200)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
