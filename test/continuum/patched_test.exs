defmodule Continuum.PatchedTest do
  @moduledoc """
  Real `Continuum.patched?/1` — journaled patch decisions (V0.3 PR 3, §3.4).
  """

  use ExUnit.Case, async: false

  alias Continuum.Runtime.{Context, Effect, Instance}
  alias Continuum.Runtime.Journal.InMemory

  defmodule PatchedBranchFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      if Continuum.patched?(:feature) do
        {:ok, :new_branch}
      else
        {:ok, :old_branch}
      end
    end
  end

  defmodule PatchedTailFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      p = Continuum.patched?(:feature)
      s = Continuum.side_effect(fn -> :tail end)
      {:ok, {p, s}}
    end
  end

  setup do
    Continuum.Test.reset_in_memory!()
    :ok
  end

  test "a fresh run journals patched=true and replays the journaled value" do
    {:ok, run_id} = Continuum.Test.start_synchronous(PatchedBranchFlow, %{})

    assert {:ok, %{state: :completed, result: {:ok, :new_branch}}} =
             Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)

    assert [%{type: :patched, patch_name: :feature, value: true, command_id: command_id}] =
             history

    assert is_tuple(command_id) and tuple_size(command_id) >= 5

    # Replaying the journaled history reproduces the same branch without
    # re-deciding the patch.
    assert {:ok, {:ok, :new_branch}} = Continuum.Test.replay(PatchedBranchFlow, %{}, history)
  end

  test "an old run on the pre-patch branch returns false without advancing the cursor" do
    # Hand-crafted "old" history: recorded before the patch line existed, so its
    # next event is an ordinary effect, not a `patched` marker.
    events = [
      %{
        type: :side_effect,
        kind: :user,
        payload: :old,
        command_id: {:side_effect, :user, {:old_site}, "hash", 0},
        seq: 0
      }
    ]

    ctx = forge_ctx("patched-old", events)
    Context.put(ctx)

    try do
      command = {:side_effect, __MODULE__, {:run, 1}, 10, "patch-site"}
      assert false == Effect.run({:patched, :feature}, {:command, command})
      # The pre-patch event was NOT consumed.
      assert Context.get().cursor == 0
    after
      Context.clear()
    end
  end

  test "two patched?/1 calls at distinct command_ids each return false without consuming a downstream event" do
    events = [
      %{
        type: :side_effect,
        kind: :user,
        payload: :old,
        command_id: {:side_effect, :user, {:old_site}, "hash", 0},
        seq: 0
      }
    ]

    ctx = forge_ctx("patched-two", events)
    Context.put(ctx)

    try do
      base_a = {:side_effect, __MODULE__, {:run, 1}, 11, "patch-a"}
      base_b = {:side_effect, __MODULE__, {:run, 1}, 12, "patch-b"}

      assert false == Effect.run({:patched, :a}, {:command, base_a})
      assert false == Effect.run({:patched, :b}, {:command, base_b})
      assert Context.get().cursor == 0

      # The downstream effect still consumes the single recorded event.
      assert :old ==
               Effect.run(
                 {:side_effect, :user},
                 {:command, {:side_effect, :user, {:old_site}, "hash"},
                  fn ->
                    raise "producer must not run on replay"
                  end}
               )

      assert Context.get().cursor == 1
    after
      Context.clear()
    end
  end

  test "a compacted patched marker for another command_id returns false without advancing" do
    old_marker_command = {:patched, __MODULE__, {:run, 1}, 9, "old-patch", 0}

    ctx =
      forge_ctx("patched-snapshot-miss", [])
      |> Map.put(:history_offset, 1)
      |> Map.put(:snapshot_steps, %{
        0 => %{
          effect_type: :patched,
          command_id: old_marker_command,
          shape: :old_feature,
          result: true,
          advance_by: 1
        }
      })

    Context.put(ctx)

    try do
      new_marker_base = {:patched, __MODULE__, {:run, 1}, 10, "new-patch"}
      assert false == Effect.run({:patched, :new_feature}, {:command, new_marker_base})
      assert Context.get().cursor == 0
    after
      Context.clear()
    end
  end

  test "a compacted patched marker for the same command_id but different patch name drifts" do
    command = {:patched, __MODULE__, {:run, 1}, 9, "patch", 0}

    ctx =
      forge_ctx("patched-snapshot-drift", [])
      |> Map.put(:history_offset, 1)
      |> Map.put(:snapshot_steps, %{
        0 => %{
          effect_type: :patched,
          command_id: command,
          shape: :old_feature,
          result: true,
          advance_by: 1
        }
      })

    Context.put(ctx)

    try do
      base = {:patched, __MODULE__, {:run, 1}, 9, "patch"}

      assert_raise Continuum.ReplayDriftError, fn ->
        Effect.run({:patched, :new_feature}, {:command, base})
      end
    after
      Context.clear()
    end
  end

  test "patched? at the live tail journals value true exactly once" do
    ctx = forge_ctx("patched-live", [])
    Context.put(ctx)

    try do
      command = {:side_effect, __MODULE__, {:run, 1}, 1, "live-site"}
      assert true == Effect.run({:patched, :feature}, {:command, command})
      assert Context.get().cursor == 1
    after
      Context.clear()
    end

    assert [%{type: :patched, patch_name: :feature, value: true}] =
             InMemory.load(Instance.default(), "patched-live")
  end

  test "tampering with a journaled patched event surfaces as replay drift" do
    {:ok, run_id} = Continuum.Test.start_synchronous(PatchedTailFlow, %{})

    assert {:ok, %{state: :completed, result: {:ok, {true, :tail}}}} =
             Continuum.await(run_id, 1_000)

    history = Continuum.Test.history(run_id)
    assert [%{type: :patched} = patched, %{type: :side_effect}] = history

    tampered = [%{patched | command_id: {:tampered}} | tl(history)]

    ctx = forge_ctx("patched-drift", tampered)
    Context.put(ctx)

    try do
      assert_raise Continuum.ReplayDriftError, fn ->
        PatchedTailFlow.run(%{})
      end
    after
      Context.clear()
    end
  end

  defp forge_ctx(run_id, history) do
    %Context{
      run_id: run_id,
      history: history,
      cursor: 0,
      workflow_module: __MODULE__,
      lease_token: nil,
      instance: Instance.default(),
      journal: InMemory,
      command_counts: %{}
    }
  end
end
