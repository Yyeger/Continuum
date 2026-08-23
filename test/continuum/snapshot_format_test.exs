defmodule Continuum.SnapshotFormatTest do
  use ExUnit.Case, async: true

  alias Continuum.Snapshot

  test "encodes snapshots in a versioned envelope" do
    snapshot = snapshot()
    encoded = Snapshot.encode(snapshot)

    assert {:continuum_snapshot, 1, ^snapshot} = :erlang.binary_to_term(encoded)
    assert Snapshot.decode(encoded) == snapshot
    assert Snapshot.format_version() == 1
  end

  test "decodes legacy unversioned v1 payloads" do
    snapshot = snapshot()
    legacy_payload = :erlang.term_to_binary(snapshot)

    assert Snapshot.decode(legacy_payload) == snapshot
  end

  test "raises a clear error for unsupported future versions" do
    payload = :erlang.term_to_binary({:continuum_snapshot, 99, %{}})

    assert_raise ArgumentError, ~r/snapshot format version 99 is not supported/, fn ->
      Snapshot.decode(payload)
    end
  end

  test "raises a clear error for non-snapshot payloads" do
    payload = :erlang.term_to_binary({:not_a_snapshot, 1, %{}})

    assert_raise ArgumentError, ~r/invalid Continuum snapshot payload/, fn ->
      Snapshot.decode(payload)
    end
  end

  test "compacts an append-only manual activity retry lineage into one replay step" do
    command_id = {:activity, __MODULE__, :run, 12, <<1>>, 0}
    mfa = {__MODULE__, :activity, [1]}

    events = [
      %{type: :activity_scheduled, seq: 0, mfa: mfa, command_id: command_id},
      %{type: :activity_failed, seq: 1, mfa: mfa, error: :down, command_id: command_id},
      %{type: :activity_retry_scheduled, seq: 2, mfa: mfa, command_id: command_id},
      %{type: :activity_completed, seq: 3, mfa: mfa, payload: {:ok, 1}, command_id: command_id}
    ]

    assert {:ok, %Snapshot{through_seq: 3, steps_by_seq: %{0 => step}}} =
             Snapshot.compact("retry-run", <<1>>, events)

    assert step.effect_type == :activity
    assert step.result == {:ok, 1}
    assert step.advance_by == 4
  end

  describe "activity_all batches" do
    test "compact into one step covering all 2N events" do
      assert {:ok, %Snapshot{through_seq: 5, steps_by_seq: steps}} =
               Snapshot.compact("batch-run", <<1>>, batch_events())

      assert map_size(steps) == 1
      assert %{0 => step} = steps
      assert step.effect_type == :activity_batch
      assert step.advance_by == 6
      assert step.command_ids == [command_id(0), command_id(1), command_id(2)]
      assert step.shape == List.duplicate({__MODULE__, :activity, 1}, 3)
      assert step.input_hashes == ["h0", "h1", "h2"]

      assert step.results == %{
               command_id(0) => {:ok, :a},
               command_id(1) => {:ok, :b},
               command_id(2) => {:ok, :c}
             }
    end

    test "compact the same way regardless of terminal arrival order" do
      {schedules, terminals} = Enum.split(batch_events(), 3)

      shuffled = schedules ++ resequence(Enum.reverse(terminals), 3)

      assert {:ok, %Snapshot{steps_by_seq: %{0 => step}}} =
               Snapshot.compact("batch-run", <<1>>, shuffled)

      assert step.results == %{
               command_id(0) => {:ok, :a},
               command_id(1) => {:ok, :b},
               command_id(2) => {:ok, :c}
             }
    end

    # The distinction that matters: a partial batch is `:incomplete`, which
    # stops compaction cleanly, not `:error`, which `compact_events/3`
    # propagates and which would stop snapshots for the run advancing forever.
    test "a prefix cut inside a batch skips rather than erroring" do
      for landed <- 0..2 do
        prefix = Enum.take(batch_events(), 3 + landed)

        assert {:skip, :no_complete_steps} = Snapshot.compact("batch-run", <<1>>, prefix)
      end
    end

    test "a batch followed by another step still compacts the later step" do
      tail = [
        %{type: :side_effect, seq: 6, kind: :now, payload: 7, command_id: command_id(9)}
      ]

      assert {:ok, %Snapshot{through_seq: 6, steps_by_seq: steps}} =
               Snapshot.compact("batch-run", <<1>>, batch_events() ++ tail)

      assert Map.keys(steps) |> Enum.sort() == [0, 6]
    end

    test "a terminal that belongs to no member of the batch is an error" do
      {schedules, terminals} = Enum.split(batch_events(), 3)

      foreign =
        terminals
        |> List.replace_at(1, %{
          type: :activity_completed,
          seq: 4,
          mfa: {__MODULE__, :activity, [1]},
          payload: {:ok, :foreign},
          command_id: command_id(99)
        })

      assert {:error, {:activity_batch_command_mismatch, 4}} =
               Snapshot.compact("batch-run", <<1>>, schedules ++ foreign)
    end
  end

  defp batch_events do
    mfa = {__MODULE__, :activity, [1]}

    [
      %{
        type: :activity_batch_scheduled,
        seq: 0,
        mfa: mfa,
        index: 0,
        input_hash: "h0",
        command_id: command_id(0)
      },
      %{
        type: :activity_batch_scheduled,
        seq: 1,
        mfa: mfa,
        index: 1,
        input_hash: "h1",
        command_id: command_id(1)
      },
      %{
        type: :activity_batch_scheduled,
        seq: 2,
        mfa: mfa,
        index: 2,
        input_hash: "h2",
        command_id: command_id(2)
      },
      %{
        type: :activity_completed,
        seq: 3,
        mfa: mfa,
        payload: {:ok, :a},
        command_id: command_id(0)
      },
      %{
        type: :activity_completed,
        seq: 4,
        mfa: mfa,
        payload: {:ok, :b},
        command_id: command_id(1)
      },
      %{
        type: :activity_completed,
        seq: 5,
        mfa: mfa,
        payload: {:ok, :c},
        command_id: command_id(2)
      }
    ]
  end

  defp resequence(events, offset) do
    events
    |> Enum.with_index(offset)
    |> Enum.map(fn {event, seq} -> Map.put(event, :seq, seq) end)
  end

  defp command_id(index), do: {:activity_batch, __MODULE__, :run, 12, <<1>>, index, 0}

  defp snapshot do
    %Snapshot{
      run_id: "snapshot-format-test",
      through_seq: 1,
      version_hash: <<1::256>>,
      taken_at: DateTime.from_naive!(~N[2026-05-31 00:00:00], "Etc/UTC"),
      steps_by_seq: %{
        0 => %{
          effect_type: :side_effect,
          command_id: {:side_effect, :test, 0},
          shape: :user,
          result: :ok,
          advance_by: 1
        }
      }
    }
  end
end
