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
