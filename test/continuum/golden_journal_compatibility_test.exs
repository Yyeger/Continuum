defmodule Continuum.GoldenJournalCompatibilityTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.TestSupport.GoldenJournalFixtures

  @fixture_dir Path.join(["test", "fixtures", "journals"])

  test "all expected golden journal fixtures are committed" do
    committed =
      @fixture_dir
      |> Path.join("*.journal")
      |> Path.wildcard()
      |> Enum.map(&Path.basename(&1, ".journal"))
      |> Enum.sort()

    assert committed == Enum.sort(GoldenJournalFixtures.fixture_names())
  end

  test "committed histories replay without drift" do
    for name <- GoldenJournalFixtures.fixture_names() do
      fixture = GoldenJournalFixtures.load!(name)

      assert fixture.schema_version == 1
      assert fixture.continuum_version == Mix.Project.config()[:version]
      assert fixture.name == name
      assert File.exists?(GoldenJournalFixtures.fixture_path(name))

      assert Enum.map(fixture.history, & &1.type) == fixture.expected_event_types
      assert command_ids(fixture.history) == fixture.expected_command_ids
      assert Enum.all?(fixture.expected_command_ids, &valid_command_id?/1)

      assert_replay_contract(fixture)
    end
  end

  defp assert_replay_contract(%{replay: :ok} = fixture) do
    opts = replay_opts(fixture)

    assert {:ok, result} =
             Continuum.Test.replay(fixture.workflow_module, fixture.input, fixture.history, opts)

    assert result == fixture.expected_result
  end

  defp assert_replay_contract(%{replay: :continued} = fixture) do
    assert {:continued, next_run_id} =
             Continuum.Test.replay(fixture.workflow_module, fixture.input, fixture.history)

    assert is_binary(next_run_id)
    assert fixture.run_metadata.chain_length == 3
  end

  defp assert_replay_contract(%{replay: :suspended} = fixture) do
    assert {:suspended, _reason} =
             Continuum.Test.replay(
               fixture.workflow_module,
               fixture.input,
               fixture.history,
               replay_opts(fixture)
             )

    assert fixture.expected_terminal_state == :failed
  end

  defp assert_replay_contract(%{replay: :metadata_only} = fixture) do
    assert is_map(fixture.run_metadata)
    assert Map.has_key?(fixture.run_metadata, :terminal_state)
  end

  defp replay_opts(fixture) do
    fixture
    |> snapshot_opt()
    |> journal_opt(fixture)
  end

  defp snapshot_opt(fixture) do
    case Map.fetch(fixture, :snapshot) do
      {:ok, snapshot} -> [snapshot: snapshot]
      :error -> []
    end
  end

  defp journal_opt(opts, fixture) do
    case Map.fetch(fixture, :replay_journal) do
      {:ok, journal} -> Keyword.put(opts, :journal, journal)
      :error -> opts
    end
  end

  defp command_ids(history) do
    history
    |> Enum.filter(&Map.has_key?(&1, :command_id))
    |> Enum.map(& &1.command_id)
  end

  defp valid_command_id?(command_id) when is_tuple(command_id), do: tuple_size(command_id) >= 5
  defp valid_command_id?(_command_id), do: false
end
