defmodule Continuum.PartitionsTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Partitions
  alias Continuum.Runtime.{Instance, PartitionMaintainer}

  @january ~D[2041-01-01]
  @january_partition "continuum_events_y2041_m01"
  @february_partition "continuum_events_y2041_m02"

  test "ensure creates a future horizon idempotently and emits maintenance telemetry" do
    drop_partition(@january_partition)
    drop_partition(@february_partition)
    test_pid = self()
    handler_id = "partition-maintained-test-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :partition, :maintained],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, plan} = Partitions.plan(repo: Repo, start_month: @january, months: 2)
    assert plan.missing == [@january_partition, @february_partition]
    assert plan.default_present?

    assert {:ok, summary} =
             Partitions.ensure(repo: Repo, start_month: @january, months: 2)

    assert summary.status == :ok
    assert summary.created == [@january_partition, @february_partition]
    assert summary.existing == []
    assert summary.moved_row_count == 0
    assert summary.default_partition == "continuum_events_default"
    assert partition_exists?(@january_partition)
    assert partition_exists?(@february_partition)

    assert_receive {:telemetry, [:continuum, :partition, :maintained],
                    %{created_count: 2, moved_row_count: 0},
                    %{status: :ok, created: [@january_partition, @february_partition]}}

    assert {:ok, repeated} =
             Partitions.ensure(repo: Repo, start_month: @january, months: 2)

    assert repeated.created == []
    assert repeated.existing == [@january_partition, @february_partition]
  end

  test "ensure transactionally evacuates matching rows from the default partition" do
    drop_partition(@january_partition)
    run_id = Ecto.UUID.generate()
    insert_event(run_id, ~U[2041-01-15 12:00:00Z])

    assert event_partition(run_id) == "continuum_events_default"

    assert {:ok, summary} =
             Partitions.ensure(repo: Repo, start_month: @january, months: 1)

    assert summary.created == [@january_partition]
    assert summary.moved_row_count == 1
    assert event_partition(run_id) == @january_partition
    assert default_row_count() == 0
  end

  test "overflow rows degrade shared health until their month is maintained" do
    march = ~D[2042-03-01]
    march_partition = "continuum_events_y2042_m03"
    drop_partition(march_partition)
    run_id = Ecto.UUID.generate()
    insert_event(run_id, ~U[2042-03-12 00:00:00Z])

    assert {:ok, report} = Continuum.Health.report(repo: Repo, partition_months: 1)
    assert report.partitions.default_partition.present?
    assert report.partitions.default_partition.row_count == 1

    assert DateTime.compare(
             report.partitions.default_partition.oldest_inserted_at,
             ~U[2042-03-12 00:00:00Z]
           ) == :eq

    assert report.partitions.status == :degraded
    assert report.status == :degraded

    assert {:ok, %{moved_row_count: 1}} =
             Partitions.ensure(repo: Repo, start_month: march, months: 1)

    assert {:ok, repaired_report} = Continuum.Health.report(repo: Repo, partition_months: 1)
    assert repaired_report.partitions.default_partition.row_count == 0
  end

  test "missing default safety partition degrades health and ensure restores it" do
    Repo.query!("DROP TABLE continuum_events_default")

    assert {:ok, report} = Continuum.Health.report(repo: Repo, partition_months: 1)
    refute report.partitions.default_partition.present?
    assert report.partitions.status == :degraded

    assert {:ok, %{default_created?: true, default_partition: "continuum_events_default"}} =
             Partitions.ensure(repo: Repo, start_month: @january, months: 1)

    assert partition_exists?("continuum_events_default")
  end

  test "the optional maintainer schedules and exposes successful state" do
    april = ~D[2043-04-01]
    april_partition = "continuum_events_y2043_m04"
    drop_partition(april_partition)

    name = :partition_maintainer_test

    instance =
      Instance.new(name: name, repo: Repo)
      |> Instance.register()

    start_supervised!(
      {PartitionMaintainer,
       instance: instance, months: 1, start_month: april, initial_delay_ms: 0, interval_ms: 60_000}
    )

    assert_eventually(fn -> PartitionMaintainer.status(instance).state == :ready end)

    assert %{
             state: :ready,
             months: 1,
             last_result: %{created: [^april_partition]},
             last_error: nil,
             last_run_at: %DateTime{}
           } = PartitionMaintainer.status(instance)

    assert {:ok, %{created: [], existing: [^april_partition]}} =
             PartitionMaintainer.maintain_once(instance)
  end

  test "invalid horizons and start months fail before maintenance" do
    assert {:error, {:invalid_horizon_months, 0}} = Partitions.ensure(repo: Repo, months: 0)

    assert {:error, {:start_month_must_be_first_day, ~D[2041-01-02]}} =
             Partitions.plan(repo: Repo, start_month: ~D[2041-01-02])

    assert {:error, {:invalid_lock_mode, :never}} =
             Partitions.ensure(repo: Repo, lock: :never)
  end

  defp insert_event(run_id, inserted_at) do
    Repo.query!(
      """
      INSERT INTO continuum_events (run_id, seq, event_type, payload, inserted_at)
      VALUES ($1, 0, 'side_effect', $2, $3::timestamptz)
      """,
      [dump_uuid(run_id), :erlang.term_to_binary(%{payload: :overflow}), inserted_at]
    )
  end

  defp event_partition(run_id) do
    %{rows: [[partition]]} =
      Repo.query!(
        """
        SELECT tableoid::regclass::text
        FROM continuum_events
        WHERE run_id = $1
        """,
        [dump_uuid(run_id)]
      )

    partition
  end

  defp default_row_count do
    %{rows: [[count]]} =
      Repo.query!("SELECT count(*) FROM ONLY continuum_events_default")

    count
  end

  defp partition_exists?(partition) do
    %{rows: [[exists?]]} =
      Repo.query!("SELECT to_regclass($1) IS NOT NULL", [partition])

    exists?
  end

  defp drop_partition(partition) do
    Repo.query!("DROP TABLE IF EXISTS \"#{partition}\"")
  end

  defp dump_uuid(run_id) do
    {:ok, dumped} = Ecto.UUID.dump(run_id)
    dumped
  end

  defp assert_eventually(fun, attempts \\ 40)

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
