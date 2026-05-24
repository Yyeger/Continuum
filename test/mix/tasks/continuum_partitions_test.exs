defmodule Mix.Tasks.Continuum.PartitionsTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres

  defmodule SomeWorkflow do
    @moduledoc false

    def __continuum_workflow__ do
      %{
        module: __MODULE__,
        entrypoint: __MODULE__,
        version: 1,
        version_hash: :crypto.hash(:sha256, "partitions-test")
      }
    end
  end

  setup do
    previous_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)

    on_exit(fn ->
      Mix.shell(previous_shell)
    end)
  end

  test "create builds monthly partitions idempotently" do
    month = "2030-01"
    partition = "continuum_events_y2030_m01"

    Mix.Task.rerun("continuum.partitions.create", [month])
    Mix.Task.rerun("continuum.partitions.create", [month])

    assert partition_exists?(partition)
  end

  test "list reports managed partitions and row counts" do
    Mix.Task.rerun("continuum.partitions.create", ["2030-02"])

    Mix.Task.rerun("continuum.partitions.list", [])

    assert_received {:mix_shell, :info, ["continuum_events_y2030_m02\t0"]}
  end

  test "parent inserts route events into the generated month partitions" do
    Mix.Task.rerun("continuum.partitions.create", ["2030-03"])
    Mix.Task.rerun("continuum.partitions.create", ["2030-04"])

    march_run = Ecto.UUID.generate()
    april_run = Ecto.UUID.generate()
    payload = :erlang.term_to_binary(%{type: :side_effect, kind: :user, payload: :ok})

    insert_event(march_run, 0, payload, ~U[2030-03-15 00:00:00Z])
    insert_event(april_run, 0, payload, ~U[2030-04-15 00:00:00Z])

    assert event_partition(march_run) == "continuum_events_y2030_m03"
    assert event_partition(april_run) == "continuum_events_y2030_m04"
  end

  test "drop_old is dry-run by default and drops only expired partitions when executed" do
    partition = "continuum_events_y2000_m01"
    Mix.Task.rerun("continuum.partitions.create", ["2000-01"])

    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

    Repo.update_all(
      from(r in Continuum.Schema.Run, where: r.id == ^run_id),
      set: [retention_until: ~U[2000-02-01 00:00:00Z]]
    )

    payload = :erlang.term_to_binary(%{type: :side_effect, kind: :user, payload: :expired})
    insert_event(run_id, 0, payload, ~U[2000-01-15 00:00:00Z])
    create_activity_results_fixture(run_id)
    flush_mix_shell()

    handler_id = "partition-drop-test-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :partition, :dropped],
        fn event, measurements, metadata, test_pid ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    Mix.Task.rerun("continuum.partitions.drop_old", [])
    assert partition_exists?(partition)
    assert_received {:mix_shell, :info, ["Would clean 1 activity_results rows"]}
    assert_received {:mix_shell, :info, [dry_run_message]}
    assert dry_run_message == "Would drop #{partition}"
    assert activity_results_count() == 1

    assert_received {:telemetry, [:continuum, :partition, :dropped], %{count: 1},
                     %{dry_run?: true}}

    Mix.Task.rerun("continuum.partitions.drop_old", ["--execute"])
    refute partition_exists?(partition)
    assert_received {:mix_shell, :info, ["Cleaned 1 activity_results rows"]}
    assert_received {:mix_shell, :info, [drop_message]}
    assert drop_message == "Dropped #{partition}"
    assert activity_results_count() == 0

    assert_received {:telemetry, [:continuum, :partition, :dropped], %{count: 1},
                     %{dry_run?: false}}
  end

  defp insert_event(run_id, seq, payload, inserted_at) do
    Repo.query!(
      """
      INSERT INTO continuum_events (run_id, seq, event_type, payload, inserted_at)
      VALUES ($1, $2, 'side_effect', $3, $4::timestamptz)
      """,
      [dump_uuid(run_id), seq, payload, inserted_at]
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

  defp partition_exists?(partition) do
    %{rows: [[name]]} = Repo.query!("SELECT to_regclass($1)::text", [partition])
    name == partition
  end

  defp create_activity_results_fixture(run_id) do
    unless table_exists?("continuum_activity_results") do
      Repo.query!("""
      CREATE TABLE continuum_activity_results (
        activity_module text NOT NULL,
        idempotency_key text NOT NULL,
        run_id uuid NOT NULL,
        seq bigint NOT NULL,
        result bytea NOT NULL,
        completed_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (activity_module, idempotency_key)
      )
      """)
    end

    Repo.query!(
      """
      INSERT INTO continuum_activity_results
        (activity_module, idempotency_key, run_id, seq, result)
      VALUES ('SomeActivity', 'key-1', $1, 0, $2)
      """,
      [dump_uuid(run_id), :erlang.term_to_binary(:ok)]
    )
  end

  defp table_exists?(table) do
    %{rows: [[exists?]]} = Repo.query!("SELECT to_regclass($1) IS NOT NULL", [table])
    exists?
  end

  defp activity_results_count do
    %{rows: [[count]]} = Repo.query!("SELECT count(*) FROM continuum_activity_results")
    count
  end

  defp dump_uuid(run_id) do
    {:ok, dumped} = Ecto.UUID.dump(run_id)
    dumped
  end

  defp flush_mix_shell do
    receive do
      {:mix_shell, _, _} -> flush_mix_shell()
    after
      0 -> :ok
    end
  end
end
