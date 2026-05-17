defmodule ContinuumExampleOrders.Repo.Migrations.CreateContinuumTables do
  use Ecto.Migration

  def up do
    execute("CREATE SEQUENCE IF NOT EXISTS continuum_lease_token_seq")

    create table(:continuum_runs, primary_key: false, options: "WITH (fillfactor = 70)") do
      add(:id, :uuid, primary_key: true)
      add(:workflow, :text, null: false)
      add(:version_hash, :bytea, null: false)
      add(:state, :text, null: false)
      add(:input, :bytea, null: false)
      add(:result, :bytea)
      add(:error, :bytea)
      add(:trace_context, :bytea)
      add(:started_at, :utc_datetime_usec, null: false, default: fragment("now()"))
      add(:completed_at, :utc_datetime_usec)
      add(:lease_owner, :text)
      add(:lease_token, :bigint)
      add(:lease_expires_at, :utc_datetime_usec)
      add(:next_wakeup_at, :utc_datetime_usec)
      add(:retention_until, :utc_datetime_usec)
    end

    execute("""
    CREATE INDEX continuum_runs_dispatch_idx
      ON continuum_runs (next_wakeup_at NULLS LAST)
      WHERE state = 'suspended' AND lease_owner IS NULL
    """)

    execute("""
    CREATE INDEX continuum_runs_lease_idx
      ON continuum_runs (lease_expires_at)
      WHERE lease_owner IS NOT NULL
    """)

    execute("""
    CREATE TABLE continuum_events (
      run_id uuid NOT NULL,
      seq bigint NOT NULL,
      event_type text NOT NULL,
      payload bytea NOT NULL,
      inserted_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (run_id, seq, inserted_at)
    ) PARTITION BY RANGE (inserted_at)
    """)

    create_initial_event_partitions()

    create table(:continuum_signals) do
      add(:run_id, :uuid, null: false)
      add(:name, :text, null: false)
      add(:payload, :bytea, null: false)
      add(:delivered, :boolean, null: false, default: false)
      add(:inserted_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    execute("""
    CREATE INDEX continuum_signals_pending_idx
      ON continuum_signals (run_id, name)
      WHERE delivered = false
    """)

    create table(:continuum_timers, primary_key: false) do
      add(:id, :uuid, primary_key: true)
      add(:run_id, :uuid, null: false)
      add(:fires_at, :utc_datetime_usec, null: false)
      add(:fired, :boolean, null: false, default: false)
    end

    execute("""
    CREATE INDEX continuum_timers_due_idx
      ON continuum_timers (fires_at)
      WHERE fired = false
    """)

    create table(:continuum_activity_tasks, primary_key: false) do
      add(:id, :uuid, primary_key: true)
      add(:run_id, :uuid, null: false)
      add(:seq, :bigint, null: false)
      add(:mfa, :bytea, null: false)
      add(:attempt, :integer, null: false, default: 1)
      add(:state, :text, null: false)
      add(:scheduled_at, :utc_datetime_usec, null: false, default: fragment("now()"))
      add(:available_at, :utc_datetime_usec, null: false, default: fragment("now()"))
      add(:lease_owner, :text)
      add(:lease_expires_at, :utc_datetime_usec)
      add(:result, :bytea)
      add(:error, :bytea)
    end

    execute("""
    CREATE INDEX continuum_activity_tasks_pickup_idx
      ON continuum_activity_tasks (available_at)
      WHERE state = 'available'
    """)

    create table(:continuum_activity_results, primary_key: false) do
      add(:activity_module, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:run_id, :uuid, null: false)
      add(:seq, :bigint, null: false)
      add(:result, :bytea, null: false)
      add(:completed_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    execute("""
    ALTER TABLE continuum_activity_results
      ADD PRIMARY KEY (activity_module, idempotency_key)
    """)

    create table(:continuum_snapshots) do
      add(:run_id, :uuid, null: false)
      add(:through_seq, :bigint, null: false)
      add(:version_hash, :bytea, null: false)
      add(:payload, :bytea, null: false)
      add(:taken_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    create(
      unique_index(:continuum_snapshots, [:run_id, :through_seq],
        name: :continuum_snapshots_run_seq_idx
      )
    )

    execute("""
    CREATE INDEX continuum_snapshots_latest_idx
      ON continuum_snapshots (run_id, through_seq DESC)
    """)
  end

  def down do
    drop_if_exists(table(:continuum_snapshots))
    drop_if_exists(table(:continuum_activity_results))
    drop_if_exists(table(:continuum_activity_tasks))
    drop_if_exists(table(:continuum_timers))
    drop_if_exists(table(:continuum_signals))
    drop_if_exists(table(:continuum_events))
    drop_if_exists(table(:continuum_runs))
    execute("DROP SEQUENCE IF EXISTS continuum_lease_token_seq")
  end

  defp create_initial_event_partitions do
    today = Date.utc_today()
    month = Date.new!(today.year, today.month, 1)

    for offset <- 0..3 do
      create_event_partition(Date.add(month, offset * 32) |> Date.beginning_of_month())
    end
  end

  defp create_event_partition(month) do
    next_month = month |> Date.add(32) |> Date.beginning_of_month()

    execute("""
    CREATE TABLE IF NOT EXISTS #{event_partition_name(month)}
    PARTITION OF continuum_events
    FOR VALUES FROM ('#{Date.to_iso8601(month)} 00:00:00+00')
    TO ('#{Date.to_iso8601(next_month)} 00:00:00+00')
    """)
  end

  defp event_partition_name(%Date{year: year, month: month}) do
    "continuum_events_y#{year}_m#{pad2(month)}"
  end

  defp pad2(month) when month < 10, do: "0#{month}"
  defp pad2(month), do: "#{month}"
end
