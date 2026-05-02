defmodule Continuum.Test.Repo.Migrations.CreateContinuumTables do
  use Ecto.Migration

  def up do
    execute "CREATE SEQUENCE IF NOT EXISTS continuum_lease_token_seq"

    create table(:continuum_runs, primary_key: false, options: "WITH (fillfactor = 70)") do
      add :id, :uuid, primary_key: true
      add :workflow, :text, null: false
      add :version_hash, :bytea, null: false
      add :state, :text, null: false
      add :input, :jsonb, null: false
      add :result, :jsonb
      add :error, :jsonb
      add :started_at, :utc_datetime_usec, null: false, default: fragment("now()")
      add :completed_at, :utc_datetime_usec
      add :lease_owner, :text
      add :lease_token, :bigint
      add :lease_expires_at, :utc_datetime_usec
      add :next_wakeup_at, :utc_datetime_usec
      add :retention_until, :utc_datetime_usec
    end

    execute """
    CREATE INDEX continuum_runs_dispatch_idx
      ON continuum_runs (next_wakeup_at NULLS LAST)
      WHERE state = 'suspended' AND lease_owner IS NULL
    """

    execute """
    CREATE INDEX continuum_runs_lease_idx
      ON continuum_runs (lease_expires_at)
      WHERE lease_owner IS NOT NULL
    """

    # Events: append-only history.
    # In production, consider partitioning by month — see plan.
    create table(:continuum_events, primary_key: false) do
      add :run_id, :uuid, null: false
      add :seq, :bigint, null: false
      add :event_type, :text, null: false
      add :payload, :jsonb, null: false
      add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    execute "ALTER TABLE continuum_events ADD PRIMARY KEY (run_id, seq)"

    create table(:continuum_signals) do
      add :run_id, :uuid, null: false
      add :name, :text, null: false
      add :payload, :jsonb, null: false
      add :delivered, :boolean, null: false, default: false
      add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    execute """
    CREATE INDEX continuum_signals_pending_idx
      ON continuum_signals (run_id, name)
      WHERE delivered = false
    """

    create table(:continuum_timers, primary_key: false) do
      add :id, :uuid, primary_key: true
      add :run_id, :uuid, null: false
      add :fires_at, :utc_datetime_usec, null: false
      add :fired, :boolean, null: false, default: false
    end

    execute """
    CREATE INDEX continuum_timers_due_idx
      ON continuum_timers (fires_at)
      WHERE fired = false
    """

    create table(:continuum_activity_tasks, primary_key: false) do
      add :id, :uuid, primary_key: true
      add :run_id, :uuid, null: false
      add :seq, :bigint, null: false
      add :mfa, :jsonb, null: false
      add :attempt, :integer, null: false, default: 1
      add :state, :text, null: false
      add :scheduled_at, :utc_datetime_usec, null: false, default: fragment("now()")
      add :available_at, :utc_datetime_usec, null: false, default: fragment("now()")
      add :lease_owner, :text
      add :lease_expires_at, :utc_datetime_usec
      add :result, :jsonb
      add :error, :jsonb
    end

    execute """
    CREATE INDEX continuum_activity_tasks_pickup_idx
      ON continuum_activity_tasks (available_at)
      WHERE state = 'available'
    """
  end

  def down do
    drop_if_exists table(:continuum_activity_tasks)
    drop_if_exists table(:continuum_timers)
    drop_if_exists table(:continuum_signals)
    drop_if_exists table(:continuum_events)
    drop_if_exists table(:continuum_runs)
    execute "DROP SEQUENCE IF EXISTS continuum_lease_token_seq"
  end
end
