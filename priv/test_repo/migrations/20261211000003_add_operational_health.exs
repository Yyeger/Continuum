defmodule Continuum.Test.Repo.Migrations.AddOperationalHealth do
  use Ecto.Migration

  def up do
    alter table(:continuum_runs) do
      add(:lease_acquired_at, :utc_datetime_usec)
      add(:lease_heartbeat_at, :utc_datetime_usec)
    end

    execute("""
    UPDATE continuum_runs
    SET lease_acquired_at = COALESCE(lease_acquired_at, started_at),
        lease_heartbeat_at = COALESCE(lease_heartbeat_at, lease_expires_at - interval '30 seconds')
    WHERE lease_owner IS NOT NULL
    """)

    create table(:continuum_health_reviews, primary_key: false) do
      add(:finding_type, :text, null: false)
      add(:subject_id, :text, null: false)
      add(:fingerprint, :text, null: false)
      add(:reviewed_by, :text)
      add(:reason, :text)
      add(:reviewed_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    execute("""
    ALTER TABLE continuum_health_reviews
      ADD PRIMARY KEY (finding_type, subject_id, fingerprint)
    """)
  end

  def down do
    drop_if_exists(table(:continuum_health_reviews))

    alter table(:continuum_runs) do
      remove(:lease_heartbeat_at)
      remove(:lease_acquired_at)
    end
  end
end
