defmodule Continuum.Test.Repo.Migrations.AddPruningIdempotencyRecords do
  use Ecto.Migration

  def up do
    create table(:continuum_run_ingress_keys, primary_key: false) do
      add(:namespace, :text, null: false)
      add(:workflow, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:run_id, :uuid, null: false)
      add(:created_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    execute("""
    ALTER TABLE continuum_run_ingress_keys
      ADD PRIMARY KEY (namespace, workflow, idempotency_key)
    """)

    create(index(:continuum_run_ingress_keys, [:run_id]))

    execute("""
    INSERT INTO continuum_run_ingress_keys
      (namespace, workflow, idempotency_key, run_id, created_at)
    SELECT namespace, workflow, idempotency_key, id, started_at
    FROM continuum_runs
    WHERE idempotency_key IS NOT NULL
    ON CONFLICT DO NOTHING
    """)
  end

  def down do
    drop_if_exists(table(:continuum_run_ingress_keys))
  end
end
