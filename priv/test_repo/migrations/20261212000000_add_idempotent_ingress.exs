defmodule Continuum.Test.Repo.Migrations.AddIdempotentIngress do
  use Ecto.Migration

  def up do
    alter table(:continuum_runs) do
      add(:idempotency_key, :text)
    end

    execute("""
    CREATE UNIQUE INDEX continuum_runs_ingress_key_idx
      ON continuum_runs (namespace, workflow, idempotency_key)
      WHERE idempotency_key IS NOT NULL
        AND parent_run_id IS NULL
        AND continued_from_run_id IS NULL
    """)

    alter table(:continuum_signals) do
      add(:correlation_id, :uuid)
      add(:delivery_id, :text)
    end

    execute("""
    UPDATE continuum_signals AS signal
    SET correlation_id = COALESCE(run.correlation_id, run.id)
    FROM continuum_runs AS run
    WHERE signal.run_id = run.id
      AND signal.correlation_id IS NULL
    """)

    execute("""
    CREATE UNIQUE INDEX continuum_signals_delivery_key_idx
      ON continuum_signals (correlation_id, name, delivery_id)
      WHERE delivery_id IS NOT NULL
    """)
  end

  def down do
    execute("DROP INDEX IF EXISTS continuum_signals_delivery_key_idx")

    alter table(:continuum_signals) do
      remove(:delivery_id)
      remove(:correlation_id)
    end

    execute("DROP INDEX IF EXISTS continuum_runs_ingress_key_idx")

    alter table(:continuum_runs) do
      remove(:idempotency_key)
    end
  end
end
