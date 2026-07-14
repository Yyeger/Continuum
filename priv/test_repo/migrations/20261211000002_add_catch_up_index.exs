defmodule Continuum.Test.Repo.Migrations.AddCatchUpIndex do
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def up do
    execute("""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS continuum_runs_catch_up_idx
      ON continuum_runs (next_wakeup_at, lease_expires_at, id)
      WHERE state IN ('running', 'suspended')
        AND lease_owner IS NOT NULL
        AND lease_token IS NOT NULL
        AND next_wakeup_at IS NOT NULL
    """)
  end

  def down do
    execute("DROP INDEX CONCURRENTLY IF EXISTS continuum_runs_catch_up_idx")
  end
end
