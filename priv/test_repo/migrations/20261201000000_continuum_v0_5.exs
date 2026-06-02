defmodule Continuum.Test.Repo.Migrations.ContinuumV05 do
  use Ecto.Migration

  def up do
    alter table(:continuum_runs) do
      add(:namespace, :text, null: false, default: "default")
      add(:attributes, :map, null: false, default: %{})
    end

    execute("""
    CREATE INDEX continuum_runs_attributes_gin_idx
      ON continuum_runs USING gin (attributes)
    """)

    execute("""
    CREATE INDEX continuum_runs_namespace_state_idx
      ON continuum_runs (namespace, state)
    """)
  end

  def down do
    execute("DROP INDEX IF EXISTS continuum_runs_namespace_state_idx")
    execute("DROP INDEX IF EXISTS continuum_runs_attributes_gin_idx")

    alter table(:continuum_runs) do
      remove(:attributes)
      remove(:namespace)
    end
  end
end
