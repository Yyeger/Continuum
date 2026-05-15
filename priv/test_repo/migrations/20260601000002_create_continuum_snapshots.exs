defmodule Continuum.Test.Repo.Migrations.CreateContinuumSnapshots do
  use Ecto.Migration

  def up do
    create table(:continuum_snapshots) do
      add :run_id, :uuid, null: false
      add :through_seq, :bigint, null: false
      add :version_hash, :bytea, null: false
      add :payload, :bytea, null: false
      add :taken_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    create unique_index(:continuum_snapshots, [:run_id, :through_seq],
             name: :continuum_snapshots_run_seq_idx
           )

    execute """
    CREATE INDEX continuum_snapshots_latest_idx
      ON continuum_snapshots (run_id, through_seq DESC)
    """
  end

  def down do
    drop_if_exists table(:continuum_snapshots)
  end
end
