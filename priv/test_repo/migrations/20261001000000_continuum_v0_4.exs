defmodule Continuum.Test.Repo.Migrations.ContinuumV04 do
  use Ecto.Migration

  def up do
    alter table(:continuum_snapshots) do
      add(:format_version, :smallint, null: false, default: 1)
    end

    create(
      index(:continuum_runs, [:correlation_id, :completed_at],
        where: "correlation_id IS NOT NULL",
        name: :continuum_runs_correlation_completed_idx
      )
    )
  end

  def down do
    drop_if_exists(
      index(:continuum_runs, [:correlation_id, :completed_at],
        name: :continuum_runs_correlation_completed_idx
      )
    )

    alter table(:continuum_snapshots) do
      remove(:format_version)
    end
  end
end
