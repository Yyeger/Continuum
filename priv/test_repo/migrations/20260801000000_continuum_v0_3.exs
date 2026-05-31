defmodule Continuum.Test.Repo.Migrations.ContinuumV03 do
  use Ecto.Migration

  # v0.3 run-row deltas: parent/child linkage (PR 5) and continue-as-new chain
  # linkage (PR 6). Lineage columns are nullable; every run gets a
  # correlation_id, defaulting to its own id until it continues as new.
  def up do
    alter table(:continuum_runs) do
      add(:parent_run_id, :uuid)
      add(:parent_command_id, :bytea)
      add(:correlation_id, :uuid)
      add(:continued_from_run_id, :uuid)
    end

    execute("UPDATE continuum_runs SET correlation_id = id WHERE correlation_id IS NULL")

    create(
      index(:continuum_runs, [:parent_run_id],
        where: "parent_run_id IS NOT NULL",
        name: :continuum_runs_parent_idx
      )
    )

    create(
      index(:continuum_runs, [:correlation_id],
        where: "correlation_id IS NOT NULL",
        name: :continuum_runs_correlation_idx
      )
    )

    create(
      index(:continuum_runs, [:continued_from_run_id],
        where: "continued_from_run_id IS NOT NULL",
        name: :continuum_runs_continued_from_idx
      )
    )
  end

  def down do
    drop_if_exists(index(:continuum_runs, [:parent_run_id], name: :continuum_runs_parent_idx))

    drop_if_exists(
      index(:continuum_runs, [:correlation_id], name: :continuum_runs_correlation_idx)
    )

    drop_if_exists(
      index(:continuum_runs, [:continued_from_run_id], name: :continuum_runs_continued_from_idx)
    )

    alter table(:continuum_runs) do
      remove(:parent_run_id)
      remove(:parent_command_id)
      remove(:correlation_id)
      remove(:continued_from_run_id)
    end
  end
end
