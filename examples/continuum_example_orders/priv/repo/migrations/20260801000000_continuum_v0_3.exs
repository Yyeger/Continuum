defmodule ContinuumExampleOrders.Repo.Migrations.ContinuumV03 do
  use Ecto.Migration

  # v0.3 run-row deltas: parent/child + continue-as-new chain linkage and the
  # content-addressed workflow-version registry table.
  def up do
    alter table(:continuum_runs) do
      add :parent_run_id, :uuid
      add :parent_command_id, :bytea
      add :correlation_id, :uuid
      add :continued_from_run_id, :uuid
    end

    create index(:continuum_runs, [:parent_run_id],
             where: "parent_run_id IS NOT NULL",
             name: :continuum_runs_parent_idx
           )

    create index(:continuum_runs, [:correlation_id],
             where: "correlation_id IS NOT NULL",
             name: :continuum_runs_correlation_idx
           )

    create index(:continuum_runs, [:continued_from_run_id],
             where: "continued_from_run_id IS NOT NULL",
             name: :continuum_runs_continued_from_idx
           )

    create table(:continuum_workflow_versions, primary_key: false) do
      add :workflow, :text, null: false
      add :version_hash, :bytea, null: false
      add :entrypoint, :text, null: false
      add :registered_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    execute "ALTER TABLE continuum_workflow_versions ADD PRIMARY KEY (workflow, version_hash)"
  end

  def down do
    drop_if_exists table(:continuum_workflow_versions)

    drop_if_exists index(:continuum_runs, [:parent_run_id], name: :continuum_runs_parent_idx)
    drop_if_exists index(:continuum_runs, [:correlation_id], name: :continuum_runs_correlation_idx)

    drop_if_exists index(:continuum_runs, [:continued_from_run_id],
                     name: :continuum_runs_continued_from_idx
                   )

    alter table(:continuum_runs) do
      remove :parent_run_id
      remove :parent_command_id
      remove :correlation_id
      remove :continued_from_run_id
    end
  end
end
