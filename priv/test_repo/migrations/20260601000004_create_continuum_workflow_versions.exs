defmodule Continuum.Test.Repo.Migrations.CreateContinuumWorkflowVersions do
  use Ecto.Migration

  def up do
    create table(:continuum_workflow_versions, primary_key: false) do
      add :workflow, :text, null: false
      add :version_hash, :bytea, null: false
      add :entrypoint, :text, null: false
      add :registered_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    execute """
    ALTER TABLE continuum_workflow_versions
      ADD PRIMARY KEY (workflow, version_hash)
    """
  end

  def down do
    drop_if_exists table(:continuum_workflow_versions)
  end
end
