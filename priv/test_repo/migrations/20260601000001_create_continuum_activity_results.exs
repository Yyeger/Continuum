defmodule Continuum.Test.Repo.Migrations.CreateContinuumActivityResults do
  use Ecto.Migration

  def up do
    create table(:continuum_activity_results, primary_key: false) do
      add :activity_module, :text, null: false
      add :idempotency_key, :text, null: false
      add :run_id, :uuid, null: false
      add :seq, :bigint, null: false
      add :result, :bytea, null: false
      add :completed_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    execute """
    ALTER TABLE continuum_activity_results
      ADD PRIMARY KEY (activity_module, idempotency_key)
    """
  end

  def down do
    drop_if_exists table(:continuum_activity_results)
  end
end
