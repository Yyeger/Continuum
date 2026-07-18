defmodule ContinuumExampleOrders.Repo.Migrations.AddActivityOperations do
  use Ecto.Migration

  def up do
    alter table(:continuum_activity_tasks) do
      add(:lineage_id, :uuid)

      add(
        :parent_task_id,
        references(:continuum_activity_tasks, type: :uuid, on_delete: :nilify_all)
      )
    end

    execute("UPDATE continuum_activity_tasks SET lineage_id = id WHERE lineage_id IS NULL")
    create(index(:continuum_activity_tasks, [:lineage_id, :scheduled_at]))

    create table(:continuum_activity_attempts) do
      add(:task_id, :uuid, null: false)
      add(:run_id, :uuid, null: false)
      add(:lineage_id, :uuid, null: false)
      add(:attempt, :integer, null: false)
      add(:outcome, :text, null: false)
      add(:error, :bytea)
      add(:recorded_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    create(unique_index(:continuum_activity_attempts, [:task_id, :attempt]))
    create(index(:continuum_activity_attempts, [:lineage_id, :recorded_at]))

    create table(:continuum_activity_operations, primary_key: false) do
      add(:id, :uuid, primary_key: true)
      add(:task_id, :uuid, null: false)
      add(:successor_task_id, :uuid)
      add(:run_id, :uuid, null: false)
      add(:lineage_id, :uuid, null: false)
      add(:action, :text, null: false)
      add(:classification, :text)
      add(:operator, :text, null: false)
      add(:reason, :text, null: false)
      add(:retry_policy, :bytea)
      add(:inserted_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    create(index(:continuum_activity_operations, [:lineage_id, :inserted_at]))
    create(unique_index(:continuum_activity_operations, [:successor_task_id]))
  end

  def down do
    drop_if_exists(table(:continuum_activity_operations))
    drop_if_exists(table(:continuum_activity_attempts))

    alter table(:continuum_activity_tasks) do
      remove(:parent_task_id)
      remove(:lineage_id)
    end
  end
end
