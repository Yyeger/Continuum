defmodule Continuum.Test.Repo.Migrations.AddActivityQueues do
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def up do
    alter table(:continuum_activity_tasks) do
      add(:queue, :text, null: false, default: "default")
      add(:priority, :integer, null: false, default: 0)
    end

    execute("DROP INDEX CONCURRENTLY IF EXISTS continuum_activity_tasks_pickup_idx")

    execute("""
    CREATE INDEX CONCURRENTLY continuum_activity_tasks_pickup_idx
      ON continuum_activity_tasks (queue, priority DESC, available_at, scheduled_at)
      WHERE state = 'available'
    """)
  end

  def down do
    execute("DROP INDEX CONCURRENTLY IF EXISTS continuum_activity_tasks_pickup_idx")

    execute("""
    CREATE INDEX CONCURRENTLY continuum_activity_tasks_pickup_idx
      ON continuum_activity_tasks (available_at)
      WHERE state = 'available'
    """)

    alter table(:continuum_activity_tasks) do
      remove(:priority)
      remove(:queue)
    end
  end
end
