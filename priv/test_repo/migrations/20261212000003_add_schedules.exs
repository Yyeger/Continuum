defmodule Continuum.Test.Repo.Migrations.AddSchedules do
  use Ecto.Migration

  def up do
    create table(:continuum_schedules, primary_key: false) do
      add(:id, :uuid, primary_key: true)
      add(:run_id, :uuid, null: false)
      add(:workflow, :text, null: false)
      add(:version_hash, :bytea, null: false)
      add(:input, :bytea, null: false)
      add(:namespace, :text, null: false, default: "default")
      add(:attributes, :map, null: false, default: %{})
      add(:trace_context, :bytea)
      add(:scheduled_at, :utc_datetime_usec, null: false)
      add(:state, :text, null: false, default: "scheduled")
      add(:attempt, :integer, null: false, default: 0)
      add(:claimed_at, :utc_datetime_usec)
      add(:started_at, :utc_datetime_usec)
      add(:last_error, :text)
      add(:inserted_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    execute("""
    CREATE INDEX continuum_schedules_due_idx
      ON continuum_schedules (scheduled_at, id)
      WHERE state IN ('scheduled', 'starting')
    """)
  end

  def down do
    drop_if_exists(table(:continuum_schedules))
  end
end
