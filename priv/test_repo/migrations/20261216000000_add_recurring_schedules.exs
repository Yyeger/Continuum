defmodule Continuum.Test.Repo.Migrations.AddRecurringSchedules do
  use Ecto.Migration

  def up do
    create table(:continuum_recurring_schedules, primary_key: false) do
      add(:id, :uuid, primary_key: true)
      add(:workflow, :text, null: false)
      add(:version_hash, :bytea, null: false)
      add(:input, :bytea, null: false)
      add(:namespace, :text, null: false, default: "default")
      add(:attributes, :map, null: false, default: %{})
      add(:trace_context, :bytea)
      add(:every_ms, :bigint, null: false)
      add(:next_occurrence_at, :utc_datetime_usec, null: false)
      add(:overlap_policy, :text, null: false)
      add(:missed_policy, :text, null: false)
      add(:max_catch_up, :integer, null: false, default: 10)
      add(:state, :text, null: false, default: "active")
      add(:inserted_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    create(
      constraint(:continuum_recurring_schedules, :continuum_recurring_contract,
        check:
          "every_ms >= 1000 AND max_catch_up BETWEEN 1 AND 100 AND overlap_policy IN ('allow', 'skip') AND missed_policy IN ('skip', 'catch_up') AND state IN ('active', 'paused')"
      )
    )

    create(
      index(:continuum_recurring_schedules, [:next_occurrence_at, :id],
        where: "state = 'active'",
        name: :continuum_recurring_due_idx
      )
    )

    alter table(:continuum_schedules) do
      add(:recurring_schedule_id, references(:continuum_recurring_schedules, type: :uuid))
      add(:occurrence_at, :utc_datetime_usec)
    end

    create(
      unique_index(:continuum_schedules, [:recurring_schedule_id, :occurrence_at],
        name: :continuum_schedule_occurrence_idx
      )
    )

    create(
      constraint(:continuum_schedules, :continuum_schedule_occurrence_pair,
        check: "(recurring_schedule_id IS NULL) = (occurrence_at IS NULL)"
      )
    )
  end

  def down do
    drop(constraint(:continuum_schedules, :continuum_schedule_occurrence_pair))

    drop(
      index(:continuum_schedules, [:recurring_schedule_id, :occurrence_at],
        name: :continuum_schedule_occurrence_idx
      )
    )

    alter table(:continuum_schedules) do
      remove(:occurrence_at)
      remove(:recurring_schedule_id)
    end

    drop(table(:continuum_recurring_schedules))
  end
end
