defmodule Continuum.Test.Repo.Migrations.AddActivityProgress do
  use Ecto.Migration

  def change do
    alter table(:continuum_activity_tasks) do
      add(:last_heartbeat_at, :utc_datetime_usec)
      add(:heartbeat_details, :bytea)
    end
  end
end
