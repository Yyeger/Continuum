defmodule Continuum.Test.Repo.Migrations.AddCancelRequestedAt do
  use Ecto.Migration

  def change do
    alter table(:continuum_runs) do
      add :cancel_requested_at, :utc_datetime_usec
    end
  end
end
