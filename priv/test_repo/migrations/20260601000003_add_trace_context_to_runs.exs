defmodule Continuum.Test.Repo.Migrations.AddTraceContextToRuns do
  use Ecto.Migration

  def change do
    alter table(:continuum_runs) do
      add :trace_context, :bytea
    end
  end
end
