defmodule ContinuumExampleOrders.Repo.Migrations.AddErrorStacktrace do
  use Ecto.Migration

  def change do
    alter table(:continuum_runs) do
      add_if_not_exists(:error_stacktrace, :bytea)
    end
  end
end
