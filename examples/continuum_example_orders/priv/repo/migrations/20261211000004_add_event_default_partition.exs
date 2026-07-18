defmodule ContinuumExampleOrders.Repo.Migrations.AddEventDefaultPartition do
  use Ecto.Migration

  def up do
    execute("""
    CREATE TABLE IF NOT EXISTS continuum_events_default
    PARTITION OF continuum_events DEFAULT
    """)
  end

  def down do
    execute("DROP TABLE IF EXISTS continuum_events_default")
  end
end
