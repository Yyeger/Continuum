defmodule Continuum.Test.Repo.Migrations.PartitionContinuumEvents do
  use Ecto.Migration

  def up do
    rename table(:continuum_events), to: table(:continuum_events_legacy)

    execute """
    CREATE TABLE continuum_events (
      run_id uuid NOT NULL,
      seq bigint NOT NULL,
      event_type text NOT NULL,
      payload bytea NOT NULL,
      inserted_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (run_id, seq, inserted_at)
    ) PARTITION BY RANGE (inserted_at)
    """

    create_initial_partitions()

    # Fails loudly if legacy rows fall outside the four pre-created partitions.
    # Local v0.1 dev DBs can reset or add the missing partitions manually.
    execute """
    INSERT INTO continuum_events (run_id, seq, event_type, payload, inserted_at)
    SELECT run_id, seq, event_type, payload, inserted_at
    FROM continuum_events_legacy
    """

    drop table(:continuum_events_legacy)
  end

  def down do
    rename table(:continuum_events), to: table(:continuum_events_partitioned)

    create table(:continuum_events, primary_key: false) do
      add :run_id, :uuid, null: false
      add :seq, :bigint, null: false
      add :event_type, :text, null: false
      add :payload, :bytea, null: false
      add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    execute "ALTER TABLE continuum_events ADD PRIMARY KEY (run_id, seq)"

    execute """
    INSERT INTO continuum_events (run_id, seq, event_type, payload, inserted_at)
    SELECT run_id, seq, event_type, payload, inserted_at
    FROM continuum_events_partitioned
    """

    drop table(:continuum_events_partitioned)
  end

  defp create_initial_partitions do
    today = Date.utc_today()
    month = Date.new!(today.year, today.month, 1)

    for offset <- 0..3 do
      create_partition(Date.add(month, offset * 32) |> Date.beginning_of_month())
    end
  end

  defp create_partition(month) do
    next_month = month |> Date.add(32) |> Date.beginning_of_month()

    execute """
    CREATE TABLE IF NOT EXISTS #{partition_name(month)}
    PARTITION OF continuum_events
    FOR VALUES FROM ('#{Date.to_iso8601(month)} 00:00:00+00')
    TO ('#{Date.to_iso8601(next_month)} 00:00:00+00')
    """
  end

  defp partition_name(%Date{year: year, month: month}) do
    "continuum_events_y#{year}_m#{pad2(month)}"
  end

  defp pad2(month) when month < 10, do: "0#{month}"
  defp pad2(month), do: "#{month}"
end
