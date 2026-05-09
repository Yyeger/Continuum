defmodule Mix.Tasks.Continuum.Gen.Migration do
  @moduledoc """
  Generates the Ecto migration that creates Continuum's Postgres tables.

      mix continuum.gen.migration

  Writes a single migration file under `priv/repo/migrations/` (or whatever
  is configured for your repo) that creates: `continuum_runs`,
  monthly-partitioned `continuum_events`, `continuum_signals`,
  `continuum_timers`, `continuum_activity_tasks`, and the
  `continuum_lease_token_seq` sequence.
  """
  use Mix.Task

  import Macro, only: [camelize: 1]

  @shortdoc "Generates a migration for Continuum's Postgres schema"

  @impl true
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string])

    repo = parse_repo(opts)
    path = source_migrations_path(repo)
    File.mkdir_p!(path)

    timestamp = timestamp()
    name = "create_continuum_tables"
    filename = Path.join(path, "#{timestamp}_#{name}.exs")
    module_name = "#{inspect(repo)}.Migrations.#{camelize(name)}"

    if File.exists?(filename) do
      Mix.raise("migration #{filename} already exists")
    end

    File.write!(filename, migration_source(module_name))
    Mix.shell().info("Created #{filename}")
  end

  defp source_migrations_path(repo) do
    priv = Keyword.get(repo.config(), :priv, "priv/repo")
    Path.join([File.cwd!(), priv, "migrations"])
  end

  defp parse_repo(opts) do
    case opts[:repo] do
      nil ->
        Application.get_env(:continuum, :repo) ||
          Mix.raise(
            "no repo configured. Pass --repo MyApp.Repo or set " <>
              ":continuum, :repo in config"
          )

      repo_str ->
        Module.concat([repo_str])
    end
  end

  defp timestamp do
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
  end

  defp pad(n) when n < 10, do: "0#{n}"
  defp pad(n), do: "#{n}"

  defp migration_source(module_name) do
    """
    defmodule #{module_name} do
      use Ecto.Migration

      def up do
        execute "CREATE SEQUENCE IF NOT EXISTS continuum_lease_token_seq"

        create table(:continuum_runs, primary_key: false, options: "WITH (fillfactor = 70)") do
          add :id, :uuid, primary_key: true
          add :workflow, :text, null: false
          add :version_hash, :bytea, null: false
          add :state, :text, null: false
          add :input, :bytea, null: false
          add :result, :bytea
          add :error, :bytea
          add :started_at, :utc_datetime_usec, null: false, default: fragment("now()")
          add :completed_at, :utc_datetime_usec
          add :lease_owner, :text
          add :lease_token, :bigint
          add :lease_expires_at, :utc_datetime_usec
          add :next_wakeup_at, :utc_datetime_usec
          add :retention_until, :utc_datetime_usec
        end

        execute \"\"\"
        CREATE INDEX continuum_runs_dispatch_idx
          ON continuum_runs (next_wakeup_at NULLS LAST)
          WHERE state = 'suspended' AND lease_owner IS NULL
        \"\"\"

        execute \"\"\"
        CREATE INDEX continuum_runs_lease_idx
          ON continuum_runs (lease_expires_at)
          WHERE lease_owner IS NOT NULL
        \"\"\"

        execute \"\"\"
        CREATE TABLE continuum_events (
          run_id uuid NOT NULL,
          seq bigint NOT NULL,
          event_type text NOT NULL,
          payload bytea NOT NULL,
          inserted_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (run_id, seq, inserted_at)
        ) PARTITION BY RANGE (inserted_at)
        \"\"\"

        create_initial_event_partitions()

        create table(:continuum_signals) do
          add :run_id, :uuid, null: false
          add :name, :text, null: false
          add :payload, :bytea, null: false
          add :delivered, :boolean, null: false, default: false
          add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        execute \"\"\"
        CREATE INDEX continuum_signals_pending_idx
          ON continuum_signals (run_id, name)
          WHERE delivered = false
        \"\"\"

        create table(:continuum_timers, primary_key: false) do
          add :id, :uuid, primary_key: true
          add :run_id, :uuid, null: false
          add :fires_at, :utc_datetime_usec, null: false
          add :fired, :boolean, null: false, default: false
        end

        execute \"\"\"
        CREATE INDEX continuum_timers_due_idx
          ON continuum_timers (fires_at)
          WHERE fired = false
        \"\"\"

        create table(:continuum_activity_tasks, primary_key: false) do
          add :id, :uuid, primary_key: true
          add :run_id, :uuid, null: false
          add :seq, :bigint, null: false
          add :mfa, :bytea, null: false
          add :attempt, :integer, null: false, default: 1
          add :state, :text, null: false
          add :scheduled_at, :utc_datetime_usec, null: false, default: fragment("now()")
          add :available_at, :utc_datetime_usec, null: false, default: fragment("now()")
          add :lease_owner, :text
          add :lease_expires_at, :utc_datetime_usec
          add :result, :bytea
          add :error, :bytea
        end

        execute \"\"\"
        CREATE INDEX continuum_activity_tasks_pickup_idx
          ON continuum_activity_tasks (available_at)
          WHERE state = 'available'
        \"\"\"
      end

      def down do
        drop_if_exists table(:continuum_activity_tasks)
        drop_if_exists table(:continuum_timers)
        drop_if_exists table(:continuum_signals)
        drop_if_exists table(:continuum_events)
        drop_if_exists table(:continuum_runs)
        execute "DROP SEQUENCE IF EXISTS continuum_lease_token_seq"
      end

      defp create_initial_event_partitions do
        today = Date.utc_today()
        month = Date.new!(today.year, today.month, 1)

        for offset <- 0..3 do
          create_event_partition(Date.add(month, offset * 32) |> Date.beginning_of_month())
        end
      end

      defp create_event_partition(month) do
        next_month = month |> Date.add(32) |> Date.beginning_of_month()

        execute \"\"\"
        CREATE TABLE IF NOT EXISTS \#{event_partition_name(month)}
        PARTITION OF continuum_events
        FOR VALUES FROM ('\#{Date.to_iso8601(month)} 00:00:00+00')
        TO ('\#{Date.to_iso8601(next_month)} 00:00:00+00')
        \"\"\"
      end

      defp event_partition_name(%Date{year: year, month: month}) do
        "continuum_events_y\#{year}_m\#{pad2(month)}"
      end

      defp pad2(month) when month < 10, do: "0\#{month}"
      defp pad2(month), do: "\#{month}"
    end
    """
  end
end
