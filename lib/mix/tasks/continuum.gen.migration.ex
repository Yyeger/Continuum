defmodule Mix.Tasks.Continuum.Gen.Migration do
  @moduledoc """
  Generates the Ecto migration that creates Continuum's Postgres tables.

      mix continuum.gen.migration
      mix continuum.gen.migration --from 0.6.1

  Writes a single migration file under `priv/repo/migrations/` (or whatever
  is configured for your repo) that creates: `continuum_runs`,
  monthly-partitioned `continuum_events`, `continuum_signals`,
  `continuum_timers`, `continuum_activity_tasks`,
  `continuum_activity_results`, `continuum_snapshots`,
  `continuum_workflow_versions`, and the `continuum_lease_token_seq` sequence.
  """
  use Mix.Task

  import Macro, only: [camelize: 1]

  @shortdoc "Generates a fresh or versioned Continuum migration"

  @impl true
  def run(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args, strict: [repo: :string, from: :string])

    validate_args!(rest, invalid)

    repo = parse_repo(opts)
    path = source_migrations_path(repo)
    File.mkdir_p!(path)

    timestamp = timestamp()
    {name, source} = migration(opts[:from], repo)
    filename = Path.join(path, "#{timestamp}_#{name}.exs")

    if File.exists?(filename) do
      Mix.raise("migration #{filename} already exists")
    end

    File.write!(filename, source)
    Mix.shell().info("Created #{filename}")
  end

  defp migration(nil, repo) do
    name = "create_continuum_tables"
    {name, migration_source("#{inspect(repo)}.Migrations.#{camelize(name)}")}
  end

  defp migration("0.6.1", repo) do
    name = "upgrade_continuum_v0_6_1_to_v0_6_2"
    {name, upgrade_0_6_1_source("#{inspect(repo)}.Migrations.#{camelize(name)}")}
  end

  defp migration(version, _repo) do
    Mix.raise("unsupported Continuum upgrade source #{inspect(version)}; supported: 0.6.1")
  end

  defp validate_args!(rest, invalid) do
    if invalid != [], do: Mix.raise("invalid options: #{inspect(invalid)}")
    if rest != [], do: Mix.raise("unexpected arguments: #{Enum.join(rest, " ")}")
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
          add :namespace, :text, null: false, default: "default"
          add :idempotency_key, :text
          add :state, :text, null: false
          add :input, :bytea, null: false
          add :attributes, :map, null: false, default: %{}
          add :result, :bytea
          add :error, :bytea
          add :error_stacktrace, :bytea
          add :trace_context, :bytea
          add :parent_run_id, :uuid
          add :parent_command_id, :bytea
          add :correlation_id, :uuid
          add :continued_from_run_id, :uuid
          add :started_at, :utc_datetime_usec, null: false, default: fragment("now()")
          add :completed_at, :utc_datetime_usec
          add :lease_owner, :text
          add :lease_token, :bigint
          add :lease_acquired_at, :utc_datetime_usec
          add :lease_heartbeat_at, :utc_datetime_usec
          add :lease_expires_at, :utc_datetime_usec
          add :next_wakeup_at, :utc_datetime_usec
          add :retention_until, :utc_datetime_usec
          add :cancel_requested_at, :utc_datetime_usec
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
        CREATE INDEX continuum_runs_catch_up_idx
          ON continuum_runs (next_wakeup_at, lease_expires_at, id)
          WHERE state IN ('running', 'suspended')
            AND lease_owner IS NOT NULL
            AND lease_token IS NOT NULL
            AND next_wakeup_at IS NOT NULL
        \"\"\"

        execute \"\"\"
        CREATE INDEX continuum_runs_parent_idx
          ON continuum_runs (parent_run_id)
          WHERE parent_run_id IS NOT NULL
        \"\"\"

        execute \"\"\"
        CREATE INDEX continuum_runs_correlation_idx
          ON continuum_runs (correlation_id)
          WHERE correlation_id IS NOT NULL
        \"\"\"

        execute \"\"\"
        CREATE INDEX continuum_runs_continued_from_idx
          ON continuum_runs (continued_from_run_id)
          WHERE continued_from_run_id IS NOT NULL
        \"\"\"

        execute \"\"\"
        CREATE INDEX continuum_runs_correlation_completed_idx
          ON continuum_runs (correlation_id, completed_at)
          WHERE correlation_id IS NOT NULL
        \"\"\"

        execute \"\"\"
        CREATE INDEX continuum_runs_attributes_gin_idx
          ON continuum_runs USING gin (attributes)
        \"\"\"

        execute \"\"\"
        CREATE INDEX continuum_runs_namespace_state_idx
          ON continuum_runs (namespace, state)
        \"\"\"

        execute \"\"\"
        CREATE UNIQUE INDEX continuum_runs_ingress_key_idx
          ON continuum_runs (namespace, workflow, idempotency_key)
          WHERE idempotency_key IS NOT NULL
            AND parent_run_id IS NULL
            AND continued_from_run_id IS NULL
        \"\"\"

        create table(:continuum_run_ingress_keys, primary_key: false) do
          add :namespace, :text, null: false
          add :workflow, :text, null: false
          add :idempotency_key, :text, null: false
          add :run_id, :uuid, null: false
          add :created_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        execute \"\"\"
        ALTER TABLE continuum_run_ingress_keys
          ADD PRIMARY KEY (namespace, workflow, idempotency_key)
        \"\"\"

        create index(:continuum_run_ingress_keys, [:run_id])

        create table(:continuum_schedules, primary_key: false) do
          add :id, :uuid, primary_key: true
          add :run_id, :uuid, null: false
          add :workflow, :text, null: false
          add :version_hash, :bytea, null: false
          add :input, :bytea, null: false
          add :namespace, :text, null: false, default: "default"
          add :attributes, :map, null: false, default: %{}
          add :trace_context, :bytea
          add :scheduled_at, :utc_datetime_usec, null: false
          add :state, :text, null: false, default: "scheduled"
          add :attempt, :integer, null: false, default: 0
          add :claimed_at, :utc_datetime_usec
          add :started_at, :utc_datetime_usec
          add :last_error, :text
          add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        execute \"\"\"
        CREATE INDEX continuum_schedules_due_idx
          ON continuum_schedules (scheduled_at, id)
          WHERE state IN ('scheduled', 'starting')
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
        create_default_event_partition()

        create table(:continuum_signals) do
          add :run_id, :uuid, null: false
          add :correlation_id, :uuid
          add :name, :text, null: false
          add :delivery_id, :text
          add :payload, :bytea, null: false
          add :delivered, :boolean, null: false, default: false
          add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        execute \"\"\"
        CREATE INDEX continuum_signals_pending_idx
          ON continuum_signals (run_id, name)
          WHERE delivered = false
        \"\"\"

        execute \"\"\"
        CREATE UNIQUE INDEX continuum_signals_delivery_key_idx
          ON continuum_signals (correlation_id, name, delivery_id)
          WHERE delivery_id IS NOT NULL
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
          add :lineage_id, :uuid
          add :parent_task_id,
              references(:continuum_activity_tasks, type: :uuid, on_delete: :nilify_all)
          add :seq, :bigint, null: false
          add :queue, :text, null: false, default: "default"
          add :priority, :integer, null: false, default: 0
          add :mfa, :bytea, null: false
          add :attempt, :integer, null: false, default: 1
          add :state, :text, null: false
          add :scheduled_at, :utc_datetime_usec, null: false, default: fragment("now()")
          add :available_at, :utc_datetime_usec, null: false, default: fragment("now()")
          add :lease_owner, :text
          add :lease_expires_at, :utc_datetime_usec
          add :last_heartbeat_at, :utc_datetime_usec
          add :heartbeat_details, :bytea
          add :result, :bytea
          add :error, :bytea
        end

        execute \"\"\"
        CREATE INDEX continuum_activity_tasks_pickup_idx
          ON continuum_activity_tasks (queue, priority DESC, available_at, scheduled_at)
          WHERE state = 'available'
        \"\"\"

        create index(:continuum_activity_tasks, [:lineage_id, :scheduled_at])

        create table(:continuum_activity_attempts) do
          add :task_id, :uuid, null: false
          add :run_id, :uuid, null: false
          add :lineage_id, :uuid, null: false
          add :attempt, :integer, null: false
          add :outcome, :text, null: false
          add :error, :bytea
          add :recorded_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        create unique_index(:continuum_activity_attempts, [:task_id, :attempt])
        create index(:continuum_activity_attempts, [:lineage_id, :recorded_at])

        create table(:continuum_activity_operations, primary_key: false) do
          add :id, :uuid, primary_key: true
          add :task_id, :uuid, null: false
          add :successor_task_id, :uuid
          add :run_id, :uuid, null: false
          add :lineage_id, :uuid, null: false
          add :action, :text, null: false
          add :classification, :text
          add :operator, :text, null: false
          add :reason, :text, null: false
          add :retry_policy, :bytea
          add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        create index(:continuum_activity_operations, [:lineage_id, :inserted_at])
        create unique_index(:continuum_activity_operations, [:successor_task_id])

        create table(:continuum_activity_results, primary_key: false) do
          add :activity_module, :text, null: false
          add :idempotency_key, :text, null: false
          add :run_id, :uuid, null: false
          add :seq, :bigint, null: false
          add :result, :bytea, null: false
          add :completed_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        execute \"\"\"
        ALTER TABLE continuum_activity_results
          ADD PRIMARY KEY (activity_module, idempotency_key)
        \"\"\"

        create table(:continuum_snapshots) do
          add :run_id, :uuid, null: false
          add :through_seq, :bigint, null: false
          add :version_hash, :bytea, null: false
          add :format_version, :smallint, null: false, default: 1
          add :payload, :bytea, null: false
          add :taken_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        create unique_index(:continuum_snapshots, [:run_id, :through_seq],
                 name: :continuum_snapshots_run_seq_idx
               )

        execute \"\"\"
        CREATE INDEX continuum_snapshots_latest_idx
          ON continuum_snapshots (run_id, through_seq DESC)
        \"\"\"

        create table(:continuum_workflow_versions, primary_key: false) do
          add :workflow, :text, null: false
          add :version_hash, :bytea, null: false
          add :entrypoint, :text, null: false
          add :registered_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        execute \"\"\"
        ALTER TABLE continuum_workflow_versions
          ADD PRIMARY KEY (workflow, version_hash)
        \"\"\"

        create table(:continuum_health_reviews, primary_key: false) do
          add :finding_type, :text, null: false
          add :subject_id, :text, null: false
          add :fingerprint, :text, null: false
          add :reviewed_by, :text
          add :reason, :text
          add :reviewed_at, :utc_datetime_usec, null: false, default: fragment(\"now()\")
        end

        execute \"\"\"
        ALTER TABLE continuum_health_reviews
          ADD PRIMARY KEY (finding_type, subject_id, fingerprint)
        \"\"\"
      end

      def down do
        drop_if_exists table(:continuum_health_reviews)
        drop_if_exists table(:continuum_workflow_versions)
        drop_if_exists table(:continuum_snapshots)
        drop_if_exists table(:continuum_activity_results)
        drop_if_exists table(:continuum_activity_operations)
        drop_if_exists table(:continuum_activity_attempts)
        drop_if_exists table(:continuum_activity_tasks)
        drop_if_exists table(:continuum_timers)
        drop_if_exists table(:continuum_signals)
        drop_if_exists table(:continuum_events)
        drop_if_exists table(:continuum_schedules)
        drop_if_exists table(:continuum_run_ingress_keys)
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

      defp create_default_event_partition do
        execute \"\"\"
        CREATE TABLE IF NOT EXISTS continuum_events_default
        PARTITION OF continuum_events DEFAULT
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

  defp upgrade_0_6_1_source(module_name) do
    """
    defmodule #{module_name} do
      use Ecto.Migration

      def up do
        alter table(:continuum_runs) do
          add_if_not_exists :cancel_requested_at, :utc_datetime_usec
          add_if_not_exists :error_stacktrace, :bytea
          add_if_not_exists :lease_acquired_at, :utc_datetime_usec
          add_if_not_exists :lease_heartbeat_at, :utc_datetime_usec
        end

        execute \"\"\"
        UPDATE continuum_runs
        SET lease_acquired_at = COALESCE(lease_acquired_at, started_at),
            lease_heartbeat_at = COALESCE(
              lease_heartbeat_at,
              lease_expires_at - interval '30 seconds'
            )
        WHERE lease_owner IS NOT NULL
        \"\"\"

        execute \"\"\"
        CREATE INDEX IF NOT EXISTS continuum_runs_catch_up_idx
          ON continuum_runs (next_wakeup_at, lease_expires_at, id)
          WHERE state IN ('running', 'suspended')
            AND lease_owner IS NOT NULL
            AND lease_token IS NOT NULL
            AND next_wakeup_at IS NOT NULL
        \"\"\"

        create_if_not_exists table(:continuum_health_reviews, primary_key: false) do
          add :finding_type, :text, null: false
          add :subject_id, :text, null: false
          add :fingerprint, :text, null: false
          add :reviewed_by, :text
          add :reason, :text
          add :reviewed_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        execute \"\"\"
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'continuum_health_reviews_pkey'
          ) THEN
            ALTER TABLE continuum_health_reviews
              ADD PRIMARY KEY (finding_type, subject_id, fingerprint);
          END IF;
        END
        $$
        \"\"\"

        execute \"\"\"
        CREATE TABLE IF NOT EXISTS continuum_events_default
        PARTITION OF continuum_events DEFAULT
        \"\"\"

        alter table(:continuum_activity_tasks) do
          add_if_not_exists :lineage_id, :uuid
          add_if_not_exists :parent_task_id,
                            references(:continuum_activity_tasks,
                              type: :uuid,
                              on_delete: :nilify_all
                            )
        end

        execute "UPDATE continuum_activity_tasks SET lineage_id = id WHERE lineage_id IS NULL"
        create_if_not_exists index(:continuum_activity_tasks, [:lineage_id, :scheduled_at])

        create_if_not_exists table(:continuum_activity_attempts) do
          add :task_id, :uuid, null: false
          add :run_id, :uuid, null: false
          add :lineage_id, :uuid, null: false
          add :attempt, :integer, null: false
          add :outcome, :text, null: false
          add :error, :bytea
          add :recorded_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        create_if_not_exists unique_index(:continuum_activity_attempts, [:task_id, :attempt])
        create_if_not_exists index(:continuum_activity_attempts, [:lineage_id, :recorded_at])

        create_if_not_exists table(:continuum_activity_operations, primary_key: false) do
          add :id, :uuid, primary_key: true
          add :task_id, :uuid, null: false
          add :successor_task_id, :uuid
          add :run_id, :uuid, null: false
          add :lineage_id, :uuid, null: false
          add :action, :text, null: false
          add :classification, :text
          add :operator, :text, null: false
          add :reason, :text, null: false
          add :retry_policy, :bytea
          add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
        end

        create_if_not_exists index(:continuum_activity_operations, [:lineage_id, :inserted_at])
        create_if_not_exists unique_index(:continuum_activity_operations, [:successor_task_id])
      end

      def down do
        drop_if_exists table(:continuum_activity_operations)
        drop_if_exists table(:continuum_activity_attempts)

        alter table(:continuum_activity_tasks) do
          remove_if_exists :parent_task_id, :uuid
          remove_if_exists :lineage_id, :uuid
        end

        execute "DROP TABLE IF EXISTS continuum_events_default"
        drop_if_exists table(:continuum_health_reviews)
        execute "DROP INDEX IF EXISTS continuum_runs_catch_up_idx"

        alter table(:continuum_runs) do
          remove_if_exists :lease_heartbeat_at, :utc_datetime_usec
          remove_if_exists :lease_acquired_at, :utc_datetime_usec
          remove_if_exists :error_stacktrace, :bytea
          remove_if_exists :cancel_requested_at, :utc_datetime_usec
        end
      end
    end
    """
  end
end
