defmodule Mix.Tasks.Continuum.Gen.MigrationTest do
  use ExUnit.Case, async: false

  test "generates the current partitioned events schema" do
    in_tmp(fn ->
      Mix.Task.rerun("continuum.gen.migration", ["--repo", "Continuum.Test.Repo"])

      [path] = Path.wildcard("priv/test_repo/migrations/*_create_continuum_tables.exs")
      source = File.read!(path)

      assert source =~ "PARTITION BY RANGE (inserted_at)"
      assert source =~ "PRIMARY KEY (run_id, seq, inserted_at)"
      assert source =~ "create_initial_event_partitions()"
      assert source =~ "create_default_event_partition()"
      assert source =~ "PARTITION OF continuum_events DEFAULT"
      assert source =~ "create table(:continuum_activity_results"
      assert source =~ "PRIMARY KEY (activity_module, idempotency_key)"
      assert source =~ ~s(add :queue, :text, null: false, default: "default")
      assert source =~ "add :priority, :integer, null: false, default: 0"
      assert source =~ "continuum_activity_tasks_pickup_idx"
      assert source =~ "(queue, priority DESC, available_at, scheduled_at)"
      assert source =~ "add :trace_context, :bytea"
      assert source =~ "create table(:continuum_snapshots"
      assert source =~ "add :format_version, :smallint, null: false, default: 1"
      assert source =~ "continuum_snapshots_latest_idx"
      assert source =~ "create table(:continuum_workflow_versions"
      assert source =~ "PRIMARY KEY (workflow, version_hash)"
      assert source =~ "add :parent_run_id, :uuid"
      assert source =~ "add :parent_command_id, :bytea"
      assert source =~ "add :correlation_id, :uuid"
      assert source =~ "add :continued_from_run_id, :uuid"
      assert source =~ ~s(add :namespace, :text, null: false, default: "default")
      assert source =~ "add :attributes, :map, null: false, default: %{}"
      assert source =~ "continuum_runs_parent_idx"
      assert source =~ "continuum_runs_correlation_idx"
      assert source =~ "continuum_runs_continued_from_idx"
      assert source =~ "continuum_runs_correlation_completed_idx"
      assert source =~ "continuum_runs_attributes_gin_idx"
      assert source =~ "continuum_runs_namespace_state_idx"
      assert source =~ "continuum_runs_catch_up_idx"
      assert source =~ "add :lease_acquired_at, :utc_datetime_usec"
      assert source =~ "add :lease_heartbeat_at, :utc_datetime_usec"
      assert source =~ "create table(:continuum_health_reviews"
      assert source =~ "PRIMARY KEY (finding_type, subject_id, fingerprint)"
      assert source =~ "add :lineage_id, :uuid"
      assert source =~ "add :parent_task_id,"

      assert source =~
               "references(:continuum_activity_tasks, type: :uuid, on_delete: :nilify_all)"

      assert source =~ "create table(:continuum_activity_attempts"
      assert source =~ "create table(:continuum_activity_operations"
    end)
  end

  test "generates the complete v0.6.1 to v0.6.2 upgrade" do
    in_tmp(fn ->
      Mix.Task.rerun("continuum.gen.migration", [
        "--repo",
        "Continuum.Test.Repo",
        "--from",
        "0.6.1"
      ])

      [path] =
        Path.wildcard("priv/test_repo/migrations/*_upgrade_continuum_v0_6_1_to_v0_6_2.exs")

      source = File.read!(path)

      assert source =~ "add_if_not_exists :cancel_requested_at"
      assert source =~ "add_if_not_exists :error_stacktrace"
      assert source =~ "add_if_not_exists :lease_acquired_at"
      assert source =~ "add_if_not_exists :lease_heartbeat_at"
      assert source =~ "continuum_runs_catch_up_idx"
      assert source =~ "create_if_not_exists table(:continuum_health_reviews"
      assert source =~ "PARTITION OF continuum_events DEFAULT"
      assert source =~ "add_if_not_exists :lineage_id"
      assert source =~ "add_if_not_exists :parent_task_id"
      assert source =~ "references(:continuum_activity_tasks,"
      assert source =~ "on_delete: :nilify_all"
      assert source =~ "create_if_not_exists table(:continuum_activity_attempts"
      assert source =~ "create_if_not_exists table(:continuum_activity_operations"
    end)
  end

  test "generates the complete v0.6.4 to v0.7.0 upgrade" do
    in_tmp(fn ->
      Mix.Task.rerun("continuum.gen.migration", [
        "--repo",
        "Continuum.Test.Repo",
        "--from",
        "0.6.4"
      ])

      [path] =
        Path.wildcard("priv/test_repo/migrations/*_upgrade_continuum_v0_6_4_to_v0_7_0.exs")

      source = File.read!(path)

      assert source =~ "add :idempotency_key, :text"
      assert source =~ "continuum_signals_delivery_key_idx"
      assert source =~ "create table(:continuum_run_ingress_keys"
      assert source =~ "add :last_heartbeat_at, :utc_datetime_usec"
      assert source =~ "create table(:continuum_schedules"
      assert source =~ ~s(add :queue, :text, null: false, default: "default")
      assert source =~ "(queue, priority DESC, available_at, scheduled_at)"
      assert source =~ "@disable_ddl_transaction true"
    end)
  end

  test "rejects unsupported upgrade sources" do
    assert_raise Mix.Error, ~r/unsupported Continuum upgrade source/, fn ->
      in_tmp(fn ->
        Mix.Task.rerun("continuum.gen.migration", [
          "--repo",
          "Continuum.Test.Repo",
          "--from",
          "0.5.1"
        ])
      end)
    end
  end

  defp in_tmp(fun) do
    root = Path.join(System.tmp_dir!(), "continuum-gen-migration-#{System.unique_integer()}")
    File.rm_rf!(root)
    File.mkdir_p!(root)

    try do
      File.cd!(root, fun)
    after
      File.rm_rf(root)
    end
  end
end
