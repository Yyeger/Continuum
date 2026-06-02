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
      assert source =~ "create table(:continuum_activity_results"
      assert source =~ "PRIMARY KEY (activity_module, idempotency_key)"
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
      assert source =~ "add :attributes, :map, null: false, default: %{}"
      assert source =~ "continuum_runs_parent_idx"
      assert source =~ "continuum_runs_correlation_idx"
      assert source =~ "continuum_runs_continued_from_idx"
      assert source =~ "continuum_runs_correlation_completed_idx"
      assert source =~ "continuum_runs_attributes_gin_idx"
    end)
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
