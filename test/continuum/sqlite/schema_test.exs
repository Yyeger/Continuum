defmodule Continuum.SQLite.SchemaTest do
  use ExUnit.Case, async: false

  alias Continuum.Test.{SQLiteCase, SQLiteRepo}

  @tables ~w(
    continuum_activity_attempts
    continuum_activity_operations
    continuum_activity_results
    continuum_activity_tasks
    continuum_events
    continuum_health_reviews
    continuum_recurring_schedules
    continuum_run_ingress_keys
    continuum_runs
    continuum_schedules
    continuum_sequences
    continuum_signals
    continuum_snapshots
    continuum_timers
    continuum_workflow_versions
  )

  test "the generator emits the SQLite schema for SQLite repos" do
    in_tmp(fn ->
      Mix.Task.rerun("continuum.gen.migration", ["--repo", "Continuum.Test.SQLiteRepo"])

      [path] = Path.wildcard("priv/repo/migrations/*_create_continuum_tables.exs")
      source = File.read!(path)

      assert {:ok, _ast} = Code.string_to_quoted(source)
      assert source =~ "CREATE TABLE continuum_sequences"
      assert source =~ "VALUES ('lease_token', 0)"
      assert source =~ "PRIMARY KEY (run_id, seq)"
      assert source =~ "WITHOUT ROWID"
      assert source =~ "continuum_runs_ingress_key_idx"
      assert source =~ "continuum_signals_delivery_key_idx"
      assert source =~ "CONSTRAINT continuum_recurring_contract CHECK"
      refute source =~ "PARTITION"
      refute source =~ "SEQUENCE"
      refute source =~ "USING gin"
    end)
  end

  test "SQLite installations cannot request PostgreSQL upgrade migrations" do
    assert_raise Mix.Error, ~r/PostgreSQL-only/, fn ->
      in_tmp(fn ->
        Mix.Task.rerun("continuum.gen.migration", [
          "--repo",
          "Continuum.Test.SQLiteRepo",
          "--from",
          "0.8.1"
        ])
      end)
    end
  end

  test "the generated migration migrates up and down on a fresh database" do
    database = Path.join(System.tmp_dir!(), "continuum-schema-#{System.unique_integer()}.db")
    {:ok, pid} = SQLiteRepo.start_link(name: nil, database: database, pool_size: 1)

    try do
      SQLiteRepo.put_dynamic_repo(pid)

      assert [1] = SQLiteCase.migrate!(SQLiteRepo, dynamic_repo: pid, log: false)
      assert tables() == @tables

      assert %{rows: [["lease_token", 0]]} =
               SQLiteRepo.query!("SELECT name, value FROM continuum_sequences")

      assert [1] =
               Ecto.Migrator.run(
                 SQLiteRepo,
                 [{1, SQLiteCase.migration_module()}],
                 :down,
                 all: true,
                 dynamic_repo: pid,
                 log: false
               )

      assert tables() == []
    after
      SQLiteRepo.put_dynamic_repo(SQLiteRepo)
      GenServer.stop(pid)
      Enum.each(["", "-wal", "-shm"], &File.rm(database <> &1))
    end
  end

  defp tables do
    %{rows: rows} =
      SQLiteRepo.query!("""
      SELECT name FROM sqlite_master
      WHERE type = 'table' AND name LIKE 'continuum_%'
      ORDER BY name
      """)

    List.flatten(rows)
  end

  defp in_tmp(fun) do
    root = Path.join(System.tmp_dir!(), "continuum-sqlite-migration-#{System.unique_integer()}")
    File.rm_rf!(root)
    File.mkdir_p!(root)

    try do
      File.cd!(root, fun)
    after
      File.rm_rf(root)
    end
  end
end
