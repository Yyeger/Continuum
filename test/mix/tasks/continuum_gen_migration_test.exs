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
