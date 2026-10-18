defmodule Continuum.Test.SQLiteCase do
  @moduledoc """
  ExUnit case template for tests that run Continuum on SQLite.

      use Continuum.Test.SQLiteCase

  The suite shares one SQLite file built from the generated migration in
  `test/test_helper.exs`. SQLite has a single writer and no SQL Sandbox
  isolation, so these cases are synchronous and every test starts from empty
  tables.
  """

  use ExUnit.CaseTemplate

  alias Continuum.Test.SQLiteRepo

  @tables ~w(
    continuum_health_reviews
    continuum_workflow_versions
    continuum_snapshots
    continuum_activity_results
    continuum_activity_operations
    continuum_activity_attempts
    continuum_activity_tasks
    continuum_timers
    continuum_signals
    continuum_events
    continuum_schedules
    continuum_recurring_schedules
    continuum_run_ingress_keys
    continuum_runs
  )

  using do
    quote do
      @moduletag :sqlite

      import Ecto.Query
      alias Continuum.Test.SQLiteRepo
    end
  end

  setup do
    reset!()
    :ok
  end

  @doc "Delete every Continuum row from the shared SQLite test database."
  def reset!(repo \\ SQLiteRepo) do
    Enum.each(@tables, &repo.query!("DELETE FROM #{&1}"))
  end

  @doc "Recreate the SQLite test database from the generated migration."
  def setup_database!(repo \\ SQLiteRepo) do
    database = Keyword.fetch!(repo.config(), :database)
    File.mkdir_p!(Path.dirname(database))
    Enum.each(["", "-wal", "-shm"], &File.rm(database <> &1))

    # Switching a new file to WAL needs an exclusive lock; doing it from one
    # connection, and keeping that connection open until the pool is up so
    # the WAL files are not torn down in between, keeps the pool from racing.
    {:ok, initializer} = repo.start_link(name: nil, pool_size: 1)
    repo.put_dynamic_repo(initializer)
    repo.query!("SELECT 1")
    repo.put_dynamic_repo(repo)

    case repo.start_link() do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    GenServer.stop(initializer)

    migrate!(repo)
  end

  @doc "Run the generated Continuum SQLite migration against `repo`."
  def migrate!(repo \\ SQLiteRepo, opts \\ []) do
    Ecto.Migrator.run(repo, [{1, migration_module()}], :up, Keyword.put(opts, :all, true))
  end

  @doc "Compile the generated SQLite migration once and return its module."
  def migration_module do
    module = Continuum.Test.SQLiteRepo.Migrations.CreateContinuumTables

    if Code.ensure_loaded?(module) do
      module
    else
      inspect(module)
      |> Mix.Tasks.Continuum.Gen.Migration.sqlite_migration_source()
      |> Code.compile_string()

      module
    end
  end
end
