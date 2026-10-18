import Config

config :continuum,
  repo: Continuum.Test.Repo,
  ecto_repos: [Continuum.Test.Repo],
  trusted_modules: [Continuum.Test.ImpureProbe],
  dispatcher: false,
  activity_worker: false,
  timer_wheel: false,
  signal_router: [listen?: false],
  snapshotter_dispatch: :sync,
  recovery: false,
  determinism_violations: :error

config :continuum, Continuum.Test.Repo,
  username: "continuum",
  password: "continuum",
  hostname: "localhost",
  port: 5433,
  database: "continuum_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10,
  log: false,
  priv: "priv/test_repo"

# SQLite coverage runs against a file database rebuilt from the generated
# migration at the start of every suite run (see test/test_helper.exs).
config :continuum, Continuum.Test.SQLiteRepo,
  database:
    Path.expand(
      "../tmp/continuum_test#{System.get_env("MIX_TEST_PARTITION")}.sqlite3",
      __DIR__
    ),
  pool_size: 5,
  busy_timeout: 10_000,
  # exqlite applies :busy_timeout after the connect-time pragmas; custom
  # pragmas run first, so connections opened during a write wait instead of
  # failing with "database is locked".
  custom_pragmas: [busy_timeout: 10_000],
  log: false

config :continuum, Continuum.Test.ObserverEndpoint,
  secret_key_base: String.duplicate("a", 64),
  live_view: [signing_salt: "observer-test"],
  server: false

config :logger, level: :warning
