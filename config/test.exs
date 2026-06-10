import Config

config :continuum,
  repo: Continuum.Test.Repo,
  ecto_repos: [Continuum.Test.Repo],
  trusted_modules: [Continuum.Test.ImpureProbe],
  dispatcher: false,
  activity_worker: false,
  timer_wheel: false,
  signal_router: [listen?: false],
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

config :continuum, Continuum.Test.ObserverEndpoint,
  secret_key_base: String.duplicate("a", 64),
  live_view: [signing_salt: "observer-test"],
  server: false

config :logger, level: :warning
