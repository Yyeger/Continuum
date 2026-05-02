import Config

config :continuum,
  repo: Continuum.Test.Repo,
  ecto_repos: [Continuum.Test.Repo],
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

config :logger, level: :warning
