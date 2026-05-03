import Config

config :continuum_example_orders,
  ecto_repos: [ContinuumExampleOrders.Repo]

config :continuum_example_orders, ContinuumExampleOrders.Repo,
  database: "continuum_example_orders_dev",
  hostname: "localhost",
  port: 5432,
  username: "postgres",
  password: "postgres",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :continuum,
  repo: ContinuumExampleOrders.Repo,
  journal: Continuum.Runtime.Journal.Postgres,
  dispatcher: true,
  activity_worker: true,
  timer_wheel: true,
  signal_router: true,
  recovery: true

config :continuum_example_orders, ContinuumExampleOrdersWeb.Endpoint,
  url: [host: "localhost"],
  http: [ip: {127, 0, 0, 1}, port: 4000],
  server: true,
  secret_key_base: String.duplicate("a", 64),
  render_errors: [formats: [json: ContinuumExampleOrdersWeb.ErrorJSON]],
  pubsub_server: ContinuumExampleOrders.PubSub,
  live_view: [signing_salt: "continuum"]

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]
