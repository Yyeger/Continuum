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

config :continuum_example_orders, ContinuumExampleOrdersWeb.Endpoint,
  url: [host: "localhost"],
  http: [ip: {127, 0, 0, 1}, port: 4000],
  server: true,
  secret_key_base: String.duplicate("a", 64),
  render_errors: [formats: [json: ContinuumExampleOrdersWeb.ErrorJSON]],
  pubsub_server: ContinuumExampleOrders.PubSub,
  live_view: [signing_salt: "continuum"]

config :opentelemetry, :resource, service: %{name: "continuum_example_orders"}

config :opentelemetry,
  traces_exporter: :otlp

config :opentelemetry_exporter,
  otlp_protocol: :http_protobuf,
  otlp_endpoint: "http://localhost:4318"

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]
