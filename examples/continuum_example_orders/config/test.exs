import Config

# The example app's tests are in-memory unit tests: they demonstrate that a
# Continuum workflow can be driven end to end, including its child workflows,
# with no database at all. The Repo and the Phoenix endpoint are left out of
# the supervision tree in this environment so `mix test` needs nothing running.
config :continuum_example_orders, start_infrastructure?: false

config :continuum_example_orders, ContinuumExampleOrdersWeb.Endpoint, server: false

config :logger, level: :warning

config :opentelemetry, traces_exporter: :none
