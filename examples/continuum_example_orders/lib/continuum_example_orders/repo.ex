defmodule ContinuumExampleOrders.Repo do
  use Ecto.Repo,
    otp_app: :continuum_example_orders,
    adapter: Ecto.Adapters.Postgres
end
