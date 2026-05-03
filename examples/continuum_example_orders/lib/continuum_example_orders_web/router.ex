defmodule ContinuumExampleOrdersWeb.Router do
  use Phoenix.Router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", ContinuumExampleOrdersWeb do
    pipe_through :api

    post "/orders", OrderController, :create
    post "/runs/:run_id/fraud-review", OrderController, :fraud_review
  end
end
