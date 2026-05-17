defmodule ContinuumExampleOrdersWeb.Router do
  use Phoenix.Router

  import Continuum.Observer.Router
  import Phoenix.LiveView.Router

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_live_flash)
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
  end

  pipeline :admin_auth do
    plug(:admin_basic_auth)
  end

  pipeline :api do
    plug(:accepts, ["json"])
  end

  scope "/admin" do
    pipe_through([:browser, :admin_auth])

    continuum_observer("/continuum",
      instance: :continuum_example_orders,
      layout: {ContinuumExampleOrdersWeb.Layouts, :app}
    )
  end

  scope "/", ContinuumExampleOrdersWeb do
    pipe_through(:api)

    post("/orders", OrderController, :create)
    post("/runs/:run_id/fraud-review", OrderController, :fraud_review)
  end

  defp admin_basic_auth(conn, _opts) do
    Plug.BasicAuth.basic_auth(conn, username: "admin", password: "admin")
  end
end
