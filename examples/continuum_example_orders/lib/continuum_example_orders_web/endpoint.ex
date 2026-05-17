defmodule ContinuumExampleOrdersWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :continuum_example_orders

  @session_options [
    store: :cookie,
    key: "_continuum_example_orders_key",
    signing_salt: "continuum"
  ]

  socket("/live", Phoenix.LiveView.Socket, websocket: [connect_info: [session: @session_options]])

  plug(Plug.Static,
    at: "/assets",
    from: {:phoenix, "priv/static"},
    only: ~w(phoenix.min.js phoenix.min.js.map)
  )

  plug(Plug.Static,
    at: "/assets",
    from: {:phoenix_live_view, "priv/static"},
    only: ~w(phoenix_live_view.min.js phoenix_live_view.min.js.map)
  )

  plug(Plug.Static,
    at: "/",
    from: :continuum,
    only: ~w(observer.css)
  )

  plug(Plug.RequestId)
  plug(Plug.Telemetry, event_prefix: [:phoenix, :endpoint])

  plug(Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Jason
  )

  plug(Plug.Session, @session_options)
  plug(ContinuumExampleOrdersWeb.Router)
end
