defmodule Continuum.Test.ObserverEndpoint do
  @moduledoc false

  use Phoenix.Endpoint, otp_app: :continuum

  @session_options [
    store: :cookie,
    key: "_continuum_observer_test",
    signing_salt: "observer-test"
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

  plug(Plug.Static, at: "/", from: :continuum, only: ~w(observer.css))
  plug(Plug.Session, @session_options)
  plug(Continuum.Test.ObserverRouter)
end
