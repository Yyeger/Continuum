defmodule ContinuumExampleOrdersWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :continuum_example_orders

  plug Plug.RequestId
  plug Plug.Telemetry, event_prefix: [:phoenix, :endpoint]

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Jason

  plug ContinuumExampleOrdersWeb.Router
end
