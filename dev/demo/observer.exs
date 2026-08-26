## The Observer pane for `mix continuum.demo --observer`.
##
## Deliberately a *read-only* Continuum node: dispatcher, activity worker,
## timer wheel, and recovery are all off. Otherwise this pane would notice the
## expired lease and resume the crashed run itself — a great advert for cluster
## failover, and completely wrong for a scripted two-pane demo.

alias ContinuumDemo.{Boot, Out}

Application.put_env(:continuum, ContinuumDemo.ObserverEndpoint,
  http: [
    ip: {127, 0, 0, 1},
    port: String.to_integer(System.get_env("CONTINUUM_DEMO_PORT", "4000"))
  ],
  adapter: Bandit.PhoenixAdapter,
  server: true,
  secret_key_base: String.duplicate("continuum", 8),
  live_view: [signing_salt: "continuum-demo"]
)

defmodule ContinuumDemo.ObserverLayout do
  @moduledoc false
  use Phoenix.Component

  def root(assigns) do
    ~H"""
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="csrf-token" content={Plug.CSRFProtection.get_csrf_token()} />
        <title>Continuum Observer — crash demo</title>
        <link rel="stylesheet" href="/observer.css" />
      </head>
      <body>
        {@inner_content}
        <script src="/assets/phoenix.min.js"></script>
        <script src="/assets/phoenix_live_view.min.js"></script>
        <script>
          var csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content");
          var liveSocket = new LiveView.LiveSocket("/live", Phoenix.Socket, {
            params: {_csrf_token: csrfToken}
          });
          liveSocket.connect();
          window.liveSocket = liveSocket;
        </script>
      </body>
    </html>
    """
  end
end

defmodule ContinuumDemo.ObserverRouter do
  @moduledoc false
  use Phoenix.Router

  import Phoenix.LiveView.Router
  import Continuum.Observer.Router

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_live_flash)
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
    # The Observer ships no root layout of its own: a host app already has one.
    # This demo does not, so it supplies the surrounding HTML document here.
    plug(:put_root_layout, html: {ContinuumDemo.ObserverLayout, :root})
  end

  scope "/" do
    pipe_through(:browser)

    continuum_observer("/continuum")
  end
end

defmodule ContinuumDemo.ObserverEndpoint do
  @moduledoc false
  use Phoenix.Endpoint, otp_app: :continuum

  @session_options [store: :cookie, key: "_continuum_demo", signing_salt: "continuum-demo"]

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

  plug(Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Jason
  )

  plug(Plug.Session, @session_options)
  plug(ContinuumDemo.ObserverRouter)
end

Boot.connect!()
Boot.start_runtime!(pollers?: false)

{:ok, endpoint} = ContinuumDemo.ObserverEndpoint.start_link()
Process.unlink(endpoint)

port = Application.get_env(:continuum, ContinuumDemo.ObserverEndpoint)[:http][:port]

Out.blank()
Out.demo("Observer (read-only) → http://localhost:#{port}/continuum")
Out.demo("this node runs no dispatcher, so it will not resume the crashed run")
Out.demo("Ctrl+C twice to stop")
Out.blank()

Process.sleep(:infinity)
