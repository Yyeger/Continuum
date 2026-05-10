## Continuum Observer demo
##
## Boots the Observer against a real Postgres so you can click around in a
## browser. Run with:
##
##   docker compose up -d
##   MIX_ENV=test iex -S mix run dev/observer_demo.exs
##
## then open http://localhost:4000/continuum in your browser.

# 1. Stop the app booted with the default test config so we can override.
Application.stop(:continuum)

# 2. Point the test repo at a separate "observer demo" database with a
#    standard pool (the sandbox pool blocks LiveView mounts).
Application.put_env(:continuum, Continuum.Test.Repo,
  username: "continuum",
  password: "continuum",
  hostname: "localhost",
  port: 5433,
  database: "continuum_observer_demo",
  pool_size: 10,
  log: false,
  priv: "priv/test_repo"
)

# 3. Re-enable the runtime children that test.exs disables, so signals,
#    timers, and dispatchers actually run.
Application.put_env(:continuum, :dispatcher, [])
Application.put_env(:continuum, :activity_worker, [])
Application.put_env(:continuum, :timer_wheel, [])
Application.put_env(:continuum, :signal_router, [])
Application.put_env(:continuum, :recovery, [])

# 4. Configure the demo endpoint.
Application.put_env(:continuum, ObserverDemo.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4000],
  adapter: Phoenix.Endpoint.Cowboy2Adapter,
  server: true,
  secret_key_base: String.duplicate("a", 64),
  live_view: [signing_salt: "observer-demo"]
)

# 5. Inline modules: layout, router, endpoint.
defmodule ObserverDemo.Layout do
  use Phoenix.Component

  def app(assigns) do
    ~H"""
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="csrf-token" content={Plug.CSRFProtection.get_csrf_token()} />
        <title>Continuum Observer (demo)</title>
        <link rel="stylesheet" href="/observer.css" />
      </head>
      <body>
        <%= @inner_content %>
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

defmodule ObserverDemo.Router do
  use Phoenix.Router

  import Phoenix.LiveView.Router
  import Continuum.Observer.Router

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_live_flash)
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
  end

  scope "/" do
    pipe_through(:browser)

    continuum_observer("/continuum", layout: {ObserverDemo.Layout, :app})
  end
end

defmodule ObserverDemo.Endpoint do
  use Phoenix.Endpoint, otp_app: :continuum

  @session_options [
    store: :cookie,
    key: "_observer_demo",
    signing_salt: "observer-demo"
  ]

  socket("/live", Phoenix.LiveView.Socket,
    websocket: [connect_info: [session: @session_options]]
  )

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
  plug(ObserverDemo.Router)
end

# 6. Create the observer-demo database if needed BEFORE the runtime starts.
repo_config = Application.get_env(:continuum, Continuum.Test.Repo)
_ = Ecto.Adapters.Postgres.storage_up(repo_config)

# 7. Start the repo (no test_helper.exs runs under `mix run`, so we must do it
#    manually). Unlink so the repo survives the eval process exiting.
repo_pid =
  case Continuum.Test.Repo.start_link() do
    {:ok, pid} -> pid
    {:error, {:already_started, pid}} -> pid
  end

Process.unlink(repo_pid)

# 8. Run migrations against the freshly created DB.
Ecto.Migrator.run(
  Continuum.Test.Repo,
  "priv/test_repo/migrations",
  :up,
  all: true,
  log: false
)

# 9. Re-start Continuum with the new config.
{:ok, _} = Application.ensure_all_started(:continuum)

# 8. Boot the demo endpoint. Unlink so the endpoint survives eval process exit.
endpoint_pid =
  case ObserverDemo.Endpoint.start_link() do
    {:ok, pid} -> pid
    {:error, {:already_started, pid}} -> pid
  end

Process.unlink(endpoint_pid)

# 9. Sample workflows so the Observer has something interesting to display.
defmodule ObserverDemo.SideEffectFlow do
  use Continuum.Workflow, version: 1

  def run(input) do
    Continuum.side_effect(fn -> {:ok, input} end)
  end
end

defmodule ObserverDemo.SignalFlow do
  use Continuum.Workflow, version: 1

  def run(_input) do
    case await(signal(:approve)) do
      :ok -> :approved
      other -> {:rejected, other}
    end
  end
end

defmodule ObserverDemo.TimerFlow do
  use Continuum.Workflow, version: 1

  def run(_input) do
    timer(seconds(120))
    :done
  end
end

# 10. Seed a few runs in different states.
{:ok, _} =
  Continuum.Test.start_postgres(ObserverDemo.SideEffectFlow, %{value: "completed run"})

{:ok, _} =
  Continuum.Test.start_postgres(ObserverDemo.SignalFlow, %{name: "waiting on signal"})

{:ok, _} =
  Continuum.Test.start_postgres(ObserverDemo.TimerFlow, %{name: "waiting on timer"})

IO.puts("""

  Continuum Observer demo running.

    Open: http://localhost:4000/continuum

  iex helpers (for spawning more runs / driving them outside the UI):

    {:ok, id} = Continuum.Test.start_postgres(ObserverDemo.SignalFlow, %{name: "another"})
    Continuum.signal(id, :approve, :ok, journal: Continuum.Runtime.Journal.Postgres)
    Continuum.cancel(id, journal: Continuum.Runtime.Journal.Postgres)

  Press Ctrl+C twice to stop.
""")
