defmodule Continuum.Test.ObserverAuth do
  @moduledoc false
  def on_mount(:default, _params, %{"observer_allowed" => true}, socket), do: {:cont, socket}

  def on_mount(:default, _params, _session, socket),
    do: {:halt, Phoenix.LiveView.redirect(socket, to: "/login")}
end

defmodule Continuum.Test.ObserverRouter do
  @moduledoc false

  use Phoenix.Router

  import Phoenix.LiveView.Router
  import Continuum.Observer.Router

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_live_flash)
  end

  scope "/" do
    pipe_through(:browser)

    continuum_observer("/continuum")
  end

  scope "/" do
    pipe_through(:browser)

    continuum_observer("/named-continuum", instance: :observer_named_instance)
    continuum_observer("/guarded-continuum", on_mount: [Continuum.Test.ObserverAuth])
  end
end
