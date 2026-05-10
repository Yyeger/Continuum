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
  end
end
