defmodule Continuum.Observer.Router do
  @moduledoc """
  Router macro for mounting the optional Continuum Observer.

  Mount the Observer only inside an authenticated admin scope:

      import Continuum.Observer.Router

      scope "/admin" do
        pipe_through [:browser, :authenticate_admin]
        continuum_observer "/continuum", instance: :myapp_continuum
      end

  Continuum ships no built-in Observer authentication.
  """

  @doc """
  Defines the Observer routes under `path`.

  Options:

    * `:instance` - Continuum instance name. Defaults to `Continuum`.
    * `:layout` - Optional `{Module, :function}` LiveView layout passed through
      to `Phoenix.LiveView.Router.live_session/3`. Use this when you need a
      custom HTML chrome around the Observer (for example to load the LV.js
      client from a development demo).
  """
  defmacro continuum_observer(path, opts \\ []) do
    unless Code.ensure_loaded?(Phoenix.LiveView.Router) do
      raise ArgumentError,
            "Continuum.Observer requires phoenix_live_view. Add phoenix_live_view and phoenix_html to your app before mounting the Observer."
    end

    path = Macro.expand(path, __CALLER__)

    unless is_binary(path) do
      raise ArgumentError, "continuum_observer/2 expects a literal path string"
    end

    base = "/" <> (path |> String.trim() |> String.trim("/"))
    instance = Keyword.get(opts, :instance, Continuum)
    layout = Keyword.get(opts, :layout)
    session = %{"instance" => instance, "observer_path" => base}
    live_session_name = :"continuum_observer_#{:erlang.phash2(base)}"

    session_kv = quote do: {:session, unquote(Macro.escape(session))}

    layout_kv =
      if layout do
        [quote(do: {:layout, unquote(layout)})]
      else
        []
      end

    live_session_opts_ast = [session_kv | layout_kv]

    quote do
      Phoenix.LiveView.Router.live_session unquote(live_session_name),
                                           unquote(live_session_opts_ast) do
        Phoenix.LiveView.Router.live(
          unquote(base),
          Continuum.Observer.RunsLive,
          :index,
          as: :continuum_observer
        )

        Phoenix.LiveView.Router.live(
          unquote(base) <> "/runs/:id",
          Continuum.Observer.RunLive,
          :show,
          as: :continuum_observer_run
        )
      end
    end
  end
end
