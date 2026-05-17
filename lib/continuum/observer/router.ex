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

    layout =
      case Keyword.get(opts, :layout) do
        {module, template} -> {Macro.expand(module, __CALLER__), template}
        nil -> nil
      end

    layout_ast = Macro.escape(layout)

    quote do
      scoped_base = Phoenix.Router.scoped_path(__MODULE__, unquote(base))
      session = %{"instance" => unquote(Macro.escape(instance)), "observer_path" => scoped_base}

      live_session_opts =
        if unquote(layout_ast) do
          [session: session, layout: unquote(layout_ast)]
        else
          [session: session]
        end

      Phoenix.LiveView.Router.live_session :"continuum_observer_#{:erlang.phash2(scoped_base)}",
                                           live_session_opts do
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
