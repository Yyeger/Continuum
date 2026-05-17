# Observer

Continuum Observer is an optional Phoenix LiveView UI for inspecting workflow
runs. It is not started by `Continuum.Application`; mount it in your Phoenix
router when your app directly depends on `:phoenix_live_view` and
`:phoenix_html`. Continuum declares Phoenix as optional; host applications that
mount the Observer must include the Phoenix dependencies they use.

```elixir
defmodule MyAppWeb.Router do
  use MyAppWeb, :router

  import Continuum.Observer.Router

  scope "/admin" do
    pipe_through [:browser, :authenticate_admin]

    continuum_observer "/continuum", instance: :myapp_continuum
  end
end
```

Do not mount the Observer publicly. Continuum does not ship authentication or
authorization callbacks in v0.2; the host application owns access control.

The Observer provides:

* a runs index at `/continuum` with state, workflow, run id search, and simple
  pagination
* a run detail page at `/continuum/runs/:id` with run metadata and decoded
  journal events
* operator actions for cancelling a run and sending a JSON signal payload

The index subscribes to the per-instance `"continuum:runs"` PubSub topic. That
topic intentionally receives only coarse state changes and terminal
transitions. Detail pages subscribe to the existing per-run topic,
`"continuum:run:<run_id>"`.

Event payloads are decoded with `:erlang.binary_to_term/1` because Continuum
stores its own journal data as `bytea`. Treat database write access as trusted;
the Observer is not a sandbox for malicious journal rows.

Continuum includes `priv/static/observer.css` as a small baseline stylesheet.
Copy or serve it from your host app if you want the default styling. One simple
option is to serve it directly from the Continuum dependency:

```elixir
plug Plug.Static,
  at: "/",
  from: :continuum,
  only: ~w(observer.css)
```

You can also copy it into your app's `priv/static` directory and allow it from
your existing `Plug.Static`:

```elixir
plug Plug.Static,
  at: "/",
  from: :my_app,
  gzip: false,
  only: ~w(assets fonts images favicon.ico robots.txt observer.css)
```
