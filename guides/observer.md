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

    continuum_observer "/continuum",
      instance: :myapp_continuum,
      on_mount: [{MyAppWeb.AdminAuth, :require_admin}]
  end
end
```

Do not mount the Observer publicly. Continuum does not ship authentication or
authorization logic; the host application owns access control. The optional
`on_mount:` hooks run in the LiveView session as well as on reconnects. Use
them to enforce the same admin access as the HTTP pipeline.

The Observer provides:

* a runs index at `/continuum` with state, workflow, run id search, and stable
  cursor pagination
* a run detail page at `/continuum/runs/:id` with run metadata and decoded
  journal events
* operator actions for cancelling a run and sending a JSON signal payload
* read-only replay diagnostics on each run detail page
* an operational health panel at `/continuum/health` backed by
  `Continuum.Health`, including partition/version drift, run wait reasons,
  durable wake and timer lag, lease heartbeats, activity queues, dead-letter
  candidates, and signal backlog

Health repairs in the panel always perform a dry-run preview first. The
confirmation step executes the same fenced, idempotent repair API exposed by
`mix continuum.health`; stale lease epochs, owners, and attempts are rejected.

The index subscribes to the per-instance `"continuum:runs"` PubSub topic. That
topic intentionally receives only coarse state changes and terminal
transitions. Detail pages subscribe to the existing per-run topic,
`"continuum:run:<run_id>"`.

Event payloads are decoded with `:erlang.binary_to_term/2` using `[:safe]` because Continuum
stores its own journal data as `bytea`. Treat database write access as trusted;
the Observer is not a sandbox for malicious journal rows.

Run and event payloads are bounded before decoding. Configure a unary redactor
globally with `config :continuum, observer_redactor: MyApp.Redactor`, where the
module exports `redact/1`, or pass `redactor:` and `max_payload_bytes:` to the
Observer query helpers. Oversized payloads are represented by an omission map
that includes their encoded byte size. Event pages use sequence cursors and the
display renderer also applies a hard byte cap.

## Replay diagnostics

The **Replay history** button resolves the run's stored workflow version and
compares code with recorded history. It shows outcome, loaded event count,
resolved entrypoint, snapshot use, stored-result agreement, and drift cursor
with expected/actual commands. Missing historical code is reported explicitly;
the Observer never silently substitutes the latest workflow version.

Replay runs asynchronously so the LiveView remains responsive. Each view can
run one replay at a time. It uses the read-only replay kernel: no activities,
signals, timers, or workflow mutations are performed. Reports are observations
of loaded history; a live run may progress while the report is being prepared.

The default limits are 2 seconds, 2,000 raw events, 8 MiB of combined encoded
snapshot/history data, and 64 KiB per input/result/event payload. PostgreSQL
applies byte limits before transferring payloads. An isolated worker also has
a heap cap and is terminated when its caller disappears. Oversized history,
timeouts, and unavailable code produce a diagnostic error rather than a partial
success report. Result and drift details use `observer_redactor`; redactor
failures produce a generic error without exposing the original payload.

The Phoenix-independent `Continuum.Observer.replay_report/2` helper exposes
the same report and accepts bounded overrides documented in its API. The CLI
remains available for larger offline investigations. These read limits do not
turn workflow code or a malicious database into a security sandbox.

## Host styling

The underlying `Continuum.Query` API supports nested, type-preserving attribute
filters without abandoning the generated JSONB GIN index:

```elixir
Continuum.list_runs(
  where: [{:eq, [:attributes, :customer, :profile, :tier], 4}],
  cursor: previous_page.next_cursor
)
```

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
