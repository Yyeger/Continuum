# Multi-Instance Continuum

A Continuum *instance* is a named runtime — its own registry, run supervisor,
dispatchers, timer wheel, signal router, lease heartbeater, snapshotter, and
recovery process — bound to a single Ecto repo. Most applications only ever run
one instance and never have to think about this. Umbrella apps and library
hosts that genuinely have more than one repo can run more than one Continuum
instance in the same BEAM.

## The Default Instance

Continuum ships with a default instance named `Continuum`. It is started by
`Continuum.Application` whenever `config :continuum, :repo` is set:

```elixir
# config/config.exs
config :continuum, repo: MyApp.Repo
```

All public calls operate on the default instance unless you say otherwise:

```elixir
{:ok, run_id} = Continuum.start(MyApp.OrderFlow, input)
:ok           = Continuum.signal(run_id, :fraud_review, :approved)
{:ok, _}      = Continuum.await(run_id, 5_000)
```

If `:repo` is not configured, `Continuum.Application` still starts the
in-memory journal and the local PubSub/registry, but no Postgres-backed
runtime children. Host applications that wire their own instances should leave
`:repo` unset and use `Continuum.children/1`.

## Named Instances

To run a second instance side by side with the default, or to embed Continuum
in a host that owns the repo, add child specs with `Continuum.children/1`:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      MyApp.Repo,
      MyApp.Billing.Repo
    ] ++ Continuum.children(name: :billing_continuum, repo: MyApp.Billing.Repo)

    Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
  end
end
```

`Continuum.children/1` registers the instance during child-spec construction
(via `:persistent_term`), so the name is resolvable by the time the
supervisor starts the children. The returned list includes one child for each
of: PubSub, registry, run supervisor, activity worker supervisor, lease
heartbeater, dispatcher, activity dispatcher, snapshotter, timer wheel, and
signal router — all named under the instance.

Calling `Continuum.children(name: Continuum)` returns `[]`. The default
instance is owned by `Continuum.Application`; trying to start it twice would
collide on registered names.

You can compose multiple `Continuum.children/1` calls into one tree:

```elixir
children =
  [MyApp.Repo, MyApp.Billing.Repo] ++
    Continuum.children(name: :orders_continuum, repo: MyApp.Repo) ++
    Continuum.children(name: :billing_continuum, repo: MyApp.Billing.Repo)
```

Per-child options are accepted for fine-grained tuning: `:heartbeater`,
`:run_supervisor`, `:activity_supervisor`, `:recovery`, `:dispatcher`,
`:activity_dispatcher`, `:timer_wheel`, `:signal_router`, `:snapshotter`. Pass
`false` for any child to omit it from the list (for example, omit
`:activity_dispatcher` if the host wants its own custom worker pool).

## Calling Into a Named Instance

Public entry points accept an `:instance` option:

```elixir
{:ok, run_id} =
  Continuum.start(MyApp.BillingFlow, input, instance: :billing_continuum)

:ok = Continuum.signal(run_id, :invoice_paid, %{txid: "..."}, instance: :billing_continuum)
:ok = Continuum.cancel(run_id, instance: :billing_continuum)
{:ok, _} = Continuum.await(run_id, 5_000, instance: :billing_continuum)
```

The four-arity `Continuum.signal/4` keeps the payload positional; do not pass
options as the third argument.

Unknown instance names raise `Continuum.InstanceNotRegisteredError`. Register
the instance once (`Continuum.children/1`) before any call that names it.

## Run Isolation

Two instances are fully isolated:

* A run started in one instance is invisible to the other.
* The same `run_id` can exist in both instances simultaneously without
  collision — the registry, lease owner, and PubSub topics are all namespaced.
* The lease-owner string includes the instance name
  (`node()/instance/monotonic_int`), so Postgres CAS writes from one
  instance cannot accidentally validate against another.

## Durable Cancellation Without a Local Engine

`Continuum.cancel(run_id, instance: name)` works even when no Engine process is
alive for that run. The facade resolves the instance, acquires the run lease
through the Postgres journal, and runs the cancellation transaction directly.
A live engine still owning the lease causes the cancel to fail cleanly, and
the caller can retry once the engine has released or lost the lease.

This is the same code path used for normal cancellation; there is no separate
`cancel_durable/2` API.

## Telemetry and Tracing

All Continuum telemetry events include `instance: name` in metadata. Use that
key to split dashboards or filter spans when more than one instance is active
in the same BEAM. The OpenTelemetry bridge (`Continuum.OpenTelemetry.setup/1`)
emits the same attribute as `continuum.instance` on every run-attempt and
activity-attempt span.

## When to Use More Than One Instance

Run more than one instance when:

* the host application genuinely has more than one repo and each repo owns
  different business workflows
* you need hard isolation between unrelated tenants and tenant repos are
  already separate
* a library embeds Continuum and the host already owns the Continuum repo,
  so the library should not start the default instance from app env

Otherwise, keep the default. A single instance handles thousands of
workflows per second on Postgres and is the simpler operational story.
