# SQLite

Continuum can keep its durable journal in SQLite, through the
[`ecto_sqlite3`](https://hex.pm/packages/ecto_sqlite3) adapter, for
applications that run on **one node**. Workflows behave exactly as they do on
PostgreSQL: the same replay, drift detection, lease fencing, activities and
retries, timers, signals, child workflows, sagas, `continue_as_new`, schedules,
search attributes, Observer pages, and operator tooling.

PostgreSQL remains the reference backend and the right choice when more than
one node runs Continuum. Choose SQLite when a single BEAM owns the database
file: embedded and edge deployments, small services, desktop applications, or
development environments without a database server.

## Install

Add the adapter next to Continuum:

```elixir
def deps do
  [
    {:continuum, "~> 0.8.5"},
    {:ecto_sqlite3, "~> 0.25"}
  ]
end
```

`ecto_sqlite3` bundles SQLite. If you build `exqlite` against a system SQLite
instead, it must be version 3.43 or newer. Postgrex is still installed as a
Continuum dependency, but no PostgreSQL server is needed.

## Configure the repo

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.SQLite3
end
```

```elixir
config :my_app, MyApp.Repo,
  database: "priv/my_app.db",
  pool_size: 5,
  busy_timeout: 10_000,
  custom_pragmas: [busy_timeout: 10_000],
  default_transaction_mode: :immediate
```

- `journal_mode` stays at the adapter's default, WAL, so readers do not block
  the writer.
- `exqlite` applies `:busy_timeout` after its other connection pragmas. Setting
  it again in `:custom_pragmas`, which run first, lets a connection that opens
  during a write wait instead of failing with `database is locked`.
- Continuum runs its own transactions as `BEGIN IMMEDIATE` regardless of this
  setting. `default_transaction_mode: :immediate` gives your application's
  transactions — including `Continuum.Multi` ingress — the same behavior, so a
  transaction that reads and then writes cannot fail when another writer
  commits in between. Alternatively pass `mode: :immediate` to
  `Repo.transaction/2` for those transactions.
- Keep `ecto_sqlite3`'s default `:iso8601` datetime storage. Continuum compares
  timestamps as text and raises if `config :ecto_sqlite3, datetime_type:` is
  changed.

## Configure Continuum

For the default instance, select the SQLite journal explicitly:

```elixir
config :continuum, repo: MyApp.Repo, journal: Continuum.Runtime.Journal.SQLite
```

Named instances choose the journal from the repo's adapter:

```elixir
Continuum.children(name: :jobs, repo: MyApp.Repo)
```

Pairing a durable journal with a repo of the other SQL dialect raises an
`ArgumentError` when the instance is built.

Add the runtime children after the repo, exactly as for PostgreSQL:

```elixir
children = [MyApp.Repo] ++ Continuum.children() ++ [MyAppWeb.Endpoint]
```

## Create the schema

```bash
mix continuum.gen.migration --repo MyApp.Repo
mix ecto.migrate
```

The generator detects the SQLite adapter and writes the SQLite schema: the
same tables and partial indexes, a single `continuum_events` table, and a
`continuum_sequences` counter in place of the PostgreSQL lease-token sequence.
SQLite support starts with the current schema, so the `--from` upgrade
migrations are PostgreSQL-only.

## How the runtime adapts

- **Fencing.** SQLite has one writer and no row locks. Every Continuum
  transaction that reads, validates a lease token, and then writes starts with
  `BEGIN IMMEDIATE`, so it holds the writer lock from its first read. The CAS
  checks are the same ones PostgreSQL runs; lease tokens come from
  `continuum_sequences` inside the transaction that stores them.
- **Clock.** The database and the application share a host, so lease expiry,
  retry backoff, and timer due times use the application's UTC clock.
- **Wakeups.** SQLite has no `LISTEN`/`NOTIFY`. After a timer is armed, the
  journal notifies the local timer wheel once the transaction commits; signal
  deliveries and child completions wake local engines directly. The
  dispatcher poll, timer refresh, and signal catch-up scan remain the
  backstops, as they are on PostgreSQL.
- **Events.** The events table is not partitioned. Bound its size with the
  retention tools (`Continuum.Pruner`, `mix continuum.runs.prune`,
  `mix continuum.archive_continued_chains`).

## Limits

- **One node.** Clustering, cross-node wake routing, and the cluster test
  suite are PostgreSQL-only. If several BEAM nodes or OS processes open the
  same file, lease fencing still keeps every write correct, but no process is
  notified of another's commits, so wakeups wait for the polls. Never place
  the database on a network file system; SQLite's locking is not reliable
  there.
- **Write throughput.** All writes serialize on one writer. Workloads with many
  concurrent activities complete correctly but contend for that lock.
- **Partitions.** `Continuum.Partitions` and the partition mix tasks return or
  raise `:partitions_unsupported_on_sqlite`; leave `partition_maintainer`
  disabled. The health report shows partition coverage as not applicable.
- **Attribute filters.** `:eq` and `:neq` filters on search attributes support
  scalar values and maps of scalars. List values return
  `{:error, {:unsupported_attribute_value, value}}`. Numbers compare by their
  JSON text, so `4` and `4.0` are different values.
- **Oban executor.** `activity_executor: {:oban, opts}` has no SQLite-specific
  code, but requires Oban's own SQLite engine and is not covered by Continuum's
  test suite on SQLite.

## Testing

`Continuum.Test` works with SQLite instances. Pass `journal: :sqlite` where a
helper takes a journal, and `instance:` for a named instance:

```elixir
{:ok, run_id} = Continuum.start(MyApp.Checkout, order, instance: :jobs)
:ok = Continuum.Test.drive_until_state(run_id, [:suspended], instance: :jobs)
:ok = Continuum.Test.inject_signal(run_id, :approved, true, journal: :sqlite, instance: :jobs)
assert {:ok, %{state: :completed}} = Continuum.Test.drive(run_id, instance: :jobs)
```

SQLite's single writer and the SQL Sandbox do not combine well for concurrent
tests. Continuum's own suite uses a file database rebuilt at the start of each
run, synchronous test modules, and table cleanup between tests.

## Why not a SQLite queue extension?

Extensions such as [Honker](https://hex.pm/packages/honker) add
`NOTIFY`-style notifications and queues to SQLite. Continuum does not need
them for a single node: its activity tasks, timers, and signal mailbox are
already durable tables guarded by lease fencing, every write goes through the
application's Ecto repo, and in-process notifications are immediate. Such an
extension could later supply cross-process wakeups for deployments where
several OS processes share one database file.
