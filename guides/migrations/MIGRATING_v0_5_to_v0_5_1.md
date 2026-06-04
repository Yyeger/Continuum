# Migrating from v0.5 to v0.5.1

v0.5.1 adds the optional `Continuum.Oban` activity executor. Existing
applications using the built-in worker pool do not need to change anything.

## Optional: Route Activities Through Oban

Add Oban to your application and run Oban's migration if you have not already:

```elixir
{:oban, "~> 2.20"}
```

Then start Oban and configure Continuum:

```elixir
children = [
  MyApp.Repo,
  {Oban, repo: MyApp.Repo, queues: [continuum_activities: 10]},
  Continuum.children(
    name: :flows,
    repo: MyApp.Repo,
    activity_executor: {:oban, queue: :continuum_activities}
  )
]
```

The Oban executor does not change Continuum migrations or workflow histories.
Continuum still owns activity retries, idempotency, timeout handling, and
fencing-token completion writes.

## Telemetry

Activity and compensation telemetry metadata now includes `executor:
:builtin | :oban`. Oban-backed activity attempts also include `oban_job_id`.
