# Oban Activity Executor

Continuum can route activity execution through a host-operated Oban queue while
keeping Continuum's durable workflow semantics unchanged.

Oban runs activities only. Workflow engines, replay, leases, timers, signals,
idempotency, retry policy, and completion writes remain owned by Continuum.

## When to Use It

Use the Oban executor when your application already operates Oban and wants
activity attempts visible in Oban's queueing and operational tools.

Keep the built-in executor when you do not already need Oban. It has fewer
moving parts and remains the default.

## Setup

Add Oban to the host application's dependencies and run Oban's own migration:

```elixir
{:oban, "~> 2.20"}
```

Start Oban in the host supervision tree with a queue for Continuum activities:

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

For the default Continuum instance:

```elixir
config :continuum,
  repo: MyApp.Repo,
  activity_executor: {:oban, queue: :continuum_activities}
```

## Semantics

Continuum still inserts one row in `continuum_activity_tasks` when a workflow
schedules an activity. The activity dispatcher enqueues an Oban job containing
only the Continuum instance, task id, and task attempt.

The Oban worker claims the Continuum task row when the job performs. That claim
captures the run's current fencing token immediately before the activity MFA
runs. Completion then uses the same `Journal.Postgres.complete_activity_task!/5`,
failure, retry, compensation, and idempotency paths as the built-in worker.

This means a job can sit in `oban_jobs` without holding a Continuum task lease.
Duplicate or stale Oban jobs are harmless: only one job can claim a matching
`{task_id, attempt}` and the rest no-op.

Oban retries are disabled for Continuum jobs. Continuum owns all activity retry
and backoff through `use Continuum.Activity`.

## Configuration

Supported executor options:

```elixir
activity_executor:
  {:oban,
   queue: :continuum_activities,
   name: Oban,
   unique_period: 60,
   ttl_seconds: 30}
```

`:queue` selects the Oban queue. `:name` selects a non-default Oban supervision
tree. `:unique_period` controls Oban duplicate-insert suppression for
`{instance, task_id, attempt}`. `:ttl_seconds` controls the Continuum task lease
once the Oban worker has claimed the task.

Oban uniqueness is operational hygiene, not correctness. Correctness comes from
the perform-time Continuum task claim and completion CAS.
