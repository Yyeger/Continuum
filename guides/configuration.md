# Runtime Configuration Reference

This file is generated from `Continuum.Config.schema/0`. Configure a runtime
through `Continuum.children/1`; unknown, duplicate, or invalid options raise
before child specs are returned.

| Option | Type | Default | Description |
|---|---|---|---|
| `name` | `atom` | `Continuum` | Runtime instance name. |
| `repo` | `module \| nil` | `configured repo` | Ecto repo used by durable components. |
| `journal` | `module \| nil` | `derived` | Journal adapter override. |
| `workflow_modules` | `[module] \| nil` | `discovered` | Workflow modules registered at boot. |
| `activity_executor` | `:builtin \| {:oban, keyword}` | `:builtin` | Activity execution backend. |
| `activity_max_concurrency` | `positive_integer` | `10` | Instance-wide built-in worker limit. |
| `activity_queues` | `map \| keyword` | `%{}` | Per-queue positive concurrency limits. |
| `heartbeater` | `boolean \| keyword` | `[]` | Lease heartbeater child options. |
| `run_supervisor` | `boolean \| keyword` | `[]` | Run supervisor child options. |
| `activity_supervisor` | `boolean \| keyword` | `[]` | Activity supervisor child options. |
| `recovery` | `boolean \| keyword` | `[]` | Startup recovery child options. |
| `dispatcher` | `boolean \| keyword` | `[]` | Run dispatcher child options. |
| `activity_dispatcher` | `boolean \| keyword` | `[]` | Activity dispatcher child options. |
| `timer_wheel` | `boolean \| keyword` | `[]` | Timer wheel child options. |
| `schedule_runner` | `boolean \| keyword` | `[]` | One-shot schedule runner child options. |
| `signal_router` | `boolean \| keyword` | `[]` | Signal router child options. |
| `snapshotter` | `boolean \| keyword` | `[]` | Snapshotter child options. |
| `partition_maintainer` | `boolean \| keyword` | `false` | Optional partition DDL child options. |
| `version_registry` | `boolean \| keyword` | `[]` | Workflow version registrar child options. |

## Child option keys

Each child accepts `false` to omit it, `true` for defaults, or a keyword list:

- `activity_dispatcher`: `enabled?`, `interval_ms`, `batch_size`, `ttl_seconds`, `backpressure_jitter_ms`
- `activity_supervisor`: no child-specific options
- `dispatcher`: `enabled?`, `interval_ms`, `batch_size`, `ttl_seconds`
- `heartbeater`: `name`, `interval_ms`, `ttl_seconds`, `drain_timeout_ms`
- `partition_maintainer`: `months`, `start_month`, `interval_ms`, `initial_delay_ms`
- `recovery`: `enabled?`, `name`
- `run_supervisor`: no child-specific options
- `schedule_runner`: `interval_ms`, `batch_size`
- `signal_router`: `listen?`, `catch_up_interval_ms`, `catch_up_batch_size`
- `snapshotter`: `snapshot_threshold`, `snapshot_max_size_bytes`, `journal`
- `timer_wheel`: `enabled?`, `listen?`, `refresh_ms`, `window_ms`, `batch_size`
- `version_registry`: `workflow_modules`, `registration_fun`, `retry_base_ms`, `retry_max_ms`

Regenerate with `mix continuum.config.docs`; CI can verify the committed file
with `mix continuum.config.docs --check`.
