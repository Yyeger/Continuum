# Migrating from v0.7.1 to v0.7.2

v0.7.2 is a patch release. It does not change the workflow, snapshot, or event
history formats, and it adds no new public API. It does add one data migration,
and it makes `Continuum.AstCheck` reject calls that previously only warned — so
some workflow modules that compiled on v0.7.1 will now fail to compile.

## Upgrade order

1. Apply the schema change below while v0.7.1 is still running. It rewrites only
   terminal rows and the old runtime keeps reading them correctly.
2. Fix any compile errors from the widened determinism denylist (below).
3. Deploy v0.7.2 and restart the Continuum runtime normally.

No configuration change is required.

## Schema changes

Generate the delta migration in your application:

```bash
mix continuum.gen.migration --from 0.7.1 --repo MyApp.Repo
```

It promotes runs that were cancelled before v0.5.2 into the canonical terminal
state:

```sql
UPDATE continuum_runs
SET state = 'cancelled'
WHERE state = 'failed' AND error = $1  -- :erlang.term_to_binary(:cancelled)
```

Those rows stored a cancel as `failed` plus an encoded `:cancelled` error
payload, so every read path had to recognise that shape by comparing encoded
bytes in SQL. `Continuum.query/2`'s `state` filter now compares the state column
alone, which is both correct on its own merits and a prerequisite for the payload
codec planned for v0.9 — under any non-deterministic encoding the byte comparison
would silently stop matching and legacy cancelled runs would reclassify as
failures with no error.

**If you skip this migration**, legacy cancelled runs are returned by
`state: :failed` rather than `state: :cancelled`. They still *display* as
cancelled (`Continuum.Query` decodes the payload in Elixir, which is encoding
agnostic), so the filter and the displayed state can disagree until you run it.
Deployments created on v0.5.2 or later have no such rows and the migration is a
no-op.

## Determinism denylist: breaking for some workflow modules

`Continuum.AstCheck` now rejects calls that previously fell through to the
generic "cannot determine whether this module is deterministic" warning. Each one
is a hard `CompileError` with a targeted remediation hint.

### `Logger` is forbidden in workflow and `Continuum.Pure` code

Workflow code replays, so a direct `Logger` call duplicates the breadcrumb on
every wake. Use `Continuum.log/2`, which journals the log at its command site and
emits Logger and telemetry only at the live tail:

```elixir
# Before — compiled on v0.7.1, duplicated on every replay
Logger.info("charging order #{input.id}")

# After
Continuum.log(:info, "charging order #{input.id}")
```

Every level maps directly (`:debug` through `:emergency`), and `Logger.log/3` and
`Logger.bare_log/3` map to `Continuum.log(level, message)`. `Logger.metadata/1`
and the process-level functions are rejected outright: Logger process state is
not durable, so pass that context as workflow input instead.

Logging from an activity is unchanged — activities do not replay, so `Logger`
remains correct there.

### Newly rejected standard-library calls

| Rejected | Use instead |
|---|---|
| `:timer.sleep/1` and the rest of the `:timer` scheduling API | `timer/1` |
| the whole `File` and `:file` surface | an activity |
| the whole `:ets`, `:dets`, `:mnesia`, `:counters`, `:atomics` surface | an activity |
| `:httpc`, `:gen_tcp`, `:gen_udp`, `:ssl` | an activity |
| the host and socket parts of `:inet` | an activity |
| `IO.gets/1,2`, `IO.read/1,2`, `IO.binread/2`, `IO.binwrite/2`, `IO.warn/1,2` | workflow input, or `Continuum.log/2` |

The pure functions of the enumerated modules are untouched:
`:timer.seconds/1`, `IO.iodata_to_binary/1`, and `:inet.ntoa/1` still compile.

The denylist now has two layers, both introspectable:

```elixir
Continuum.AstCheck.forbidden_calls()    # {module, function} => hint
Continuum.AstCheck.forbidden_modules()  # module => hint, banned in full
```

A module banned in full cannot be re-allowed with `config :continuum,
trusted_modules:` or `use Continuum.Pure` — those only affect the untrusted-helper
warning. Move the call into an activity.

### The implicit `catch` spelling now warns

The suspend-swallow warning previously only fired for an explicit
`try do ... catch ... end`. It now also fires for a `catch` written directly in
the function body, which is the more idiomatic spelling:

```elixir
def run(input) do
  activity Payments.charge(input)
catch
  _, _ -> :swallowed        # now warns; previously silent
end
```

This is a warning, not an error, and `Continuum.SuspendLeakError` remains the
runtime backstop. `rescue` and `after` are unaffected.

## Behaviour changes worth knowing

- **Retry jitter survives at maximum backoff.** `Policy.backoff_ms/2` reserves the
  jitter window below `max_backoff_ms` instead of clamping the sum, so a cohort of
  retries that has reached the cap stays spread across
  `max_backoff_ms - jitter_ms` through `max_backoff_ms`. Previously jitter became
  a no-op exactly at the cap. Delays never exceed `max_backoff_ms`, as before.

- **A failing schedule start backs off and eventually fails.** A schedule whose run
  cannot be started retries on an exponential jittered backoff from five seconds to
  five minutes rather than every five seconds forever, and after twelve attempts
  moves to the terminal `failed` state. New telemetry:
  `[:continuum, :schedule, :retried]` and `[:continuum, :schedule, :failed]`
  (`[:continuum, :schedule, :started]` is now documented too). Failed schedules are
  actionable findings under `report.schedules` and make
  `Continuum.Health.report/1` `:degraded`; a terminal schedule is never retried
  automatically, so deploy the missing version and create a new schedule.

- **`Continuum.Health.report/1` gains a `:schedules` section**
  (`failed_count`, `failed`, `due_count`, `oldest_due_at`, `due_lag_ms`).

- **Dead-letter classification reads the run's state, not the task's error payload.**
  An activity task discarded because its run was cancelled is now identified by
  joining `continuum_runs.state`. A task whose error term merely happens to be
  `:cancelled` on a live run is therefore now reported as a dead letter, which it
  is; conversely, a task dead-lettered before its run was cancelled is no longer
  reported. Tasks whose run row is gone stay visible.

- **Notification listeners retry instead of going deaf for the life of the node.**
  `Continuum.Runtime.TimerWheel` and `Continuum.Runtime.SignalRouter` now trap
  exits, survive an unreachable Postgres at boot, retry the `LISTEN` every five
  seconds, and re-attach after a notifier crash. A node that booted during a
  Postgres blip previously lost `continuum_timer_armed` permanently and fell back
  to the periodic refresh.

- **`deliver_signal!/5` rolls back instead of reporting `:delivered` for a row it
  did not insert**, and its mailbox insert now names the delivery-ID index in
  `conflict_target`. Behaviour is unchanged for existing deployments; the previous
  form would have turned a future unique index into silent signal loss reported as
  success.

## Removed

- `consume_signal/4` on `Continuum.Runtime.Journal.Postgres` is deleted. It had no
  callers, was not part of the `Continuum.Runtime.Journal` behaviour, and journaled
  `signal_received` without a command id — and a missing command id matches any
  await at the cursor, so such an event replayed as a wildcard. The live paths
  (`consume_pending_signal!/6`, `resolve_signal_await/4`) all stamp the id and are
  unchanged. `Continuum.Runtime.*` is internal API; no supported code path used it.
