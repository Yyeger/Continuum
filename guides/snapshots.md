# Snapshots

Snapshots are an opt-in feature for workflows with very long histories. They
reduce the cost of repeatedly loading, decoding, and matching old journal
events. They do not snapshot a BEAM continuation.

Workflow code still re-executes from the top on every resume. A snapshot is a
compacted history prefix: each compacted step validates that the workflow is
asking for the same effect at the same command identity, then returns the
previously journaled result and advances the cursor by the number of raw events
covered by that step.

## Configuration

Snapshots are disabled by default:

```elixir
config :continuum,
  snapshot_threshold: :infinity,
  snapshot_max_size_bytes: 1_000_000
```

Set `:snapshot_threshold` to a positive integer to opt in. For example,
`snapshot_threshold: 200` asks Continuum to consider taking a new snapshot after
roughly 200 additional events since the previous snapshot. `:snapshot_max_size_bytes`
is a safety cap for the encoded snapshot payload; oversized snapshots are
skipped and emit `[:continuum, :snapshot, :skipped]`.

Workflows can override the app default:

```elixir
defmodule MyApp.SubscriptionFlow do
  use Continuum.Workflow, version: 1, snapshot_threshold: 500
end
```

Resolution order is workflow option, then runtime/app configuration, then
`:infinity`.

## What Gets Compacted

Continuum compacts completed effect shapes only:

- `side_effect` and fast-path `signal_received` become one compacted step.
- `activity_scheduled` plus `activity_completed` or `activity_failed` become
  one compacted activity step.
- `signal_awaited` plus the winning `signal_received` or timeout `timer_fired`
  become one compacted await step.
- `timer_started` plus `timer_fired` become one compacted timer step.

Pending scheduled events are not covered by a snapshot. The snapshotter stops at
the last complete step and leaves any incomplete tail in raw history.

Snapshot steps require a recorded `command_id`. Histories from before structured
command identity, or manually injected events that omit it, are skipped rather
than compacted. This is deliberate: a snapshot step without command identity
would match too broadly and could hide replay drift.

## Operational Notes

Keep snapshots disabled unless you have a workflow whose replay cost is
dominated by old journal history. For high-value workflows, run replay tests
both with and without snapshots and compare results.

The table is `continuum_snapshots`. Payloads are opaque Erlang external terms
inside a versioned Continuum envelope. v0.4 writes format version `1`, stores
that value in `continuum_snapshots.format_version`, and still reads legacy
v0.2/v0.3 unversioned payloads as version `1`. Future snapshot shape changes
must either decode version `1` or raise a clear unsupported-format error.

Snapshots are not a deserialization boundary for untrusted input. Continuum
decodes snapshot blobs that it previously wrote to the application's database;
do not accept arbitrary external snapshot payloads.
