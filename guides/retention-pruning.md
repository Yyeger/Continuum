# Retention and terminal-run pruning

Workflow retention sets `retention_until` when a run becomes terminal. Use the
terminal-run pruner to remove expired histories in bounded batches:

```console
# Dry run
mix continuum.runs.prune --repo MyApp.Repo --batch-size 100

# Execute one batch
mix continuum.runs.prune --repo MyApp.Repo --batch-size 100 --execute
```

The pruner treats a `continue_as_new` chain as one unit. Every incarnation must
be terminal and expired; chains with child-run references are skipped so no
lineage or stable-root relationship is left dangling.

Activity result keys and external start keys have a separate lifetime. They are
kept by default even after history is removed, preserving at-most-once ingress
and activity reuse:

```console
# Explicitly opt out of those guarantees for the selected batch
mix continuum.runs.prune --repo MyApp.Repo \
  --idempotency-policy delete --execute
```

Run the task repeatedly until a dry run reports zero chains. Each execution
re-locks and revalidates its plan before deleting anything.
