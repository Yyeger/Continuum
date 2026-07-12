# Patching Workflows

`Continuum.patched?/1` is for small, backward-compatible changes that should
take effect for new executions without moving old in-flight runs onto a new
branch mid-replay.

```elixir
def run(input) do
  if Continuum.patched?(:add_fraud_check_v2) do
    activity FraudCheck.v2(input)
  else
    activity FraudCheck.v1(input)
  end
end
```

Inside a workflow, `patched?/1` is journaled. The first live execution at that
source location writes a `patched` event with `value: true`; later replays use
the journaled value. Histories recorded before the patch line existed return
`false` without consuming the next old event, so they stay on the old branch.
The first decision for a patch name is also memoized per run: every use of the
same name returns that decision, including calls in loops or at sites that cross
from replayed history into the live tail.

Outside a workflow process, `patched?/1` returns `false`.

## When To Use It

Use `patched?/1` when the old and new branches can both remain in the same
workflow module until affected runs drain.

Prefer a new workflow version when the shape of the workflow state changes
substantially, when you remove or reorder existing effects, or when you cannot
keep both branches loaded.

Prefer a normal deploy with no patch marker only when there are no active runs
that can replay through the edited code.

## Operational Discipline

Patch markers are durable decisions. Once some runs have journaled `true` and
older runs are still taking the `false` branch, removing either branch is a
compatibility decision. Keep the marker and both branches until you know the old
histories have completed or been cancelled.

Content-addressed workflow versioning protects runs from being dispatched into
the wrong entrypoint, but it does not remove the need to keep code for old patch
branches while old histories reference them.

### Compatibility note

Older Continuum releases decided patches per call site. A run could therefore
record a later `true` marker after first taking a pre-patch `false` branch.
Those histories remain decodable, but after upgrading the first decision wins
for every later use of the same patch name. Audit active histories for repeated
patch names before deployment if code previously called one patch on both sides
of a suspension point.

## Telemetry

Continuum emits `[:continuum, :patched, :hit]` with the patch name and value.
