# Auditing

`mix continuum.audit` is a read-only operator task for workflow metadata and
patch-marker hygiene.

```bash
mix continuum.audit --repo MyApp.Repo
mix continuum.audit --repo MyApp.Repo --format json
mix continuum.audit --repo MyApp.Repo --strict
```

The task reports loaded workflow versions, their `Continuum.patched?/1` call
sites, and whether each patch is still needed by non-terminal runs. It also
counts `expired_leased_activity_tasks` — tasks still `leased` past their lease
expiry. A persistently non-zero count means workers are dying between claim
and completion faster than the steady-state sweep rescues them.

Patch verdicts:

* `still-in-use` means at least one non-terminal run of that workflow version has
  not journaled the patch decision yet.
* `safe-to-remove` means no non-terminal run is still before that patch site.

`--strict` exits non-zero when a patch is safe to remove or when any run is stuck
with `state = 'stuck_unknown_version'`, making the task suitable for CI hygiene.
