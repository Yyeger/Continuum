defmodule Continuum.VersionRegistryPersistenceTest do
  @moduledoc """
  Registering a version that is already registered must not rewrite the
  `persistent_term`.

  Replacing one schedules a literal-area collection that scans every process
  which may reference the old term. `ensure_registered/1,2` sits on the
  run-start path twice, so paying that per start is a throughput ceiling that
  scales with live process count — for a value that is content-addressed and
  therefore never changes.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Instance
  alias Continuum.VersionRegistry

  defmodule StableFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: {:ok, :stable}
  end

  defmodule ThresholdFlow do
    use Continuum.Workflow, version: 1, snapshot_threshold: 5

    def run(_input), do: {:ok, :threshold}
  end

  test "re-registering a loaded version leaves the registry term untouched" do
    {:ok, _entry} = VersionRegistry.ensure_registered(StableFlow)
    before = registry_term()

    for _ <- 1..50, do: {:ok, _} = VersionRegistry.ensure_registered(StableFlow)

    # Same value *and* the same term identity: a rewritten persistent_term
    # produces a structurally equal but distinct term.
    assert registry_term() == before
    assert :erts_debug.same(registry_term(), before)
  end

  test "re-flagging snapshot thresholds leaves the hint term untouched" do
    instance = Instance.default()
    {:ok, _entry} = VersionRegistry.ensure_registered(ThresholdFlow, instance)
    before = hint_term()

    for _ <- 1..50, do: {:ok, _} = VersionRegistry.ensure_registered(ThresholdFlow, instance)

    assert :erts_debug.same(hint_term(), before)
  end

  defp registry_term, do: :persistent_term.get({Continuum.VersionRegistry, :entries}, %{})

  defp hint_term,
    do: :persistent_term.get({Continuum.VersionRegistry, :snapshot_thresholds}, %{})
end
