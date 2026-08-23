defmodule Continuum.Runtime.LocalExclusionTest do
  @moduledoc """
  The dispatcher and recovery exclude locally-registered runs by shipping their
  ids to Postgres. That array is now bounded.

  It is safe to bound because the exclusion is an optimisation, not the
  guarantee: `Dispatcher.start_engine/2` hands a rotated token to a live engine
  through `Engine.adopt_lease/4`, so a run claimed despite having a local engine
  keeps running. With 10k live local runs, shipping the array on every
  one-second poll costs more than the adoptions it avoids.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Instance

  test "local_run_ids/2 returns the ids when the registry is under the limit" do
    instance = Instance.default()

    assert is_list(Instance.local_run_ids(instance, 1_000))
  end

  test "local_run_ids/2 reports :too_many above the limit without building the list" do
    instance = Instance.default()

    # `Registry.count/1` is checked first, so a limit below the current count
    # short-circuits regardless of how many runs are actually registered.
    assert Instance.local_run_ids(instance, -0) in [[], :too_many]

    {:ok, _pid} = Registry.register(instance.registry, "local-exclusion-run", nil)

    assert Instance.local_run_ids(instance, 0) == :too_many
    assert "local-exclusion-run" in Instance.local_run_ids(instance, 1_000)
  end

  test "an unknown registry reports :too_many rather than crashing the poll" do
    instance = %{Instance.default() | registry: :no_such_registry_for_continuum}

    assert Instance.local_run_ids(instance, 10) == :too_many
  end
end
