defmodule Continuum.Runtime.DispatcherTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Dispatcher
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease
  alias Continuum.Schema.Event
  alias Continuum.Schema.Run

  defmodule DispatchFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      value = Continuum.side_effect(fn -> input.seed * 3 end)
      {:ok, value}
    end
  end

  setup do
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "dispatch_once leases an unowned run and starts an engine" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(run_id, DispatchFlow, %{seed: 7})
    :ok = Postgres.suspend!(run_id, nil)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "dispatcher-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 21}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    run = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert run.lease_owner == "dispatcher-test"
    assert is_integer(run.lease_token)
  end

  test "dispatch_once skips rows scheduled for the future" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(run_id, DispatchFlow, %{seed: 7})
    :ok = Postgres.suspend!(run_id, nil)

    future =
      DateTime.utc_now()
      |> DateTime.add(60, :second)
      |> DateTime.truncate(:microsecond)

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [next_wakeup_at: future]
    )

    assert {:ok, 0} = Dispatcher.dispatch_once(owner: "dispatcher-test", batch_size: 1)
    assert {:error, :timeout} = Continuum.await(run_id, 25, journal: Postgres)
  end

  test "dispatch_once steals an expired lease and resumes the run" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(run_id, DispatchFlow, %{seed: 4})
    assert {:ok, %Lease{token: stale_token}} = Lease.acquire(run_id, owner: "old-owner")
    :ok = Postgres.suspend!(run_id, stale_token)

    expire_lease(run_id)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "dispatcher-test", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, 12}}} =
             Continuum.await(run_id, 1_000, journal: Postgres)

    run = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert run.lease_owner == "dispatcher-test"
    assert run.lease_token > stale_token
  end

  defp expire_lease(run_id) do
    expired_at =
      DateTime.utc_now()
      |> DateTime.add(-60, :second)
      |> DateTime.truncate(:microsecond)

    Repo.update_all(
      from(r in Run, where: r.id == ^run_id),
      set: [lease_expires_at: expired_at]
    )
  end
end
