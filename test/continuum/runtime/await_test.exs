defmodule Continuum.Runtime.AwaitTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.InMemory
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease
  alias Continuum.Schema.{ActivityTask, Event, Run, Signal, Timer}

  setup do
    InMemory.reset()
    Repo.delete_all(Signal)
    Repo.delete_all(Timer)
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "await/3 wakes from an in-memory run_finished broadcast" do
    run_id = Ecto.UUID.generate()
    :ok = InMemory.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

    waiter = Task.async(fn -> Continuum.await(run_id, 1_000, journal: InMemory) end)
    assert Task.yield(waiter, 25) == nil

    :ok = InMemory.complete!(Continuum.Runtime.Instance.default(), run_id, {:ok, 123}, nil)

    assert {:ok, {:ok, %{run_id: ^run_id, state: :completed, result: {:ok, 123}}}} =
             Task.yield(waiter, 250)
  end

  test "await/3 wakes from Postgres cancel_run!/2 broadcast" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
    assert {:ok, %Lease{token: token}} = Lease.acquire(run_id, owner: "await-test")

    waiter = Task.async(fn -> Continuum.await(run_id, 1_000, journal: Postgres) end)
    assert Task.yield(waiter, 25) == nil

    :ok = Phoenix.PubSub.subscribe(Continuum.PubSub, "continuum:run:#{run_id}")
    :ok = Postgres.cancel_run!(Continuum.Runtime.Instance.default(), run_id, token)
    assert_receive {:run_finished, ^run_id, :failed, :cancelled}, 250

    assert {:ok, {:error, %{run_id: ^run_id, state: :failed, error: :cancelled}}} =
             Task.yield(waiter, 1_000)
  end
end
