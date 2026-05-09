defmodule Continuum.Runtime.LeaseTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease
  alias Continuum.Schema.{ActivityTask, Event, Run, Signal, Timer}

  defmodule FencedActivity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(value), do: {:ok, value}
  end

  defmodule FencedActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(FencedActivity.run(input.value))
    end
  end

  setup do
    Repo.delete_all(Signal)
    Repo.delete_all(Timer)
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  describe "acquire/2" do
    test "claims an unleased run and returns a fencing token" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      assert {:ok, %Lease{owner: "node-a", token: token}} =
               Lease.acquire(run_id, owner: "node-a")

      run = Repo.one!(from(r in Run, where: r.id == ^run_id))

      assert is_integer(token)
      assert run.lease_owner == "node-a"
      assert run.lease_token == token
      assert run.lease_expires_at != nil
    end

    test "does not claim a run with an active lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{}} = Lease.acquire(run_id, owner: "node-a")

      assert {:error, :not_acquired} = Lease.acquire(run_id, owner: "node-b")
    end

    test "claims a run whose lease expired and increments the fencing token" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{token: token_a}} = Lease.acquire(run_id, owner: "node-a")

      expire_lease(run_id)

      assert {:ok, %Lease{owner: "node-b", token: token_b}} =
               Lease.acquire(run_id, owner: "node-b")

      assert token_b > token_a
    end
  end

  describe "renew/4" do
    test "renews only when owner and token still match" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{owner: owner, token: token}} = Lease.acquire(run_id, owner: "node-a")

      assert :ok = Lease.renew(run_id, owner, token)
      assert {:error, :lost} = Lease.renew(run_id, "node-b", token)
      assert {:error, :lost} = Lease.renew(run_id, owner, token + 1)
    end

    test "reports lost after another owner steals an expired lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{owner: owner, token: token}} = Lease.acquire(run_id, owner: "node-a")

      expire_lease(run_id)
      assert {:ok, %Lease{owner: "node-b"}} = Lease.acquire(run_id, owner: "node-b")

      assert {:error, :lost} = Lease.renew(run_id, owner, token)
    end
  end

  describe "journal fencing" do
    test "rejects stale journal writes after another owner steals the lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{token: stale_token}} = Lease.acquire(run_id, owner: "node-a")

      expire_lease(run_id)

      assert {:ok, %Lease{token: current_token}} = Lease.acquire(run_id, owner: "node-b")
      assert current_token > stale_token

      assert_raise RuntimeError, ~r/lease_mismatch/, fn ->
        Postgres.append!(
          Continuum.Runtime.Instance.default(),
          run_id,
          %{type: :side_effect, kind: :user, payload: :stale_write, seq: nil},
          stale_token
        )
      end

      assert Postgres.load(Continuum.Runtime.Instance.default(), run_id) == []
    end

    test "rejects stale cancel_run! after another owner steals the lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{token: stale_token}} = Lease.acquire(run_id, owner: "node-a")

      expire_lease(run_id)

      assert {:ok, %Lease{token: current_token}} = Lease.acquire(run_id, owner: "node-b")
      assert current_token > stale_token

      assert_raise RuntimeError, ~r/lease_mismatch/, fn ->
        Postgres.cancel_run!(Continuum.Runtime.Instance.default(), run_id, stale_token)
      end

      run = Repo.one!(from(r in Run, where: r.id == ^run_id))
      assert run.state == "running"
      assert run.lease_token == current_token
    end

    test "rejects stale complete_activity_task! after another owner steals the run lease" do
      {:ok, run_id} =
        Continuum.Runtime.Engine.start_run(FencedActivityFlow, %{value: 10}, journal: Postgres)

      assert_eventually(fn ->
        Repo.aggregate(ActivityTask, :count) == 1
      end)

      task = Repo.one!(ActivityTask)
      run = Repo.one!(from(r in Run, where: r.id == ^run_id))

      Repo.update_all(
        from(t in ActivityTask, where: t.id == ^task.id),
        set: [state: "leased", lease_owner: "worker-a", lease_expires_at: future_time()]
      )

      claimed_task =
        task.mfa
        |> decode_term()
        |> Map.merge(%{
          id: task.id,
          run_id: task.run_id,
          seq: task.seq,
          attempt: task.attempt,
          lease_owner: "worker-a"
        })

      expire_lease(run_id)

      assert {:ok, %Lease{token: current_token}} = Lease.acquire(run_id, owner: "node-b")
      assert current_token > run.lease_token

      assert_raise RuntimeError, ~r/lease_mismatch/, fn ->
        Postgres.complete_activity_task!(
          Continuum.Runtime.Instance.default(),
          claimed_task,
          {:ok, 10},
          run.lease_token
        )
      end

      assert ["activity_scheduled"] = event_types(run_id)
      assert Repo.one!(ActivityTask).state == "leased"
    end
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

  defp future_time do
    DateTime.utc_now()
    |> DateTime.add(60, :second)
    |> DateTime.truncate(:microsecond)
  end

  defp decode_term(binary) when is_binary(binary), do: :erlang.binary_to_term(binary)

  defp event_types(run_id) do
    Repo.all(
      from(e in Event,
        where: e.run_id == ^run_id,
        order_by: [asc: e.seq],
        select: e.event_type
      )
    )
  end

  defp assert_eventually(fun, attempts \\ 20)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end

defmodule Continuum.Runtime.LeaseHeartbeaterTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease
  alias Continuum.Runtime.Lease.Heartbeater
  alias Continuum.Schema.Run

  defmodule SuspendedPgFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      await(signal(:continue))
    end
  end

  test "engine stops itself when the heartbeater detects a stolen lease" do
    {:ok, run_id} =
      Continuum.Runtime.Engine.start_run(SuspendedPgFlow, %{}, journal: Postgres)

    [{pid, _}] = Registry.lookup(Continuum.Runtime.Registry, run_id)
    ref = Process.monitor(pid)

    original = Repo.one!(from(r in Run, where: r.id == ^run_id))
    assert is_integer(original.lease_token)

    expire_lease(run_id)

    assert {:ok, %Lease{owner: "node-b", token: stolen_token}} =
             Lease.acquire(run_id, owner: "node-b")

    assert stolen_token > original.lease_token

    assert :ok = Heartbeater.renew_once(Continuum.Runtime.Instance.default())
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    assert_eventually(fn -> Registry.lookup(Continuum.Runtime.Registry, run_id) == [] end)
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

  defp assert_eventually(fun, attempts \\ 20)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
