defmodule Continuum.Runtime.LeaseTest do
  use Continuum.Test.DataCase, async: true

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease
  alias Continuum.Schema.Run

  describe "acquire/2" do
    test "claims an unleased run and returns a fencing token" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

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
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{}} = Lease.acquire(run_id, owner: "node-a")

      assert {:error, :not_acquired} = Lease.acquire(run_id, owner: "node-b")
    end

    test "claims a run whose lease expired and increments the fencing token" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})
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
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{owner: owner, token: token}} = Lease.acquire(run_id, owner: "node-a")

      assert :ok = Lease.renew(run_id, owner, token)
      assert {:error, :lost} = Lease.renew(run_id, "node-b", token)
      assert {:error, :lost} = Lease.renew(run_id, owner, token + 1)
    end

    test "reports lost after another owner steals an expired lease" do
      run_id = Ecto.UUID.generate()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})
      assert {:ok, %Lease{owner: owner, token: token}} = Lease.acquire(run_id, owner: "node-a")

      expire_lease(run_id)
      assert {:ok, %Lease{owner: "node-b"}} = Lease.acquire(run_id, owner: "node-b")

      assert {:error, :lost} = Lease.renew(run_id, owner, token)
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
end

defmodule Continuum.Runtime.LeaseHeartbeaterTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease
  alias Continuum.Runtime.Lease.Heartbeater
  alias Continuum.Schema.Run
  import ExUnit.CaptureLog

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

    log =
      capture_log(fn ->
        assert :ok = Heartbeater.renew_once()
        assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
      end)

    assert log =~ "lost its Postgres lease"
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
