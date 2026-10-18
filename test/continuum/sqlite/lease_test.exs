defmodule Continuum.SQLite.LeaseTest do
  @moduledoc """
  Lease fencing, claim exclusivity, and recovery sweeps on SQLite, where a
  single writer lock stands in for PostgreSQL row locks.
  """

  use Continuum.Test.SQLiteCase

  alias Continuum.Runtime.ActivityWorker.Dispatcher, as: ActivityDispatcher
  alias Continuum.Runtime.{Instance, Lease, Recovery}
  alias Continuum.Schema.{ActivityTask, Run, Timer}

  @opts [repo: SQLiteRepo]

  setup do
    name = :"sqlite_lease_#{System.unique_integer([:positive])}"
    %{instance: Instance.register(Instance.new(name: name, repo: SQLiteRepo))}
  end

  test "acquire is a CAS on unowned or expired leases with monotonic tokens" do
    run_id = insert_run!()

    assert {:ok, %Lease{owner: "a", token: first}} = Lease.acquire(run_id, [owner: "a"] ++ @opts)
    assert {:error, :not_acquired} = Lease.acquire(run_id, [owner: "b"] ++ @opts)

    expire!(run_id)

    assert {:ok, %Lease{owner: "b", token: second}} = Lease.acquire(run_id, [owner: "b"] ++ @opts)
    assert second > first

    assert {:error, :not_acquired} = Lease.acquire(Ecto.UUID.generate(), [owner: "a"] ++ @opts)
  end

  test "renewal and release are fenced by owner and token" do
    run_id = insert_run!()
    {:ok, lease} = Lease.acquire(run_id, [owner: "a", ttl_seconds: 1] ++ @opts)
    before = SQLiteRepo.get!(Run, run_id).lease_expires_at

    assert :ok = Lease.renew(run_id, "a", lease.token, [ttl_seconds: 60] ++ @opts)
    assert DateTime.diff(SQLiteRepo.get!(Run, run_id).lease_expires_at, before, :second) >= 50

    assert {:error, :lost} = Lease.renew(run_id, "a", lease.token + 1, @opts)
    assert {:error, :lost} = Lease.renew(run_id, "b", lease.token, @opts)

    SQLiteRepo.update_all(from(r in Run, where: r.id == ^run_id),
      set: [cancel_requested_at: DateTime.utc_now()]
    )

    assert {:ok, :cancel_requested} = Lease.renew(run_id, "a", lease.token, @opts)

    assert {:error, :lost} = Lease.release(run_id, "a", lease.token + 1, @opts)
    assert :ok = Lease.release(run_id, "a", lease.token, @opts)
    assert %Run{lease_owner: nil, lease_token: nil} = SQLiteRepo.get!(Run, run_id)
  end

  test "batch renewal reports each lease independently" do
    kept = insert_run!()
    cancelled = insert_run!()
    stolen = insert_run!()

    leases =
      Map.new([kept, cancelled, stolen], fn run_id ->
        {:ok, lease} = Lease.acquire(run_id, [owner: "a"] ++ @opts)
        {run_id, %{owner: lease.owner, token: lease.token}}
      end)

    SQLiteRepo.update_all(from(r in Run, where: r.id == ^cancelled),
      set: [cancel_requested_at: DateTime.utc_now()]
    )

    SQLiteRepo.update_all(from(r in Run, where: r.id == ^stolen), set: [lease_token: -1])

    assert {:ok, outcomes} = Lease.renew_batch(Map.to_list(leases), @opts)

    assert outcomes == %{
             kept => :ok,
             cancelled => {:ok, :cancel_requested},
             stolen => {:error, :lost}
           }
  end

  test "concurrent acquirers produce exactly one lease holder" do
    run_id = insert_run!()

    results =
      1..8
      |> Task.async_stream(&Lease.acquire(run_id, [owner: "owner-#{&1}"] ++ @opts),
        max_concurrency: 8,
        timeout: 15_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert [{:ok, %Lease{owner: owner, token: token}}] =
             Enum.filter(results, &match?({:ok, _}, &1))

    assert Enum.count(results, &(&1 == {:error, :not_acquired})) == 7
    assert %Run{lease_owner: ^owner, lease_token: ^token} = SQLiteRepo.get!(Run, run_id)
  end

  test "concurrent activity claims lease a task once", %{instance: instance} do
    run_id = insert_run!()
    {:ok, _lease} = Lease.acquire(run_id, [owner: "engine"] ++ @opts)
    task_id = insert_task!(run_id)

    results =
      1..6
      |> Task.async_stream(
        &ActivityDispatcher.claim_one(instance, task_id, 1, "worker-#{&1}"),
        max_concurrency: 6,
        timeout: 15_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert [{:ok, claimed}] = Enum.filter(results, &match?({:ok, _}, &1))
    assert claimed.id == task_id
    assert is_integer(claimed.run_lease_token)
    assert claimed.queue_age_ms >= 0
    assert Enum.all?(results -- [{:ok, claimed}], &(&1 == :not_available))
    assert %ActivityTask{state: "leased"} = SQLiteRepo.get!(ActivityTask, task_id)
  end

  test "recovery releases expired work and arms overdue timers", %{instance: instance} do
    orphaned = insert_run!(state: "running")
    {:ok, _lease} = Lease.acquire(orphaned, [owner: "dead-node"] ++ @opts)
    expire!(orphaned)

    timed = insert_run!(state: "suspended")

    SQLiteRepo.insert!(%Timer{
      id: Ecto.UUID.generate(),
      run_id: timed,
      fires_at: DateTime.add(DateTime.utc_now(), -5, :second),
      fired: false
    })

    task_id = insert_task!(orphaned)

    SQLiteRepo.update_all(from(t in ActivityTask, where: t.id == ^task_id),
      set: [
        state: "leased",
        lease_owner: "dead-worker",
        lease_expires_at: DateTime.add(DateTime.utc_now(), -5, :second)
      ]
    )

    assert {:ok, %{runs: 1, activity_tasks: 1, timers: 1}} =
             Recovery.recover_once(instance: instance)

    assert %Run{state: "suspended", lease_owner: nil, next_wakeup_at: %DateTime{}} =
             SQLiteRepo.get!(Run, orphaned)

    assert %Run{next_wakeup_at: %DateTime{}} = SQLiteRepo.get!(Run, timed)
    assert %ActivityTask{state: "available", attempt: 2} = SQLiteRepo.get!(ActivityTask, task_id)
  end

  defp insert_run!(fields \\ []) do
    id = Ecto.UUID.generate()

    SQLiteRepo.insert!(
      struct(
        %Run{
          id: id,
          workflow: "Elixir.Example",
          version_hash: <<1>>,
          state: "suspended",
          input: :erlang.term_to_binary(nil)
        },
        fields
      )
    )

    id
  end

  defp insert_task!(run_id) do
    id = Ecto.UUID.generate()

    SQLiteRepo.insert!(%ActivityTask{
      id: id,
      run_id: run_id,
      seq: 0,
      mfa: :erlang.term_to_binary(%{mfa: {__MODULE__, :work, []}}),
      state: "available"
    })

    id
  end

  defp expire!(run_id) do
    SQLiteRepo.update_all(from(r in Run, where: r.id == ^run_id),
      set: [lease_expires_at: DateTime.add(DateTime.utc_now(), -1, :second)]
    )
  end
end
