defmodule Continuum.Runtime.LeaseBatchTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{Instance, Lease}
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Runtime.Lease.Heartbeater
  alias Continuum.Schema.Run

  defmodule WaitingFlow do
    use Continuum.Workflow
    def run(_input), do: await(signal(:finish))
  end

  defmodule RecordingRepo do
    def query(_sql, [ids, _owners, _tokens, _ttl]) do
      Process.put(:renew_batch_sizes, [length(ids) | Process.get(:renew_batch_sizes, [])])

      if Process.get(:renew_batch_error) do
        {:error, :connection_unavailable}
      else
        {:ok, %{rows: Enum.map(ids, &[Ecto.UUID.load!(&1), nil])}}
      end
    end
  end

  test "batch renewals preserve owner/token fencing, terminal exclusion and cancellation" do
    entries =
      for index <- 1..5 do
        id = Ecto.UUID.generate()
        :ok = Postgres.start_run(Instance.default(), id, WaitingFlow, %{})
        {:ok, lease} = Lease.acquire(id, repo: Repo, owner: "owner-#{index}")
        {id, %{owner: lease.owner, token: lease.token}}
      end

    [{valid, _}, {cancel, _}, {wrong_owner, _}, {wrong_token, _}, {terminal, _}] = entries
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.update_all(from(r in Run, where: r.id == ^cancel), set: [cancel_requested_at: now])
    Repo.update_all(from(r in Run, where: r.id == ^wrong_owner), set: [lease_owner: "new-owner"])
    Repo.update_all(from(r in Run, where: r.id == ^wrong_token), inc: [lease_token: 1])
    Repo.update_all(from(r in Run, where: r.id == ^terminal), set: [state: "completed"])

    assert {:ok, outcomes} = Lease.renew_batch(entries, repo: Repo, ttl_seconds: 45)
    assert outcomes[valid] == :ok
    assert outcomes[cancel] == {:ok, :cancel_requested}

    for id <- [wrong_owner, wrong_token, terminal] do
      assert outcomes[id] == {:error, :lost}
    end
  end

  test "the heartbeater renews large populations in bounded queries and skips memory runs" do
    state = state_with_leases(2_501)
    {id, entry} = Enum.at(state.leases, 0)
    state = put_in(state.leases[id], %{entry | durable?: false})

    assert {:reply, :ok, ^state} = Heartbeater.handle_call(:renew_once, nil, state)
    assert Enum.sort(Process.get(:renew_batch_sizes)) == [500, 1_000, 1_000]
  end

  test "query failure keeps leases tracked and does not send ownership-loss messages" do
    state = state_with_leases(3)
    Process.put(:renew_batch_error, true)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:reply, :ok, ^state} = Heartbeater.handle_call(:renew_once, nil, state)
    end)

    refute_received {:continuum_lease_lost, _, _}
    refute_received {:continuum_cancel_requested, _}
  end

  defp state_with_leases(count) do
    leases =
      Map.new(1..count, fn index ->
        {Ecto.UUID.generate(),
         %{owner: "owner", token: index, durable?: true, pid: self(), ref: make_ref()}}
      end)

    %{
      leases: leases,
      refs: Map.new(leases, fn {id, entry} -> {entry.ref, id} end),
      instance: Continuum,
      ttl_seconds: 30,
      repo: RecordingRepo
    }
  end
end
