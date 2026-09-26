defmodule Continuum.ObserverReplayTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run}

  defmodule Flow do
    use Continuum.Workflow

    def run(input) do
      first = Continuum.side_effect(fn -> input end)
      Continuum.side_effect(fn -> first end)
    end
  end

  defmodule Forever do
    use Continuum.Workflow
    def run(_input), do: spin(0)
    defp spin(value), do: spin(value + 1)
  end

  test "reports stored agreement, code version, snapshots and redacted payloads without writes" do
    {:ok, id} = Continuum.Test.start_postgres(Flow, %{secret: "private"})
    assert {:ok, _} = Continuum.await(id, 1_000, journal: Postgres)
    before = Repo.get!(Run, id)
    events = Repo.all(from(e in Event, where: e.run_id == ^id, order_by: e.seq))
    assert {:ok, report} = Continuum.Observer.replay_report(id, redactor: fn _ -> :redacted end)
    assert report.outcome == :completed
    assert report.agrees_with_stored_result?
    assert report.detail == :redacted
    assert report.stored_result == :redacted
    assert report.entrypoint == Flow.__continuum_workflow__().entrypoint
    assert report.snapshot == nil
    assert report.event_count == 2
    assert Repo.get!(Run, id) == before
    assert Repo.all(from(e in Event, where: e.run_id == ^id, order_by: e.seq)) == events

    {:ok, snapshot} =
      Continuum.Snapshot.compact(id, before.version_hash, Postgres.load(Instance.default(), id))

    :ok = Postgres.take_snapshot!(Instance.default(), snapshot)
    assert {:ok, report} = Continuum.Observer.replay_report(id)
    assert report.snapshot.through_seq == 1
    assert report.event_count == 0
    assert report.agrees_with_stored_result?
  end

  test "refuses oversized histories, payloads and snapshots before replay" do
    {:ok, id} = Continuum.Test.start_postgres(Flow, "a moderately sized value")
    assert {:ok, _} = Continuum.await(id, 1_000, journal: Postgres)
    assert {:error, :history_event_limit} = Continuum.Observer.replay_report(id, max_events: 1)

    assert {:error, :run_payload_too_large} =
             Continuum.Observer.replay_report(id, max_payload_bytes: 1)

    assert {:error, :history_byte_limit} =
             Continuum.Observer.replay_report(id, max_history_bytes: 1)

    hash = Flow.__continuum_workflow__().version_hash
    {:ok, snapshot} = Continuum.Snapshot.compact(id, hash, Postgres.load(Instance.default(), id))
    :ok = Postgres.take_snapshot!(Instance.default(), snapshot)

    assert {:error, :snapshot_too_large} =
             Continuum.Observer.replay_report(id, max_history_bytes: 1)
  end

  test "an incompatible snapshot payload reloads the full bounded history" do
    {:ok, id} = Continuum.Test.start_postgres(Flow, :value)
    assert {:ok, _} = Continuum.await(id, 1_000, journal: Postgres)
    hash = Flow.__continuum_workflow__().version_hash
    {:ok, snapshot} = Continuum.Snapshot.compact(id, hash, Postgres.load(Instance.default(), id))
    :ok = Postgres.take_snapshot!(Instance.default(), snapshot)
    incompatible = Continuum.Snapshot.encode(%{snapshot | version_hash: "other-version"})

    Repo.update_all(from(s in Continuum.Schema.Snapshot, where: s.run_id == ^id),
      set: [payload: incompatible]
    )

    assert {:ok, report} = Continuum.Observer.replay_report(id)
    assert report.snapshot == nil
    assert report.event_count == 2
    assert report.agrees_with_stored_result?
    assert {:error, :history_event_limit} = Continuum.Observer.replay_report(id, max_events: 1)
  end

  test "drift names its cursor and commands, and unknown versions remain explicit" do
    {:ok, id} = Continuum.Test.start_postgres(Flow, :done)
    assert {:ok, _} = Continuum.await(id, 1_000, journal: Postgres)
    [event | _] = Repo.all(from(e in Event, where: e.run_id == ^id, order_by: e.seq))
    forged = Continuum.DurableTerm.decode!(event.payload) |> Map.put(:command_id, :changed)

    Repo.update_all(from(e in Event, where: e.run_id == ^id and e.seq == 0),
      set: [payload: :erlang.term_to_binary(forged)]
    )

    assert {:ok, report} = Continuum.Observer.replay_report(id)
    assert report.outcome == :drift
    assert report.detail.cursor == 0
    assert report.detail.expected
    assert report.detail.actual

    Repo.update_all(from(r in Run, where: r.id == ^id),
      set: [version_hash: "unavailable-version"]
    )

    assert {:error, {:unknown_version, %{version_hash: "unavailable-version"}}} =
             Continuum.Observer.replay_report(id)
  end

  test "execution time is bounded and redactor failures do not reveal payloads" do
    id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Instance.default(), id, Forever, :input)
    started = System.monotonic_time(:millisecond)
    assert {:error, :replay_timeout} = Continuum.Observer.replay_report(id, timeout_ms: 100)
    assert System.monotonic_time(:millisecond) - started < 1_000
    assert Repo.get!(Run, id).state == "running"

    {:ok, id} = Continuum.Test.start_postgres(Flow, "secret")
    assert {:ok, _} = Continuum.await(id, 1_000, journal: Postgres)

    assert {:error, :replay_failed} =
             Continuum.Observer.replay_report(id,
               redactor: fn _ -> raise "secret should not escape" end
             )
  end
end
