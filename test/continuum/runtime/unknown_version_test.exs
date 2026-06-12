defmodule Continuum.Runtime.UnknownVersionTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Dispatcher
  alias Continuum.Runtime.Instance
  alias Continuum.Schema.{Event, Run, WorkflowVersion}

  defmodule MissingLogicalFlow do
  end

  defmodule RecoverableFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: {:ok, :recovered}
  end

  setup do
    Repo.delete_all(WorkflowVersion)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "an unresolvable version releases the lease and leaves the run suspended" do
    run_id = insert_run(inspect(MissingLogicalFlow), "missing-version-hash", "suspended")

    handler_id = "unknown-version-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:continuum, :run, :unknown_version],
        fn event, measurements, metadata, test_pid ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "unknown-version", batch_size: 1)

    assert_receive {:telemetry, [:continuum, :run, :unknown_version], %{},
                    %{run_id: ^run_id, version_hash: "missing-version-hash"}},
                   1_000

    # The run is not marked stuck globally: it stays suspended with a cleared
    # lease so a node that has the version loaded can claim it.
    assert_eventually(fn ->
      run = Repo.get!(Run, run_id)
      run.state == "suspended" and is_nil(run.lease_owner) and is_nil(run.lease_token)
    end)

    # The release backs the run off the runnable set, so this (incapable)
    # node does not hot-loop on it at the poll rate.
    run = Repo.get!(Run, run_id)
    assert DateTime.compare(run.next_wakeup_at, DateTime.utc_now()) == :gt
    assert {:ok, 0} = Dispatcher.dispatch_once(owner: "unknown-version-retry", batch_size: 1)

    # Once the backoff lapses it is claimable again — a capable node would
    # pick it up and resume it.
    past = DateTime.utc_now() |> DateTime.add(-1, :second) |> DateTime.truncate(:microsecond)
    Repo.update_all(from(r in Run, where: r.id == ^run_id), set: [next_wakeup_at: past])

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "unknown-version-retry", batch_size: 1)
  end

  test "registering a version recovers legacy stuck_unknown_version runs" do
    metadata = RecoverableFlow.__continuum_workflow__()

    run_id =
      insert_run(inspect(RecoverableFlow), metadata.version_hash, "stuck_unknown_version")

    :ok = Continuum.VersionRegistry.upsert_instance(Instance.default(), [RecoverableFlow])

    run = Repo.get!(Run, run_id)
    assert run.state == "suspended"
    assert is_nil(run.lease_owner)
    assert is_nil(run.error)

    assert {:ok, 1} = Dispatcher.dispatch_once(owner: "unknown-version-recovered", batch_size: 1)

    assert {:ok, %{state: :completed, result: {:ok, :recovered}}} =
             Continuum.await(run_id, 1_000, journal: Continuum.Runtime.Journal.Postgres)
  end

  defp insert_run(workflow, version_hash, state) do
    run_id = Ecto.UUID.generate()

    %Run{}
    |> Ecto.Changeset.change(%{
      id: run_id,
      workflow: workflow,
      version_hash: version_hash,
      state: state,
      input: :erlang.term_to_binary(%{}),
      next_wakeup_at:
        DateTime.utc_now()
        |> DateTime.add(-1, :second)
        |> DateTime.truncate(:microsecond)
    })
    |> Repo.insert!()

    run_id
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
