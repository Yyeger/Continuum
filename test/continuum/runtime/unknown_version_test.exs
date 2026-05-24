defmodule Continuum.Runtime.UnknownVersionTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Dispatcher
  alias Continuum.Schema.{Event, Run, WorkflowVersion}

  defmodule MissingLogicalFlow do
  end

  setup do
    Repo.delete_all(WorkflowVersion)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "marks an unresolved workflow version as stuck exactly once" do
    run_id = Ecto.UUID.generate()
    missing_hash = "missing-version-hash"

    %Run{}
    |> Ecto.Changeset.change(%{
      id: run_id,
      workflow: inspect(MissingLogicalFlow),
      version_hash: missing_hash,
      state: "suspended",
      input: :erlang.term_to_binary(%{}),
      next_wakeup_at:
        DateTime.utc_now()
        |> DateTime.add(-1, :second)
        |> DateTime.truncate(:microsecond)
    })
    |> Repo.insert!()

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

    assert_eventually(fn ->
      Repo.get!(Run, run_id).state == "stuck_unknown_version"
    end)

    assert_receive {:telemetry, [:continuum, :run, :unknown_version], %{},
                    %{run_id: ^run_id, version_hash: ^missing_hash}},
                   1_000

    assert {:ok, 0} = Dispatcher.dispatch_once(owner: "unknown-version", batch_size: 1)
    refute_receive {:telemetry, [:continuum, :run, :unknown_version], %{}, _metadata}, 100
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
