defmodule Continuum.Test.ClusterScenarios do
  @moduledoc false

  alias Continuum.Runtime.{Engine, Journal}
  alias Continuum.Test.ClusterFlows

  def start_signal_run(test_pid, opts) do
    {:ok, run_id} =
      Engine.start_run(
        ClusterFlows.SignalFlow,
        %{},
        Keyword.merge([journal: Journal.Postgres], opts)
      )

    wait_until(fn ->
      case Registry.lookup(Continuum.Runtime.Registry, run_id) do
        [{pid, _}] ->
          send(test_pid, {:signal_run_started, run_id, node(), pid})
          true

        [] ->
          false
      end
    end)

    run_id
  end

  def start_activity_run(input, opts \\ []) do
    {:ok, run_id} =
      Engine.start_run(
        ClusterFlows.ActivityFlow,
        input,
        Keyword.merge([journal: Journal.Postgres], opts)
      )

    run_id
  end

  def attach_lease_lost(test_pid) do
    handler_id = "cluster-lease-lost-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:continuum, :lease, :lost],
      fn _event, _measurements, metadata, _config ->
        send(test_pid, {:cluster_lease_lost, metadata.run_id, metadata})
      end,
      nil
    )

    :ok
  end

  defp wait_until(fun, attempts \\ 60)

  defp wait_until(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(50)
      wait_until(fun, attempts - 1)
    end
  end

  defp wait_until(_fun, 0), do: raise("condition did not become true")
end
