defmodule Continuum.Test.ClusterCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      import Continuum.Test.ClusterCase

      alias Continuum.Runtime.{ActivityWorker, Dispatcher, Engine, Journal, Recovery}
      alias Continuum.Test.{ClusterFlows, Repo}
      alias Continuum.Schema.{ActivityTask, Event, Run}

      import Ecto.Query

      @moduletag :cluster
    end
  end

  setup_all do
    unless System.get_env("CONTINUUM_CLUSTER_TEST") == "1" do
      raise "cluster tests require CONTINUUM_CLUSTER_TEST=1; run `mix test.cluster`"
    end

    ensure_distributed!()
    :ok
  end

  setup do
    truncate_continuum_tables()
    on_exit(fn -> truncate_continuum_tables() end)
    :ok
  end

  def start_peer!(name) do
    name = :"#{name}_#{System.unique_integer([:positive])}"

    case :peer.start_link(%{
           name: name,
           args: peer_args(),
           env: [{~c"MIX_ENV", ~c"test"}, {~c"CONTINUUM_CLUSTER_TEST", ~c"1"}],
           wait_boot: 15_000
         }) do
      {:ok, pid, node} ->
        bootstrap_peer!(node)
        %{pid: pid, node: node}

      {:ok, pid} ->
        node = :peer.get_state(pid).node
        bootstrap_peer!(node)
        %{pid: pid, node: node}

      {:error, reason} ->
        raise "failed to start peer #{inspect(name)}: #{inspect(reason)}"
    end
  end

  def stop_peer(%{pid: pid}) do
    :peer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  def peer_call(%{node: node}, module, function, args \\ [], timeout \\ 15_000) do
    :erpc.call(node, module, function, args, timeout)
  end

  def wait_until(fun, attempts \\ 60)

  def wait_until(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(50)
      wait_until(fun, attempts - 1)
    end
  end

  def wait_until(_fun, 0), do: flunk("condition did not become true")

  def truncate_continuum_tables do
    Continuum.Test.Repo.query!("""
    TRUNCATE continuum_activity_results,
             continuum_activity_tasks,
             continuum_events,
             continuum_runs,
             continuum_signals,
             continuum_timers,
             continuum_snapshots,
             continuum_workflow_versions
    RESTART IDENTITY CASCADE
    """)
  end

  defp ensure_distributed! do
    if Node.alive?() do
      :ok
    else
      start_distribution!()
    end
  end

  defp start_distribution! do
    case do_start_distribution() do
      :ok ->
        :ok

      {:error, reason} ->
        System.cmd("epmd", ["-daemon"], stderr_to_stdout: true)

        case do_start_distribution() do
          :ok -> :ok
          {:error, _retry_reason} -> raise "failed to start distributed node: #{inspect(reason)}"
        end
    end
  end

  defp do_start_distribution do
    name = :"continuum_origin_#{System.unique_integer([:positive])}"

    case Node.start(name, :shortnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp peer_args do
    :code.get_path()
    |> Enum.flat_map(fn path -> [~c"-pa", path] end)
  end

  defp bootstrap_peer!(node) do
    config = Application.fetch_env!(:continuum, Continuum.Test.Repo)

    result =
      :erpc.call(
        node,
        Continuum.Test.ClusterNode,
        :bootstrap,
        [config],
        15_000
      )

    unless result == :ok do
      raise "peer #{inspect(node)} bootstrap failed: #{inspect(result)}"
    end
  end
end

defmodule Continuum.Test.ClusterNode do
  @moduledoc false

  def bootstrap(repo_config) do
    Application.put_env(:continuum, Continuum.Test.Repo, repo_config)

    Application.put_env(:continuum, :repo, Continuum.Test.Repo)
    Application.put_env(:continuum, :ecto_repos, [Continuum.Test.Repo])
    Application.put_env(:continuum, :dispatcher, false)
    Application.put_env(:continuum, :activity_worker, false)
    Application.put_env(:continuum, :timer_wheel, false)
    Application.put_env(:continuum, :signal_router, listen?: false)
    Application.put_env(:continuum, :recovery, false)

    {:ok, _} = Application.ensure_all_started(:telemetry)
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ecto_sql)

    {:ok, _} = Application.ensure_all_started(:continuum)
    start_repo_child()

    Continuum.VersionRegistry.ensure_registered(Continuum.Test.ClusterFlows.SideEffectFlow)
    Continuum.VersionRegistry.ensure_registered(Continuum.Test.ClusterFlows.SignalFlow)
    Continuum.VersionRegistry.ensure_registered(Continuum.Test.ClusterFlows.ActivityFlow)

    :ok
  end

  defp start_repo_child do
    if Process.whereis(Continuum.Test.Repo) do
      :ok
    else
      case Supervisor.start_child(Continuum.Supervisor, Continuum.Test.Repo) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end
  end
end
