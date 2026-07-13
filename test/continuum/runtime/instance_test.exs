defmodule Continuum.Runtime.InstanceTest do
  use ExUnit.Case, async: false

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.InMemory

  defmodule EchoFlow do
    use Continuum.Workflow

    def run(input), do: input
  end

  setup do
    InMemory.reset()
    :ok
  end

  test "children/1 returns no specs for an in-memory default instance" do
    original_repo = Application.get_env(:continuum, :repo)
    original_instance = Instance.default()
    Application.delete_env(:continuum, :repo)

    on_exit(fn ->
      Application.put_env(:continuum, :repo, original_repo)
      Instance.register(original_instance)
    end)

    assert Continuum.children() == []
  end

  test "default Postgres children are host-owned and exclude application-owned processes" do
    original_instance = Instance.default()
    on_exit(fn -> Instance.register(original_instance) end)

    children = Continuum.children(name: Continuum, repo: Continuum.Test.Repo)

    assert Enum.any?(children, &match?(%{id: {Continuum.Runtime.TimerWheel, Continuum}}, &1))
    assert Enum.any?(children, &match?(%{id: {Continuum.VersionRegistry, Continuum}}, &1))

    refute Enum.any?(children, &match?(%{id: {Phoenix.PubSub, Continuum}}, &1))
    refute Enum.any?(children, &match?(%{id: {Registry, Continuum}}, &1))

    refute Enum.any?(
             children,
             &match?(%{id: {Continuum.Runtime.RunSupervisor, Continuum}}, &1)
           )
  end

  test "application-owned default children never include Repo-dependent workers" do
    instance = Instance.new(name: Continuum, repo: Continuum.Test.Repo)
    children = Continuum.Application.child_specs(instance)

    assert Continuum.Runtime.Journal.InMemory in children

    repo_dependent = [
      Continuum.VersionRegistry,
      Continuum.Runtime.Lease.Heartbeater,
      Continuum.Runtime.Recovery,
      Continuum.Runtime.Snapshotter,
      Continuum.Runtime.Dispatcher,
      Continuum.Runtime.ActivityWorker.Dispatcher,
      Continuum.Runtime.ActivityWorker.Supervisor,
      Continuum.Runtime.TimerWheel,
      Continuum.Runtime.SignalRouter
    ]

    refute Enum.any?(children, fn
             %{id: {module, Continuum}} -> module in repo_dependent
             module when is_atom(module) -> module in repo_dependent
             _child -> false
           end)
  end

  test "children/1 registers named instances with isolated process names" do
    name = unique_instance_name()

    children =
      Continuum.children(
        name: name,
        repo: Continuum.Test.Repo,
        recovery: false,
        dispatcher: false,
        activity_dispatcher: false,
        timer_wheel: false,
        signal_router: false
      )

    instance = Instance.lookup(name)

    assert instance.name == name
    assert instance.repo == Continuum.Test.Repo
    assert instance.activity_executor == :builtin
    assert instance.registry != Continuum.Runtime.Registry
    assert instance.run_supervisor != Continuum.Runtime.RunSupervisor

    assert Enum.any?(children, &match?(%{id: {Phoenix.PubSub, ^name}}, &1))
    assert Enum.any?(children, &match?(%{id: {Registry, ^name}}, &1))
    assert Enum.any?(children, &match?(%{id: {Continuum.Runtime.RunSupervisor, ^name}}, &1))
  end

  test "lookup/1 rejects unregistered named instances" do
    assert_raise Continuum.InstanceNotRegisteredError, ~r/not registered/, fn ->
      Instance.lookup(unique_instance_name())
    end
  end

  test "instance rejects invalid activity executors" do
    assert_raise ArgumentError, ~r/invalid Continuum activity executor/, fn ->
      Instance.new(name: unique_instance_name(), activity_executor: :unknown)
    end
  end

  test "same run id can exist independently in different in-memory instances" do
    run_id = "same-run-id"
    left = Instance.new(name: unique_instance_name()) |> Instance.register()
    right = Instance.new(name: unique_instance_name()) |> Instance.register()

    assert :ok = InMemory.start_run(left, run_id, EchoFlow, %{side: :left})
    assert :ok = InMemory.start_run(right, run_id, EchoFlow, %{side: :right})

    assert :ok = InMemory.append!(left, run_id, %{type: :side_effect, payload: :left}, nil)
    assert :ok = InMemory.append!(right, run_id, %{type: :side_effect, payload: :right}, nil)

    assert [%{payload: :left}] = InMemory.load(left, run_id)
    assert [%{payload: :right}] = InMemory.load(right, run_id)
  end

  test "public start and await can target a named in-memory instance" do
    name = unique_instance_name()

    {:ok, _supervisor} =
      Continuum.children(
        name: name,
        recovery: false,
        dispatcher: false,
        activity_dispatcher: false,
        timer_wheel: false,
        signal_router: false
      )
      |> Supervisor.start_link(strategy: :one_for_one)

    assert {:ok, run_id} =
             Continuum.start(EchoFlow, %{ok: true}, journal: InMemory, instance: name)

    assert {:ok, %{state: :completed, result: %{ok: true}}} =
             Continuum.await(run_id, 1_000, journal: InMemory, instance: name)
  end

  defp unique_instance_name do
    :"continuum_test_#{System.unique_integer([:positive])}"
  end
end
