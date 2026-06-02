defmodule Continuum.Runtime.EngineTest do
  use ExUnit.Case, async: false

  alias Continuum.Runtime.Engine

  defmodule WaitingFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      await(signal(:continue))
    end
  end

  test "engine joins the continuum pg scope while running" do
    run_id = Ecto.UUID.generate()

    try do
      assert {:ok, ^run_id} = Engine.start_run(WaitingFlow, %{}, run_id: run_id)

      assert_eventually(fn ->
        Engine.wake(run_id) == :ok and
          self_in_pg_members?(Continuum.Runtime.Instance.default(), run_id)
      end)
    after
      stop_run(run_id)
      Continuum.Runtime.Journal.InMemory.reset()
    end
  end

  test "wake falls back to a pg member when the run is not local" do
    instance = Continuum.Runtime.Instance.default()
    run_id = Ecto.UUID.generate()
    test_pid = self()

    member =
      spawn(fn ->
        receive do
          {:"$gen_cast", :wake} -> send(test_pid, :forwarded_wake)
        end
      end)

    :ok = :pg.join(:continuum, {instance.name, run_id}, member)

    try do
      assert :ok = Engine.wake(instance, run_id)
      assert_receive :forwarded_wake, 1_000
    after
      :pg.leave(:continuum, {instance.name, run_id}, member)
      Process.exit(member, :kill)
    end
  end

  defp self_in_pg_members?(instance, run_id) do
    case Registry.lookup(instance.registry, run_id) do
      [{pid, _}] ->
        pid in :pg.get_members(:continuum, {instance.name, run_id})

      [] ->
        false
    end
  end

  defp stop_run(run_id) do
    Continuum.Runtime.Instance.default()
    |> Map.fetch!(:registry)
    |> Registry.lookup(run_id)
    |> Enum.each(fn {pid, _} -> Process.exit(pid, :kill) end)
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
