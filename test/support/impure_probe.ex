defmodule Continuum.Test.ImpureProbe do
  @moduledoc false

  @registry __MODULE__.Registry

  # Audited impure helpers for test workflows.
  #
  # AstCheck forbids `self()`, `send/2`, `receive`, and `node()` inside
  # workflow code (including `side_effect` producer functions). Crash-resume
  # and cluster tests legitimately need them as scaffolding — to block a
  # producer mid-execution so the engine can be killed, or to journal which
  # node executed. Those uses are funneled through this module, which is
  # listed in `config :continuum, :trusted_modules` for the test env.

  def start_link do
    Agent.start_link(fn -> %{} end, name: @registry)
  end

  @doc "Register a test process and return a durable identifier for it."
  def register(test_pid \\ self()) do
    probe = Ecto.UUID.generate()
    Agent.update(@registry, &Map.put(&1, probe, test_pid))
    probe
  end

  @doc "The node currently executing, for journaling via side_effect."
  def current_node, do: node()

  @doc """
  Announce `{tag, self()}` to `probe`, then block until `:continue`
  arrives; returns `result`. Lets a test catch a workflow mid-side_effect.
  """
  def notify_and_block(probe, tag, result) do
    send(resolve(probe), {tag, self()})

    receive do
      :continue -> result
    end
  end

  @doc "Send `message` to the process registered for `probe`."
  def notify(probe, message) do
    send(resolve(probe), message)
    :ok
  end

  @doc "Send `{tag, self()}` to the process registered for `probe`."
  def notify_with_self(probe, tag), do: notify(probe, {tag, self()})

  defp resolve(pid) when is_pid(pid), do: pid
  defp resolve(probe), do: Agent.get(@registry, &Map.fetch!(&1, probe))
end
