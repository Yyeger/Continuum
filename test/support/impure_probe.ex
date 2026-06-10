defmodule Continuum.Test.ImpureProbe do
  @moduledoc false

  # Audited impure helpers for test workflows.
  #
  # AstCheck forbids `self()`, `send/2`, `receive`, and `node()` inside
  # workflow code (including `side_effect` producer functions). Crash-resume
  # and cluster tests legitimately need them as scaffolding — to block a
  # producer mid-execution so the engine can be killed, or to journal which
  # node executed. Those uses are funneled through this module, which is
  # listed in `config :continuum, :trusted_modules` for the test env.

  @doc "The node currently executing, for journaling via side_effect."
  def current_node, do: node()

  @doc """
  Announce `{tag, self()}` to `test_pid`, then block until `:continue`
  arrives; returns `result`. Lets a test catch a workflow mid-side_effect.
  """
  def notify_and_block(test_pid, tag, result) do
    send(test_pid, {tag, self()})

    receive do
      :continue -> result
    end
  end

  @doc "Send `message` to `test_pid` from inside a side_effect producer."
  def notify(test_pid, message) do
    send(test_pid, message)
    :ok
  end
end
