defmodule Continuum.Test.ClusterFlows do
  @moduledoc false

  defmodule SideEffectFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      Continuum.side_effect(fn -> {:claimed_by, node(), input.value} end)
      {:ok, input.value}
    end
  end

  defmodule SignalFlow do
    use Continuum.Workflow, version: 1

    def run(_input) do
      await(signal(:continue))
    end
  end

  defmodule Activity do
    use Continuum.Activity, retry: [max_attempts: 1]

    def run(input) do
      send(input.test_pid, {:cluster_activity_started, node()})

      if String.contains?(Atom.to_string(node()), "activity_a") do
        Process.sleep(:infinity)
      else
        {:ok, input.value * 2}
      end
    end
  end

  defmodule ActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(Activity.run(input))
    end
  end
end
