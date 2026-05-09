defmodule Continuum.InstanceNotRegisteredError do
  defexception [:instance]

  @impl true
  def message(%{instance: instance}) do
    "Continuum instance #{inspect(instance)} is not registered; start it with Continuum.children(name: ..., repo: ...) before using instance: #{inspect(instance)}"
  end
end
