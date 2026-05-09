defmodule Continuum.Runtime.Instance do
  @moduledoc false

  defstruct [
    :name,
    :repo,
    :pubsub,
    :registry,
    :run_supervisor,
    :activity_supervisor,
    :heartbeater,
    :dispatcher,
    :activity_dispatcher,
    :timer_wheel,
    :signal_router,
    :recovery
  ]

  alias __MODULE__

  @default Continuum

  @type t :: %__MODULE__{}

  def default, do: lookup(@default)

  def lookup(%Instance{} = instance), do: instance

  def lookup(nil), do: default()

  def lookup(name) do
    case :persistent_term.get({__MODULE__, name}, nil) do
      %Instance{} = instance -> instance
      nil when name == @default -> new(name: name, repo: default_repo(name))
      nil -> raise Continuum.InstanceNotRegisteredError, instance: name
    end
  end

  def register(%Instance{} = instance) do
    # Public APIs resolve names through this registry. `Continuum.children/1`
    # registers during child-spec construction so host applications can build
    # specs once and use the named instance immediately afterward.
    :persistent_term.put({__MODULE__, instance.name}, instance)
    instance
  end

  def new(opts) do
    name = Keyword.get(opts, :name, @default)
    repo = Keyword.get(opts, :repo) || default_repo(name)

    %Instance{
      name: name,
      repo: repo,
      pubsub: process_name(name, Continuum.PubSub),
      registry: process_name(name, Continuum.Runtime.Registry),
      run_supervisor: process_name(name, Continuum.Runtime.RunSupervisor),
      activity_supervisor: process_name(name, Continuum.Runtime.ActivityWorker.Supervisor),
      heartbeater: process_name(name, Continuum.Runtime.Lease.Heartbeater),
      dispatcher: process_name(name, Continuum.Runtime.Dispatcher),
      activity_dispatcher: process_name(name, Continuum.Runtime.ActivityWorker.Dispatcher),
      timer_wheel: process_name(name, Continuum.Runtime.TimerWheel),
      signal_router: process_name(name, Continuum.Runtime.SignalRouter),
      recovery: process_name(name, Continuum.Runtime.Recovery)
    }
  end

  def child_name(%Instance{name: name}, module), do: process_name(name, module)
  def child_name(name, module), do: process_name(name, module)

  defp process_name(@default, module), do: module

  defp process_name(name, module) do
    Module.concat([Continuum.Instances, inspect_name(name), module_name(module)])
  end

  defp default_repo(@default), do: Application.get_env(:continuum, :repo)
  defp default_repo(_name), do: nil

  defp inspect_name(name) do
    name
    |> to_string()
    |> Macro.camelize()
  end

  defp module_name(module) do
    module
    |> Module.split()
    |> Enum.join(".")
  end
end
