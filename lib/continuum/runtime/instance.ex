defmodule Continuum.Runtime.Instance do
  @moduledoc false

  defstruct [
    :name,
    :repo,
    :journal,
    :pubsub,
    :registry,
    :run_supervisor,
    :activity_supervisor,
    :activity_executor,
    :heartbeater,
    :dispatcher,
    :activity_dispatcher,
    :timer_wheel,
    :signal_router,
    :snapshotter,
    :recovery,
    :workflow_modules
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
      journal: Keyword.get(opts, :journal) || default_journal(name, repo),
      pubsub: process_name(name, Continuum.PubSub),
      registry: process_name(name, Continuum.Runtime.Registry),
      run_supervisor: process_name(name, Continuum.Runtime.RunSupervisor),
      activity_supervisor: process_name(name, Continuum.Runtime.ActivityWorker.Supervisor),
      activity_executor:
        Keyword.get_lazy(opts, :activity_executor, fn -> default_activity_executor(name) end)
        |> normalize_activity_executor!(),
      heartbeater: process_name(name, Continuum.Runtime.Lease.Heartbeater),
      dispatcher: process_name(name, Continuum.Runtime.Dispatcher),
      activity_dispatcher: process_name(name, Continuum.Runtime.ActivityWorker.Dispatcher),
      timer_wheel: process_name(name, Continuum.Runtime.TimerWheel),
      signal_router: process_name(name, Continuum.Runtime.SignalRouter),
      snapshotter: process_name(name, Continuum.Runtime.Snapshotter),
      recovery: process_name(name, Continuum.Runtime.Recovery),
      workflow_modules: Keyword.get(opts, :workflow_modules)
    }
  end

  # Single source of truth for which journal adapter an instance uses. Every
  # consumer (Engine init/await/cancel, SignalRouter delivery and LISTEN
  # gating) resolves through here rather than re-deriving its own default.
  def journal(%Instance{journal: nil}), do: Continuum.Runtime.Journal.default()
  def journal(%Instance{journal: journal}), do: journal

  def child_name(%Instance{name: name}, module), do: process_name(name, module)
  def child_name(name, module), do: process_name(name, module)

  defp process_name(@default, module), do: module

  defp process_name(name, module) do
    Module.concat([Continuum.Instances, inspect_name(name), module_name(module)])
  end

  defp default_repo(@default), do: Application.get_env(:continuum, :repo)
  defp default_repo(_name), do: nil

  # Named instances pin their journal at construction: Postgres-backed when
  # given a repo (the documented `Continuum.children/1` use case), in-memory
  # otherwise. The default instance keeps `nil` so `journal/1` resolves
  # `config :continuum, :journal` at call time — config overrides in test
  # setup (and the README quickstart's config) keep working regardless of
  # when the instance struct was built.
  defp default_journal(@default, _repo), do: nil
  defp default_journal(_name, nil), do: Continuum.Runtime.Journal.InMemory
  defp default_journal(_name, _repo), do: Continuum.Runtime.Journal.Postgres

  defp default_activity_executor(@default) do
    Application.get_env(:continuum, :activity_executor, :builtin)
  end

  defp default_activity_executor(_name), do: :builtin

  defp normalize_activity_executor!(:builtin), do: :builtin

  defp normalize_activity_executor!({:oban, opts}) when is_list(opts) do
    ensure_oban_loaded!()
    {:oban, opts}
  end

  defp normalize_activity_executor!(:oban) do
    ensure_oban_loaded!()
    {:oban, []}
  end

  defp normalize_activity_executor!(other) do
    raise ArgumentError,
          "invalid Continuum activity executor #{inspect(other)}; expected :builtin or {:oban, opts}"
  end

  defp ensure_oban_loaded! do
    unless Code.ensure_loaded?(Oban) do
      raise ArgumentError,
            "Continuum activity_executor: :oban requires adding :oban to your application deps"
    end
  end

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
