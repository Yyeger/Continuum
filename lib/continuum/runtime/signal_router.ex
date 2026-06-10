defmodule Continuum.Runtime.SignalRouter do
  @moduledoc """
  Routes external signals and child-completion wakeups to workflow processes.

  With the Postgres journal, signals are durable: delivery inserts a row into
  `continuum_signals`, emits `pg_notify('continuum_signal', run_id)`, and wakes
  a local engine when one is registered. The engine consumes the mailbox row
  into a journaled `signal_received` event during replay.

  This process also listens on `continuum_run_wake` — emitted when a child run
  reaches a terminal state — and wakes the parent's local engine so an awaiting
  parent resumes promptly. No separate listener process: a parent wakeup routes
  through the same "find local pid in Registry, wake it, else rely on the
  Dispatcher poll" path as a signal.
  """

  use GenServer

  alias Continuum.{Runtime.Engine, Runtime.Instance, Runtime.Journal, Telemetry}

  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    GenServer.start_link(__MODULE__, opts, name: instance.signal_router)
  end

  @doc "Deliver a signal to a run."
  @spec deliver(binary(), atom(), term()) :: :ok | {:error, term()}
  def deliver(run_id, name, payload) do
    deliver(run_id, name, payload, [])
  end

  @spec deliver(binary(), atom(), term(), keyword()) :: :ok | {:error, term()}
  def deliver(run_id, name, payload, opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    journal = Keyword.get(opts, :journal, Instance.journal(instance))

    case journal do
      Journal.Postgres -> deliver_durable(instance, run_id, name, payload)
      _journal -> deliver_local(instance, run_id, name, payload)
    end
  end

  @impl true
  def init(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    config = router_config()

    state = %{
      instance: instance,
      listen?:
        Keyword.get(opts, :listen?, Keyword.get(config, :listen?, listen_enabled?(instance))),
      notifier: nil,
      ref: nil
    }

    {:ok, start_listener(state)}
  end

  @impl true
  def handle_info({:notification, _pid, _ref, "continuum_signal", run_id}, state) do
    route(state.instance, run_id)
    {:noreply, state}
  end

  def handle_info({:notification, _pid, _ref, "continuum_run_wake", run_id}, state) do
    route(state.instance, run_id)
    {:noreply, state}
  end

  def handle_info({:notification, _pid, _ref, _channel, _payload}, state), do: {:noreply, state}

  defp deliver_durable(instance, run_id, name, payload) do
    :ok = Journal.Postgres.deliver_signal!(instance, run_id, name, payload)
    route(instance, run_id)
    :ok
  end

  defp deliver_local(instance, run_id, name, payload) do
    case Registry.lookup(instance.registry, run_id) do
      [{_pid, _value}] ->
        append_in_memory_signal!(instance, run_id, name, payload)
        Engine.wake(instance, run_id)

        Telemetry.execute([:continuum, :signal, :delivered], %{}, %{
          instance: instance.name,
          run_id: run_id,
          signal_name: name,
          durable?: false
        })

        :ok

      [] ->
        {:error, :not_found}
    end
  end

  defp append_in_memory_signal!(instance, run_id, name, payload) do
    Journal.InMemory.append!(
      instance,
      run_id,
      %{type: :signal_received, name: name, payload: payload, seq: nil},
      nil
    )
  end

  defp route(instance, run_id) do
    Engine.wake(instance, run_id)
    :ok
  end

  defp start_listener(%{listen?: false} = state), do: state

  defp start_listener(state) do
    if state.instance.repo == nil do
      state
    else
      config = state.instance.repo.config()

      case Postgrex.Notifications.start_link(config) do
        {:ok, notifier} ->
          {:ok, ref} = Postgrex.Notifications.listen(notifier, "continuum_signal")
          {:ok, _wake_ref} = Postgrex.Notifications.listen(notifier, "continuum_run_wake")
          %{state | notifier: notifier, ref: ref}

        {:error, _reason} ->
          state
      end
    end
  end

  defp router_config do
    case Application.get_env(:continuum, :signal_router, []) do
      false -> [listen?: false]
      true -> [listen?: true]
      opts when is_list(opts) -> opts
    end
  end

  defp listen_enabled?(instance) do
    Instance.journal(instance) == Journal.Postgres and instance.repo != nil
  end
end
