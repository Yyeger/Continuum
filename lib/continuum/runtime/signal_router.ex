defmodule Continuum.Runtime.SignalRouter do
  @moduledoc """
  Routes external signals to workflow processes.

  With the Postgres journal, signals are durable: delivery inserts a row into
  `continuum_signals`, emits `pg_notify('continuum_signal', run_id)`, and wakes
  a local engine when one is registered. The engine consumes the mailbox row
  into a journaled `signal_received` event during replay.
  """

  use GenServer

  alias Continuum.{Runtime.Engine, Runtime.Journal, Telemetry}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Deliver a signal to a run."
  @spec deliver(binary(), atom(), term()) :: :ok | {:error, term()}
  def deliver(run_id, name, payload) do
    if postgres_run?(run_id) do
      :ok = Journal.Postgres.deliver_signal!(run_id, name, payload)
      route(run_id)
      :ok
    else
      deliver_local(run_id, name, payload)
    end
  end

  @impl true
  def init(opts) do
    config = router_config()

    state = %{
      listen?: Keyword.get(opts, :listen?, Keyword.get(config, :listen?, listen_enabled?())),
      notifier: nil,
      ref: nil
    }

    {:ok, start_listener(state)}
  end

  @impl true
  def handle_info({:notification, _pid, _ref, "continuum_signal", run_id}, state) do
    route(run_id)
    {:noreply, state}
  end

  def handle_info({:notification, _pid, _ref, _channel, _payload}, state), do: {:noreply, state}

  defp deliver_local(run_id, name, payload) do
    case Registry.lookup(Continuum.Runtime.Registry, run_id) do
      [{_pid, _value}] ->
        append_in_memory_signal!(run_id, name, payload)
        Engine.wake(run_id)

        Telemetry.execute([:continuum, :signal, :delivered], %{}, %{
          run_id: run_id,
          signal_name: name,
          durable?: false
        })

        :ok

      [] ->
        {:error, :not_found}
    end
  end

  defp append_in_memory_signal!(run_id, name, payload) do
    Journal.InMemory.append!(
      run_id,
      %{type: :signal_received, name: name, payload: payload, seq: nil},
      nil
    )
  end

  defp route(run_id) do
    Engine.wake(run_id)
    :ok
  end

  defp postgres_run?(run_id) do
    try do
      Application.get_env(:continuum, :repo) != nil and Journal.Postgres.get_run(run_id) != nil
    rescue
      _ -> false
    catch
      :exit, _ -> false
    end
  end

  defp start_listener(%{listen?: false} = state), do: state

  defp start_listener(state) do
    repo = Application.get_env(:continuum, :repo)

    if repo == nil do
      state
    else
      config = repo.config()

      case Postgrex.Notifications.start_link(config) do
        {:ok, notifier} ->
          {:ok, ref} = Postgrex.Notifications.listen(notifier, "continuum_signal")
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

  defp listen_enabled? do
    Application.get_env(:continuum, :repo) != nil
  end
end
