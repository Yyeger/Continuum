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
  require Logger

  alias Continuum.{Runtime.Engine, Runtime.Instance, Runtime.Journal, Telemetry}

  @listener_retry_ms 5_000
  @catch_up_interval_ms 30_000

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

  @doc """
  Scan `continuum_signals` for undelivered rows whose runs have a local engine
  and wake them. The LISTEN path is best-effort (notifications can be dropped,
  the listener can be down); this is the poll backstop the router runs
  periodically while listening, exposed for tests and operators.
  """
  @spec catch_up_once(keyword()) :: :ok
  def catch_up_once(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    catch_up(instance)
  end

  @impl true
  def init(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    config = router_config()

    state = %{
      instance: instance,
      listen?:
        Keyword.get(opts, :listen?, Keyword.get(config, :listen?, listen_enabled?(instance))),
      catch_up_interval_ms:
        Keyword.get(
          opts,
          :catch_up_interval_ms,
          Keyword.get(config, :catch_up_interval_ms, @catch_up_interval_ms)
        ),
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

  def handle_info(:start_listener, state) do
    {:noreply, start_listener(state)}
  end

  def handle_info(:catch_up, state) do
    catch_up(state.instance)
    schedule_catch_up(state)
    {:noreply, state}
  end

  defp deliver_durable(instance, run_id, name, payload) do
    # Delivery resolves continue_as_new chains to the live tip; wake that run,
    # not the (possibly dead) chain root the caller addressed.
    case Journal.Postgres.deliver_signal!(instance, run_id, name, payload) do
      {:ok, delivered_run_id} ->
        route(instance, delivered_run_id)
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  # Buffer-then-wake, mirroring the durable mailbox: the engine consumes the
  # buffered payload when its replay reaches the matching `await signal`, so
  # early or out-of-order signals wait for their await instead of landing at
  # the journal tail (where replay would later read them as drift).
  defp deliver_local(instance, run_id, name, payload) do
    case Journal.InMemory.deliver_signal!(instance, run_id, name, payload) do
      :ok ->
        Engine.wake(instance, run_id)

        Telemetry.execute([:continuum, :signal, :delivered], %{}, %{
          instance: instance.name,
          run_id: run_id,
          signal_name: name,
          durable?: false
        })

        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp route(instance, run_id) do
    Engine.wake(instance, run_id)
    :ok
  end

  defp start_listener(%{listen?: false} = state), do: state

  defp start_listener(%{notifier: notifier} = state) when is_pid(notifier), do: state

  defp start_listener(state) do
    if state.instance.repo == nil do
      state
    else
      config = state.instance.repo.config()

      case Postgrex.Notifications.start_link(config) do
        {:ok, notifier} ->
          {:ok, ref} = Postgrex.Notifications.listen(notifier, "continuum_signal")
          {:ok, _wake_ref} = Postgrex.Notifications.listen(notifier, "continuum_run_wake")

          # Anything delivered while we were deaf is woken now; afterwards the
          # periodic backstop covers dropped notifications.
          catch_up(state.instance)
          schedule_catch_up(state)

          %{state | notifier: notifier, ref: ref}

        {:error, reason} ->
          # A node that silently never LISTENs is deaf to signals and parent
          # wakeups forever — log and retry instead of giving up at init.
          Logger.warning(
            "Continuum.SignalRouter listener failed to start " <>
              "(#{inspect(reason)}); retrying in #{@listener_retry_ms}ms"
          )

          Process.send_after(self(), :start_listener, @listener_retry_ms)
          state
      end
    end
  end

  defp schedule_catch_up(state) do
    Process.send_after(self(), :catch_up, state.catch_up_interval_ms)
  end

  defp catch_up(%Instance{repo: nil}), do: :ok

  defp catch_up(instance) do
    sql = """
    SELECT DISTINCT s.run_id::text
    FROM continuum_signals AS s
    JOIN continuum_runs AS r ON r.id = s.run_id
    WHERE s.delivered = false
      AND r.state IN ('running', 'suspended')
    """

    case instance.repo.query(sql, []) do
      {:ok, %{rows: rows}} ->
        Enum.each(rows, fn [run_id] ->
          # Local engines only: parked engines holding live leases are
          # invisible to the dispatcher, so a missed wake strands them.
          # Runs without a local engine are the dispatcher's job.
          case Registry.lookup(instance.registry, run_id) do
            [{_pid, _}] -> Engine.wake(instance, run_id)
            [] -> :ok
          end
        end)

      {:error, reason} ->
        Logger.warning("Continuum.SignalRouter catch-up scan failed: #{inspect(reason)}")
        :ok
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
