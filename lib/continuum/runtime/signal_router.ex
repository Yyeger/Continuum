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
  @catch_up_batch_size 500

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
    case deliver_with_status(run_id, name, payload, opts) do
      {:ok, _status} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec deliver_unique(binary(), atom(), term(), binary(), keyword()) ::
          {:ok, :delivered | :duplicate} | {:error, term()}
  def deliver_unique(run_id, name, payload, delivery_id, opts \\ []) do
    deliver_with_status(run_id, name, payload, Keyword.put(opts, :delivery_id, delivery_id))
  end

  defp deliver_with_status(run_id, name, payload, opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    journal = Keyword.get(opts, :journal, Instance.journal(instance))

    with {:ok, run_id} <-
           Continuum.Runtime.NamespacePrecondition.resolve(instance, journal, run_id, opts) do
      case journal do
        Journal.Postgres -> deliver_durable(instance, run_id, name, payload, opts)
        Journal.InMemory -> deliver_local(instance, run_id, name, payload, opts)
        custom_journal -> deliver_custom(custom_journal, instance, run_id, name, payload, opts)
      end
    end
  end

  @doc """
  Scan for durable signal or wake evidence whose runs have a local engine and
  wake them. The LISTEN path is best-effort (notifications can be dropped, the
  listener can be down); this is the poll backstop the router runs periodically
  while listening, exposed for tests and operators. Pass `:batch_size` to bound
  the number of local run IDs checked by each database query.
  """
  @spec catch_up_once(keyword()) :: :ok
  def catch_up_once(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    catch_up(instance, catch_up_batch_size(opts, router_config()))
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
      catch_up_batch_size: catch_up_batch_size(opts, config),
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
    catch_up(state.instance, state.catch_up_batch_size)
    schedule_catch_up(state)
    {:noreply, state}
  end

  defp deliver_durable(instance, run_id, name, payload, opts) do
    # Delivery resolves continue_as_new chains to the live tip; wake that run,
    # not the (possibly dead) chain root the caller addressed.
    case Journal.Postgres.deliver_signal!(instance, run_id, name, payload, opts) do
      {:ok, delivered_run_id, :delivered} ->
        route(instance, delivered_run_id)
        {:ok, :delivered}

      {:ok, _delivered_run_id, :duplicate} ->
        {:ok, :duplicate}

      {:error, _reason} = error ->
        error
    end
  end

  # Buffer-then-wake, mirroring the durable mailbox: the engine consumes the
  # buffered payload when its replay reaches the matching `await signal`, so
  # early or out-of-order signals wait for their await instead of landing at
  # the journal tail (where replay would later read them as drift).
  defp deliver_local(instance, run_id, name, payload, opts) do
    case Journal.InMemory.deliver_signal!(instance, run_id, name, payload, opts) do
      {:ok, delivered_run_id, :delivered} ->
        Engine.wake(instance, delivered_run_id)

        Telemetry.execute([:continuum, :signal, :delivered], %{}, %{
          instance: instance.name,
          run_id: delivered_run_id,
          signal_name: name,
          durable?: false
        })

        {:ok, :delivered}

      {:ok, _delivered_run_id, :duplicate} ->
        {:ok, :duplicate}

      {:error, _reason} = error ->
        error
    end
  end

  defp deliver_custom(journal, instance, run_id, name, payload, opts) do
    Code.ensure_loaded(journal)
    arity = if function_exported?(journal, :deliver_signal!, 5), do: 5, else: 4

    if function_exported?(journal, :deliver_signal!, arity) do
      args = [instance, run_id, name, payload] ++ if(arity == 5, do: [opts], else: [])

      case apply(journal, :deliver_signal!, args) do
        :ok ->
          route(instance, run_id)
          {:ok, :delivered}

        {:ok, delivered_run_id} ->
          route(instance, delivered_run_id)
          {:ok, :delivered}

        {:ok, delivered_run_id, :delivered} ->
          route(instance, delivered_run_id)
          {:ok, :delivered}

        {:ok, _delivered_run_id, :duplicate} ->
          {:ok, :duplicate}

        {:error, _reason} = error ->
          error

        other ->
          {:error, {:invalid_signal_delivery_result, other}}
      end
    else
      {:error, :unsupported}
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
          catch_up(state.instance, state.catch_up_batch_size)
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

  defp catch_up(%Instance{repo: nil}, _batch_size), do: :ok

  defp catch_up(instance, batch_size) do
    local_run_ids = local_run_ids(instance)

    stats =
      local_run_ids
      |> Enum.chunk_every(batch_size)
      |> Enum.reduce_while(initial_catch_up_stats(length(local_run_ids)), fn run_ids, stats ->
        case catch_up_page(instance, run_ids) do
          {:ok, rows} ->
            {:cont, collect_catch_up_page(instance, rows, stats)}

          {:error, reason} ->
            Logger.warning("Continuum.SignalRouter catch-up scan failed: #{inspect(reason)}")
            {:halt, %{stats | page_count: stats.page_count + 1, status: :error}}
        end
      end)

    Telemetry.execute(
      [:continuum, :signal_router, :catch_up],
      Map.take(stats, [
        :scanned_count,
        :matched_count,
        :woken_count,
        :page_count,
        :oldest_wake_age_ms
      ]),
      %{instance: instance.name, batch_size: batch_size, status: stats.status}
    )

    :ok
  end

  defp catch_up_page(instance, local_run_ids) do
    sql = """
    WITH local_runs AS (
      SELECT r.id, r.next_wakeup_at
      FROM continuum_runs AS r
      WHERE r.id = ANY($1::uuid[])
        AND r.state IN ('running', 'suspended')
        AND r.lease_owner IS NOT NULL
        AND r.lease_token IS NOT NULL
        AND r.lease_expires_at > clock_timestamp()
    ), wake_evidence AS (
      SELECT s.run_id, s.inserted_at AS evidence_at
      FROM continuum_signals AS s
      JOIN local_runs AS r ON r.id = s.run_id
      WHERE s.delivered = false

      UNION ALL

      SELECT r.id AS run_id, r.next_wakeup_at AS evidence_at
      FROM local_runs AS r
      WHERE r.next_wakeup_at <= clock_timestamp()
    )
    SELECT run_id::text,
           GREATEST(
             FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - MIN(evidence_at))) * 1000),
             0
           )::bigint AS oldest_wake_age_ms
    FROM wake_evidence
    GROUP BY run_id
    ORDER BY MIN(evidence_at), run_id
    """

    encoded_run_ids = Enum.map(local_run_ids, &Ecto.UUID.dump!/1)

    case instance.repo.query(sql, [encoded_run_ids]) do
      {:ok, %{rows: rows}} -> {:ok, rows}
      {:error, reason} -> {:error, reason}
    end
  end

  defp collect_catch_up_page(instance, rows, stats) do
    woken_count = Enum.count(rows, fn [run_id, _age_ms] -> wake_local(instance, run_id) end)

    oldest_wake_age_ms =
      rows
      |> Enum.map(fn [_run_id, age_ms] -> age_ms end)
      |> Enum.max(fn -> 0 end)

    %{
      stats
      | matched_count: stats.matched_count + length(rows),
        woken_count: stats.woken_count + woken_count,
        page_count: stats.page_count + 1,
        oldest_wake_age_ms: max(stats.oldest_wake_age_ms, oldest_wake_age_ms)
    }
  end

  defp wake_local(instance, run_id) do
    # Registry membership is checked again after the query because an engine
    # can finish or lose its lease while its page is being scanned.
    case Registry.lookup(instance.registry, run_id) do
      [{_pid, _}] ->
        Engine.wake(instance, run_id)
        true

      [] ->
        false
    end
  end

  defp initial_catch_up_stats(scanned_count) do
    %{
      scanned_count: scanned_count,
      matched_count: 0,
      woken_count: 0,
      page_count: 0,
      oldest_wake_age_ms: 0,
      status: :ok
    }
  end

  defp local_run_ids(instance) do
    Registry.select(instance.registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp catch_up_batch_size(opts, config) do
    batch_size =
      Keyword.get(
        opts,
        :batch_size,
        Keyword.get(
          opts,
          :catch_up_batch_size,
          Keyword.get(config, :catch_up_batch_size, @catch_up_batch_size)
        )
      )

    if is_integer(batch_size) and batch_size > 0 do
      batch_size
    else
      raise ArgumentError,
            "catch-up batch size must be a positive integer, got: #{inspect(batch_size)}"
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
