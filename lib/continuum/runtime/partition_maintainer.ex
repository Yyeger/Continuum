defmodule Continuum.Runtime.PartitionMaintainer do
  @moduledoc false

  use GenServer
  require Logger

  alias Continuum.{Partitions, Runtime.Instance}

  @default_interval_ms :timer.hours(6)

  @doc false
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.get(opts, :instance, Continuum)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  @doc false
  def start_link(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    GenServer.start_link(__MODULE__, opts, name: instance.partition_maintainer)
  end

  @doc "Runs one scheduled partition maintenance pass immediately."
  @spec maintain_once(Instance.t() | atom()) :: {:ok, map()} | {:error, term()}
  def maintain_once(instance_or_name \\ Continuum) do
    instance = Instance.lookup(instance_or_name)
    GenServer.call(instance.partition_maintainer, :maintain, :infinity)
  end

  @doc false
  def status(instance_or_name \\ Continuum) do
    instance = Instance.lookup(instance_or_name)
    GenServer.call(instance.partition_maintainer, :status)
  end

  @impl true
  def init(opts) do
    opts = Continuum.Config.validate_component!(:partition_maintainer, opts)
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    if is_nil(instance.repo) do
      {:stop, :repo_not_configured}
    else
      state = %{
        instance: instance,
        months: Keyword.get(opts, :months, 4),
        start_month: Keyword.get(opts, :start_month),
        interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
        timer_ref: nil,
        state: :starting,
        last_result: nil,
        last_error: nil,
        last_run_at: nil
      }

      {:ok, schedule(state, Keyword.get(opts, :initial_delay_ms, 0))}
    end
  end

  @impl true
  def handle_call(:maintain, _from, state) do
    {reply, state} = run_maintenance(state, :wait)
    {:reply, reply, state}
  end

  def handle_call(:status, _from, state), do: {:reply, status_map(state), state}

  @impl true
  def handle_info(:maintain, state) do
    {_result, state} = run_maintenance(%{state | timer_ref: nil}, :try)
    {:noreply, schedule(state, state.interval_ms)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp run_maintenance(state, lock) do
    opts = [
      instance: state.instance,
      months: state.months,
      lock: lock,
      source: :scheduled
    ]

    opts =
      if state.start_month, do: Keyword.put(opts, :start_month, state.start_month), else: opts

    case Partitions.ensure(opts) do
      {:ok, summary} = result ->
        state = %{
          state
          | state: if(summary.status == :ok, do: :ready, else: :skipped),
            last_result: summary,
            last_error: nil,
            last_run_at: DateTime.utc_now()
        }

        {result, state}

      {:error, reason} = error ->
        Logger.error(
          "Continuum partition maintenance failed for #{inspect(state.instance.name)}: #{inspect(reason)}"
        )

        state = %{
          state
          | state: :degraded,
            last_error: reason,
            last_run_at: DateTime.utc_now()
        }

        {error, state}
    end
  end

  defp schedule(state, delay_ms) when is_integer(delay_ms) and delay_ms >= 0 do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
    %{state | timer_ref: Process.send_after(self(), :maintain, delay_ms)}
  end

  defp status_map(state) do
    %{
      state: state.state,
      instance: state.instance.name,
      months: state.months,
      interval_ms: state.interval_ms,
      last_result: state.last_result,
      last_error: state.last_error,
      last_run_at: state.last_run_at
    }
  end
end
