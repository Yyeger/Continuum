# Usage:
#
#   MIX_ENV=test mix run bench/timer_wheel_bench.exs [timer_count] [refresh_intervals]
#
defmodule TimerWheelBench do
  @moduledoc false

  import Ecto.Query

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.TimerWheel
  alias Continuum.Schema.{Run, Timer}
  alias Continuum.Test.Repo

  @workflow "TimerWheelBench.IdleTimer"
  @instance_name :timer_wheel_bench
  @default_timer_count 1_000
  @default_refresh_intervals 2
  @refresh_ms 30_000
  @window_ms 60_000
  @old_poll_ms 1_000
  @minimum_speedup 10.0

  def run(timer_count \\ @default_timer_count, refresh_intervals \\ @default_refresh_intervals) do
    start_repo!()
    checkout_sandbox_if_needed!()
    register_instance!()

    cleanup!()
    seed_idle_timers!(timer_count)

    {:ok, counter} = Agent.start_link(fn -> 0 end)
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler_id,
      telemetry_event(),
      fn _event, _measurements, metadata, _config ->
        if timer_select?(metadata[:query]) do
          Agent.update(counter, &(&1 + 1))
        end
      end,
      nil
    )

    try do
      cached_queries = measure_cached_queries!(counter, refresh_intervals)
      old_poller_queries = div(refresh_intervals * @refresh_ms, @old_poll_ms)
      speedup = old_poller_queries / max(cached_queries, 1)

      result = %{
        timers: timer_count,
        simulated_ms: refresh_intervals * @refresh_ms,
        old_poller_interval_ms: @old_poll_ms,
        timer_wheel_refresh_ms: @refresh_ms,
        pre_ets_poller_queries: old_poller_queries,
        ets_cached_timer_wheel_queries: cached_queries,
        db_query_reduction: Float.round(speedup, 1)
      }

      IO.inspect(result, label: "TimerWheel bench")

      if speedup < @minimum_speedup do
        raise """
        TimerWheel DB-query reduction below #{@minimum_speedup}x:
        #{inspect(result, pretty: true)}
        """
      end

      result
    after
      :telemetry.detach(handler_id)
      Agent.stop(counter)
      cleanup!()
    end
  end

  def parse_positive!(value, name) do
    integer = String.to_integer(value)

    if integer > 0 do
      integer
    else
      raise "#{name} must be a positive integer, got #{inspect(value)}"
    end
  end

  defp measure_cached_queries!(counter, refresh_intervals) do
    {:ok, pid} =
      TimerWheel.start_link(
        instance: @instance_name,
        enabled?: true,
        listen?: false,
        refresh_ms: @refresh_ms,
        window_ms: @window_ms
      )

    try do
      wait_for_count!(counter, 1)

      Enum.each(1..refresh_intervals, fn expected_refresh ->
        send(pid, :refresh)
        wait_for_count!(counter, expected_refresh + 1)
      end)

      Agent.get(counter, & &1)
    after
      GenServer.stop(pid)
    end
  end

  defp seed_idle_timers!(timer_count) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    fires_at = now |> DateTime.add(1, :day) |> DateTime.truncate(:microsecond)
    lease_expires_at = now |> DateTime.add(1, :hour) |> DateTime.truncate(:microsecond)

    runs_and_timers =
      Enum.map(1..timer_count, fn index ->
        run_id = Ecto.UUID.generate()
        timer_id = Ecto.UUID.generate()

        run = %{
          id: run_id,
          workflow: @workflow,
          version_hash: <<0::256>>,
          state: "suspended",
          input: :erlang.term_to_binary(%{}),
          started_at: now,
          lease_owner: "timer-wheel-bench",
          lease_token: index,
          lease_expires_at: lease_expires_at,
          next_wakeup_at: fires_at
        }

        timer = %{
          id: timer_id,
          run_id: run_id,
          fires_at: fires_at,
          fired: false
        }

        {run, timer}
      end)

    {runs, timers} = Enum.unzip(runs_and_timers)

    {^timer_count, _} = Repo.insert_all(Run, runs)
    {^timer_count, _} = Repo.insert_all(Timer, timers)
  end

  defp cleanup! do
    run_ids =
      Repo.all(
        from(r in Run,
          where: r.workflow == ^@workflow,
          select: r.id
        )
      )

    if run_ids != [] do
      Repo.delete_all(from(t in Timer, where: t.run_id in ^run_ids))
    end

    Repo.delete_all(from(r in Run, where: r.workflow == ^@workflow))
  end

  defp start_repo! do
    unless Code.ensure_loaded?(Repo) do
      raise "Continuum.Test.Repo is unavailable; run with MIX_ENV=test"
    end

    case Repo.start_link() do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp checkout_sandbox_if_needed! do
    if Repo.config()[:pool] == Ecto.Adapters.SQL.Sandbox do
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
      Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
    end
  end

  defp register_instance! do
    Instance.new(name: @instance_name, repo: Repo)
    |> Instance.register()
  end

  defp telemetry_event do
    (Repo.config()[:telemetry_prefix] || [:continuum, :test, :repo]) ++ [:query]
  end

  defp timer_select?(query) when is_binary(query) do
    String.contains?(query, "continuum_timers") and
      String.match?(query, ~r/^\s*SELECT/i)
  end

  defp timer_select?(_query), do: false

  defp wait_for_count!(counter, expected, attempts \\ 50)

  defp wait_for_count!(counter, expected, attempts) when attempts > 0 do
    if Agent.get(counter, & &1) >= expected do
      :ok
    else
      Process.sleep(20)
      wait_for_count!(counter, expected, attempts - 1)
    end
  end

  defp wait_for_count!(counter, expected, 0) do
    actual = Agent.get(counter, & &1)
    raise "expected at least #{expected} TimerWheel timer SELECTs, got #{actual}"
  end
end

timer_count =
  case System.argv() do
    [value | _] -> TimerWheelBench.parse_positive!(value, "timer_count")
    _ -> 1_000
  end

refresh_intervals =
  case System.argv() do
    [_timer_count, value | _] -> TimerWheelBench.parse_positive!(value, "refresh_intervals")
    _ -> 2
  end

TimerWheelBench.run(timer_count, refresh_intervals)
