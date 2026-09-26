defmodule Continuum.Observer.ReplayReport do
  @moduledoc false

  def run(run_id, opts) do
    timeout = limit!(opts, :timeout_ms, 2_000, 5_000)

    limits = %{
      events: limit!(opts, :max_events, 2_000, 10_000),
      history_bytes: limit!(opts, :max_history_bytes, 8_388_608, 16_777_216),
      payload_bytes: limit!(opts, :max_payload_bytes, 65_536, 1_048_576)
    }

    opts = Keyword.put(opts, :limits, limits)
    parent = self()
    reply = make_ref()
    callers = [parent | Process.get(:"$callers", [])]

    # A short-lived worker isolates replay CPU/allocation from the LiveView.
    # The guardian terminates it if its caller disappears before the timeout.
    {pid, monitor} =
      :erlang.spawn_opt(
        fn ->
          Process.put(:"$callers", callers)
          guard_owner(parent)
          send(parent, {reply, replay(run_id, opts)})
        end,
        [:monitor, {:max_heap_size, %{size: 4_000_000, kill: true, error_logger: false}}]
      )

    receive do
      {^reply, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, _reason} ->
        {:error, :replay_resource_limit}
    after
      timeout ->
        Process.exit(pid, :kill)
        Process.demonitor(monitor, [:flush])
        {:error, :replay_timeout}
    end
  end

  defp replay(run_id, opts) do
    case Continuum.Replay.of_run(run_id, opts) do
      {:error, {_kind, %{} = _details}} = error -> error
      {:error, %{} = _error} -> {:error, :replay_failed}
      result -> result
    end
  rescue
    # Fail closed: SQL/decoding/redactor exceptions may embed raw payloads.
    _ -> {:error, :replay_failed}
  catch
    _, _ -> {:error, :replay_failed}
  end

  defp guard_owner(owner) do
    worker = self()

    spawn(fn ->
      owner_ref = Process.monitor(owner)
      worker_ref = Process.monitor(worker)

      receive do
        {:DOWN, ^owner_ref, :process, ^owner, _} -> Process.exit(worker, :kill)
        {:DOWN, ^worker_ref, :process, ^worker, _} -> :ok
      end
    end)
  end

  defp limit!(opts, key, default, maximum) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value > 0 and value <= maximum,
      do: value,
      else: raise(ArgumentError, "#{key} must be between 1 and #{maximum}")
  end
end
