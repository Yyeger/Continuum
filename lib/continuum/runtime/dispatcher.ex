defmodule Continuum.Runtime.Dispatcher do
  @moduledoc """
  Polls Postgres for runnable workflow runs and starts local engines.

  The dispatcher claims work with `SELECT ... FOR UPDATE SKIP LOCKED` inside a
  transaction, assigns a fresh fencing token, and then resumes an engine with
  that token. Multiple dispatchers can poll concurrently; locked rows are
  skipped rather than contended.
  """

  use GenServer
  require Logger

  alias Continuum.{Runtime.Engine, Runtime.Journal, Runtime.Lease, Telemetry}

  @default_interval_ms 1_000
  @default_batch_size 10
  @default_ttl_seconds 30

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Run one dispatch pass synchronously.
  """
  @spec dispatch_once(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def dispatch_once(opts \\ []) do
    owner = Keyword.get_lazy(opts, :owner, &Lease.owner/0)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)

    with {:ok, claimed} <- claim(owner, batch_size, ttl_seconds) do
      Enum.each(claimed, &start_engine/1)

      Telemetry.execute([:continuum, :dispatcher, :polled], %{count: length(claimed)}, %{
        owner: owner,
        batch_size: batch_size
      })

      {:ok, length(claimed)}
    end
  end

  @impl true
  def init(opts) do
    config = dispatcher_config()

    state = %{
      enabled?:
        Keyword.get(opts, :enabled?, Keyword.get(config, :enabled?, dispatcher_enabled?())),
      interval_ms:
        Keyword.get(opts, :interval_ms, Keyword.get(config, :interval_ms, @default_interval_ms)),
      batch_size:
        Keyword.get(opts, :batch_size, Keyword.get(config, :batch_size, @default_batch_size)),
      ttl_seconds:
        Keyword.get(opts, :ttl_seconds, Keyword.get(config, :ttl_seconds, @default_ttl_seconds))
    }

    if state.enabled?, do: schedule_poll(0)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    case dispatch_once(batch_size: state.batch_size, ttl_seconds: state.ttl_seconds) do
      {:ok, _count} ->
        :ok

      {:error, reason} ->
        Logger.error("Continuum dispatcher poll failed: #{inspect(reason)}")
    end

    schedule_poll(state.interval_ms)
    {:noreply, state}
  end

  defp claim(owner, batch_size, ttl_seconds) do
    sql = """
    WITH candidates AS (
      SELECT id
      FROM continuum_runs
      WHERE state IN ('running', 'suspended')
        AND (state = 'running' OR next_wakeup_at IS NULL OR next_wakeup_at <= now())
        AND (lease_owner IS NULL OR lease_expires_at < now())
      ORDER BY next_wakeup_at NULLS FIRST, started_at
      FOR UPDATE SKIP LOCKED
      LIMIT $2
    )
    UPDATE continuum_runs AS r
    SET lease_owner = $1,
        lease_token = nextval('continuum_lease_token_seq'),
        lease_expires_at = now() + make_interval(secs => $3)
    FROM candidates
    WHERE r.id = candidates.id
    RETURNING r.id::text, r.workflow, r.input, r.lease_token
    """

    case repo().query(sql, [owner, batch_size, ttl_seconds]) do
      {:ok, %{rows: rows}} ->
        claimed = Enum.map(rows, &decode_claim(owner, &1))

        Telemetry.execute([:continuum, :dispatcher, :claimed], %{count: length(claimed)}, %{
          owner: owner,
          batch_size: batch_size
        })

        {:ok, claimed}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_claim(owner, [run_id, workflow, input, token]) do
    %{
      run_id: run_id,
      workflow_module: Module.concat([workflow]),
      input: decode_term(input),
      lease_owner: owner,
      lease_token: token
    }
  end

  defp start_engine(claim) do
    opts = [
      journal: Journal.Postgres,
      lease_owner: claim.lease_owner,
      lease_token: claim.lease_token
    ]

    case Engine.resume_run(claim.workflow_module, claim.input, claim.run_id, opts) do
      {:ok, _run_id} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        Logger.error("Continuum dispatcher failed to start #{claim.run_id}: #{inspect(reason)}")
        :error
    end
  end

  defp dispatcher_config do
    case Application.get_env(:continuum, :dispatcher, []) do
      false -> [enabled?: false]
      true -> [enabled?: true]
      opts when is_list(opts) -> opts
    end
  end

  defp dispatcher_enabled? do
    Application.get_env(:continuum, :repo) != nil
  end

  defp decode_term(nil), do: nil
  defp decode_term(binary) when is_binary(binary), do: :erlang.binary_to_term(binary)
  defp decode_term(other), do: other

  defp repo do
    Application.fetch_env!(:continuum, :repo)
  end

  defp schedule_poll(interval_ms) do
    Process.send_after(self(), :poll, interval_ms)
  end
end
