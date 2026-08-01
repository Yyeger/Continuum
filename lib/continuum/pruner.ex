defmodule Continuum.Pruner do
  @moduledoc """
  Plans and executes bounded pruning of fully terminal logical run chains.

  History retention and idempotency retention are independent. The default
  `:keep` policy preserves activity results and external ingress keys after run
  history is removed. `:delete` is an explicit operator choice.
  """

  import Ecto.Query

  alias Continuum.Schema.{
    ActivityAttempt,
    ActivityOperation,
    ActivityResult,
    ActivityTask,
    Event,
    Run,
    RunIngressKey,
    Signal,
    Snapshot,
    Timer
  }

  @terminal_states ~w(completed failed cancelled)
  @default_batch_size 100
  @max_batch_size 1_000

  @type idempotency_policy :: :keep | :delete
  @type plan :: %{
          chains: [map()],
          run_ids: [binary()],
          chain_count: non_neg_integer(),
          run_count: non_neg_integer(),
          dependent_counts: map(),
          idempotency_policy: idempotency_policy()
        }

  @doc "Returns the next safe, bounded pruning plan without mutating data."
  @spec plan(keyword()) :: {:ok, plan()} | {:error, term()}
  def plan(opts) do
    with {:ok, repo} <- fetch_repo(opts),
         {:ok, batch_size} <- batch_size(opts),
         {:ok, older_than_days} <- older_than_days(opts),
         {:ok, idempotency_policy} <- idempotency_policy(opts) do
      chains = candidate_chains(repo, batch_size, older_than_days)
      run_ids = Enum.flat_map(chains, & &1.run_ids)

      {:ok,
       %{
         chains: chains,
         run_ids: run_ids,
         chain_count: length(chains),
         run_count: length(run_ids),
         dependent_counts: dependent_counts(repo, run_ids),
         idempotency_policy: idempotency_policy
       }}
    end
  rescue
    error -> {:error, error}
  end

  @doc "Executes a previously returned plan after locking and revalidating it."
  @spec execute(plan(), keyword()) :: {:ok, map()} | {:error, term()}
  def execute(%{run_ids: []} = plan, _opts) do
    {:ok, Map.merge(plan, %{deleted_run_count: 0, deleted_idempotency_count: 0})}
  end

  def execute(plan, opts) do
    with {:ok, repo} <- fetch_repo(opts) do
      repo.transaction(fn -> execute_transaction!(repo, plan) end)
    end
  rescue
    error -> {:error, error}
  end

  defp candidate_chains(repo, batch_size, older_than_days) do
    %{rows: rows} =
      repo.query!(
        """
        WITH chain_stats AS (
          SELECT
            COALESCE(r.correlation_id, r.id) AS logical_id,
            array_agg(r.id::text ORDER BY r.started_at, r.id) AS run_ids,
            min(r.completed_at) AS oldest_completed_at,
            max(r.completed_at) AS newest_completed_at,
            bool_and(r.state = ANY($1)) AS terminal,
            bool_and(r.completed_at IS NOT NULL) AS completed,
            bool_and(r.retention_until IS NOT NULL AND r.retention_until < now()) AS expired,
            bool_and(
              r.completed_at < (now() - ($2::int * interval '1 day'))
            ) AS older_than_cutoff
          FROM continuum_runs r
          GROUP BY COALESCE(r.correlation_id, r.id)
        )
        SELECT cs.logical_id::text, cs.run_ids, cs.oldest_completed_at, cs.newest_completed_at
        FROM chain_stats cs
        WHERE cs.terminal
          AND cs.completed
          AND cs.expired
          AND cs.older_than_cutoff
          AND NOT EXISTS (
            SELECT 1
            FROM continuum_runs child
            WHERE child.parent_run_id::text = ANY(cs.run_ids)
          )
        ORDER BY cs.oldest_completed_at, cs.logical_id
        LIMIT $3
        """,
        [@terminal_states, older_than_days, batch_size]
      )

    Enum.map(rows, fn [logical_id, run_ids, oldest, newest] ->
      %{
        logical_id: logical_id,
        run_ids: run_ids,
        oldest_completed_at: oldest,
        newest_completed_at: newest
      }
    end)
  end

  defp execute_transaction!(repo, plan) do
    run_ids = plan.run_ids

    locked =
      repo.all(
        from(r in Run,
          where: r.id in ^run_ids,
          lock: "FOR UPDATE",
          select: {r.id, r.state, r.retention_until}
        )
      )

    unless length(locked) == length(run_ids) and Enum.all?(locked, &still_prunable?/1) do
      repo.rollback(:plan_changed)
    end

    if repo.exists?(from(r in Run, where: r.parent_run_id in ^run_ids)) do
      repo.rollback(:child_lineage_changed)
    end

    delete_history(repo, run_ids)

    deleted_idempotency_count =
      case plan.idempotency_policy do
        :keep -> 0
        :delete -> delete_idempotency(repo, run_ids)
      end

    {deleted_run_count, _} = repo.delete_all(from(r in Run, where: r.id in ^run_ids))

    Map.merge(plan, %{
      deleted_run_count: deleted_run_count,
      deleted_idempotency_count: deleted_idempotency_count
    })
  end

  defp still_prunable?({_id, state, %DateTime{} = retention_until})
       when state in @terminal_states do
    DateTime.before?(retention_until, DateTime.utc_now())
  end

  defp still_prunable?(_run), do: false

  defp dependent_counts(_repo, []), do: %{}

  defp dependent_counts(repo, run_ids) do
    %{
      events: count(repo, Event, run_ids),
      snapshots: count(repo, Snapshot, run_ids),
      timers: count(repo, Timer, run_ids),
      signals: count(repo, Signal, run_ids),
      activity_attempts: count(repo, ActivityAttempt, run_ids),
      activity_operations: count(repo, ActivityOperation, run_ids),
      activity_tasks: count(repo, ActivityTask, run_ids),
      activity_results: count(repo, ActivityResult, run_ids),
      ingress_keys: count(repo, RunIngressKey, run_ids)
    }
  end

  defp count(repo, schema, run_ids) do
    repo.aggregate(from(row in schema, where: row.run_id in ^run_ids), :count)
  end

  defp delete_history(repo, run_ids) do
    delete_all(repo, Event, run_ids)
    delete_all(repo, Snapshot, run_ids)
    delete_all(repo, Timer, run_ids)
    delete_all(repo, Signal, run_ids)
    delete_all(repo, ActivityAttempt, run_ids)
    delete_all(repo, ActivityOperation, run_ids)
    delete_all(repo, ActivityTask, run_ids)
  end

  defp delete_idempotency(repo, run_ids) do
    {activity_results, _} =
      repo.delete_all(from(result in ActivityResult, where: result.run_id in ^run_ids))

    {ingress_keys, _} =
      repo.delete_all(from(key in RunIngressKey, where: key.run_id in ^run_ids))

    activity_results + ingress_keys
  end

  defp delete_all(repo, schema, run_ids) do
    repo.delete_all(from(row in schema, where: row.run_id in ^run_ids))
    :ok
  end

  defp fetch_repo(opts) do
    case Keyword.get(opts, :repo) || Application.get_env(:continuum, :repo) do
      nil -> {:error, :repo_not_configured}
      repo -> {:ok, repo}
    end
  end

  defp batch_size(opts) do
    case Keyword.get(opts, :batch_size, @default_batch_size) do
      size when is_integer(size) and size > 0 -> {:ok, min(size, @max_batch_size)}
      value -> {:error, {:invalid_batch_size, value}}
    end
  end

  defp older_than_days(opts) do
    case Keyword.get(opts, :older_than_days, 0) do
      days when is_integer(days) and days >= 0 -> {:ok, days}
      value -> {:error, {:invalid_older_than_days, value}}
    end
  end

  defp idempotency_policy(opts) do
    case Keyword.get(opts, :idempotency_policy, :keep) do
      policy when policy in [:keep, :delete] -> {:ok, policy}
      value -> {:error, {:invalid_idempotency_policy, value}}
    end
  end
end
