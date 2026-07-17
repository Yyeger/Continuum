defmodule Continuum.ActivityOperations do
  @moduledoc """
  Operator-facing inspection, classification, dead-letter, and manual retry API.

  Mutations are dry runs by default. Pass `execute: true` after reviewing the
  returned plan. Every executed action requires and records `:operator` and
  `:reason`.

  A manual retry never edits the failed activity event. It appends an
  `activity_retry_scheduled` event, creates a successor task linked through a
  stable lineage id, and reopens the failed root run. This is intentionally
  limited to an activity failure at the replay tail; completed runs, child
  runs, compensations, and histories that proceeded past the failure are
  rejected rather than rewritten ambiguously.
  """

  import Ecto.Query

  alias Continuum.Activity.Policy
  alias Continuum.Runtime.{Instance, Journal.Postgres}
  alias Continuum.Schema.{ActivityAttempt, ActivityOperation, ActivityTask, Run}

  @type classification :: :retryable | :non_retryable

  @doc since: "0.6.2"
  @doc "Returns one activity task together with its full task lineage, attempts, and operator actions."
  @spec get(binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def get(task_id, opts \\ []) when is_binary(task_id) do
    with {:ok, instance} <- repo_instance(opts),
         %ActivityTask{} = task <- instance.repo.get(ActivityTask, task_id) do
      lineage_id = task.lineage_id || task.id

      tasks =
        instance.repo.all(
          from(t in ActivityTask,
            where: t.lineage_id == ^lineage_id or t.id == ^lineage_id
          )
        )
        |> Enum.sort_by(fn task ->
          {not is_nil(task.parent_task_id), task.scheduled_at, task.id}
        end)

      attempts =
        instance.repo.all(
          from(a in ActivityAttempt,
            where: a.lineage_id == ^lineage_id,
            order_by: [asc: a.recorded_at, asc: a.id]
          )
        )

      operations =
        instance.repo.all(
          from(o in ActivityOperation,
            where: o.lineage_id == ^lineage_id,
            order_by: [asc: o.inserted_at, asc: o.id]
          )
        )

      {:ok,
       %{
         task_id: task.id,
         run_id: task.run_id,
         lineage_id: lineage_id,
         classification: latest_classification(operations),
         tasks: Enum.map(tasks, &task_view/1),
         attempts: Enum.map(attempts, &attempt_view/1),
         operations: Enum.map(operations, &operation_view/1)
       }}
    else
      nil -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  rescue
    error -> {:error, error}
  end

  @doc since: "0.6.2"
  @doc "Classifies an activity lineage as retryable or non-retryable. Dry-run by default."
  @spec classify(binary(), classification() | binary(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def classify(task_id, classification, opts \\ []) when is_binary(task_id) do
    with {:ok, instance} <- repo_instance(opts),
         {:ok, classification} <- normalize_classification(classification),
         {:ok, operator, reason} <- audit_identity(opts),
         %ActivityTask{} = task <- instance.repo.get(ActivityTask, task_id) do
      plan = operation_plan(task, :classify, operator, reason, classification)

      if Keyword.get(opts, :execute, false) do
        insert_operation(instance.repo, task, "classify", operator, reason,
          classification: Atom.to_string(classification)
        )

        operated(instance, plan)
      else
        planned(plan)
      end
    else
      nil -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  rescue
    error -> {:error, error}
  end

  @doc since: "0.6.2"
  @doc "Moves a terminal discarded activity into the explicit dead-letter state. Dry-run by default."
  @spec dead_letter(binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def dead_letter(task_id, opts \\ []) when is_binary(task_id) do
    with {:ok, instance} <- repo_instance(opts),
         {:ok, operator, reason} <- audit_identity(opts),
         %ActivityTask{} = task <- instance.repo.get(ActivityTask, task_id),
         :ok <- ensure_state(task, ["discarded", "dead_lettered"]) do
      plan = operation_plan(task, :dead_letter, operator, reason, :non_retryable)

      if Keyword.get(opts, :execute, false) do
        execute_dead_letter(instance.repo, task, operator, reason)
        operated(instance, plan)
      else
        planned(plan)
      end
    else
      nil -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  rescue
    error -> {:error, error}
  end

  @doc since: "0.6.2"
  @doc "Appends and schedules a manual retry with a validated replacement policy. Dry-run by default."
  @spec retry(binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def retry(task_id, opts \\ []) when is_binary(task_id) do
    with {:ok, instance} <- repo_instance(opts),
         {:ok, operator, reason} <- audit_identity(opts),
         %ActivityTask{} = task <- instance.repo.get(ActivityTask, task_id),
         :ok <- ensure_retryable(instance.repo, task),
         {:ok, policy} <- retry_policy(task, opts) do
      plan =
        operation_plan(task, :manual_retry, operator, reason, :retryable)
        |> Map.put(:retry_policy, policy_view(policy))

      if Keyword.get(opts, :execute, false) do
        case Postgres.retry_discarded_activity!(instance, task.id, policy, operator, reason) do
          {:ok, retry} -> operated(instance, Map.merge(plan, retry))
          {:error, _reason} = error -> error
        end
      else
        planned(plan)
      end
    else
      nil -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  rescue
    error -> {:error, error}
  end

  defp execute_dead_letter(repo, task, operator, reason) do
    repo.transaction(fn ->
      locked = repo.one(from(t in ActivityTask, where: t.id == ^task.id, lock: "FOR UPDATE"))

      cond do
        is_nil(locked) ->
          repo.rollback(:not_found)

        locked.state == "dead_lettered" ->
          :already_applied

        locked.state != "discarded" ->
          repo.rollback(:not_discarded)

        true ->
          repo.update_all(from(t in ActivityTask, where: t.id == ^task.id),
            set: [state: "dead_lettered"]
          )

          insert_operation(repo, locked, "dead_letter", operator, reason,
            classification: "non_retryable"
          )

          :ok
      end
    end)
    |> case do
      {:ok, result} -> result
      {:error, reason} -> raise "dead-letter operation failed: #{inspect(reason)}"
    end
  end

  defp insert_operation(repo, task, action, operator, reason, opts) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    repo.insert!(%ActivityOperation{
      id: Ecto.UUID.generate(),
      task_id: task.id,
      run_id: task.run_id,
      lineage_id: task.lineage_id || task.id,
      action: action,
      classification: Keyword.get(opts, :classification),
      operator: operator,
      reason: reason,
      retry_policy: Keyword.get(opts, :retry_policy),
      inserted_at: now
    })
  end

  defp ensure_retryable(repo, task) do
    run = repo.get(Run, task.run_id)

    cond do
      task.state not in ["discarded", "dead_lettered"] ->
        {:error, :not_discarded}

      is_nil(run) ->
        {:error, :run_not_found}

      run.state != "failed" ->
        {:error, {:run_not_failed, run.state}}

      not is_nil(run.parent_run_id) ->
        {:error, :child_run_retry_not_supported}

      repo.exists?(from(t in ActivityTask, where: t.parent_task_id == ^task.id)) ->
        {:error, :already_retried}

      true ->
        terminal =
          repo.one(
            from(e in Continuum.Schema.Event,
              where: e.run_id == ^task.run_id and e.seq == ^(task.seq + 1)
            )
          )

        max_seq =
          repo.one(
            from(e in Continuum.Schema.Event,
              where: e.run_id == ^task.run_id,
              select: max(e.seq)
            )
          )

        if terminal && terminal.event_type == "activity_failed" && max_seq == terminal.seq,
          do: :ok,
          else: {:error, :failure_not_replay_tail}
    end
  end

  defp retry_policy(task, opts) do
    source = decode(task.mfa) || %{}
    policy_opts = Keyword.get(opts, :policy, [])

    unless Keyword.keyword?(policy_opts) do
      raise ArgumentError, ":policy must be a keyword list"
    end

    normalized =
      Policy.normalize!(
        retry: Keyword.get(policy_opts, :retry, Map.get(source, :retry, max_attempts: 1)),
        timeout: Keyword.get(policy_opts, :timeout, Map.get(source, :timeout_ms, 30_000)),
        idempotency_key: Map.get(source, :idempotency_key)
      )

    {:ok, normalized}
  end

  defp ensure_state(%ActivityTask{state: state}, allowed) do
    if state in allowed, do: :ok, else: {:error, :not_discarded}
  end

  defp audit_identity(opts) do
    operator = Keyword.get(opts, :operator)
    reason = Keyword.get(opts, :reason)

    if is_binary(operator) and operator != "" and is_binary(reason) and reason != "",
      do: {:ok, operator, reason},
      else: {:error, :operator_and_reason_required}
  end

  defp normalize_classification(value) when value in [:retryable, :non_retryable],
    do: {:ok, value}

  defp normalize_classification("retryable"), do: {:ok, :retryable}
  defp normalize_classification("non-retryable"), do: {:ok, :non_retryable}
  defp normalize_classification("non_retryable"), do: {:ok, :non_retryable}
  defp normalize_classification(value), do: {:error, {:invalid_classification, value}}

  defp operation_plan(task, action, operator, reason, classification) do
    %{
      action: action,
      task_id: task.id,
      subject_id: task.id,
      run_id: task.run_id,
      lineage_id: task.lineage_id || task.id,
      classification: classification,
      operator: operator,
      reason: reason
    }
  end

  defp planned(plan), do: {:ok, Map.put(plan, :status, :planned)}

  defp operated(instance, plan) do
    :telemetry.execute([:continuum, :activity, :operated], %{count: 1}, %{
      instance: instance.name,
      action: plan.action,
      task_id: plan.task_id,
      run_id: plan.run_id,
      operator: plan.operator
    })

    {:ok, Map.put(plan, :status, :executed)}
  end

  defp latest_classification(operations) do
    operations
    |> Enum.reverse()
    |> Enum.find_value(fn operation ->
      if operation.classification, do: String.to_atom(operation.classification)
    end)
  end

  defp task_view(task) do
    source = decode(task.mfa) || %{}

    %{
      task_id: task.id,
      parent_task_id: task.parent_task_id,
      state: String.to_atom(task.state),
      attempt: task.attempt,
      mfa: Map.get(source, :mfa),
      retry_policy: Map.get(source, :retry),
      timeout_ms: Map.get(source, :timeout_ms),
      scheduled_at: task.scheduled_at,
      available_at: task.available_at,
      result: decode(task.result),
      error: decode(task.error)
    }
  end

  defp attempt_view(attempt) do
    %{
      task_id: attempt.task_id,
      attempt: attempt.attempt,
      outcome: String.to_atom(attempt.outcome),
      error: decode(attempt.error),
      recorded_at: attempt.recorded_at
    }
  end

  defp operation_view(operation) do
    %{
      id: operation.id,
      task_id: operation.task_id,
      successor_task_id: operation.successor_task_id,
      action: String.to_atom(operation.action),
      classification: if(operation.classification, do: String.to_atom(operation.classification)),
      operator: operation.operator,
      reason: operation.reason,
      retry_policy: policy_view(decode(operation.retry_policy)),
      inserted_at: operation.inserted_at
    }
  end

  defp policy_view(%Policy{} = policy) do
    %{
      retry: Policy.retry_options(policy),
      timeout_ms: policy.timeout_ms,
      idempotency_key: policy.idempotency_key
    }
  end

  defp policy_view(nil), do: nil
  defp policy_view(other), do: other

  defp decode(nil), do: nil
  defp decode(binary) when is_binary(binary), do: :erlang.binary_to_term(binary, [:safe])

  defp repo_instance(opts) do
    case Keyword.get(opts, :repo) do
      nil ->
        instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
        if instance.repo, do: {:ok, instance}, else: {:error, :repo_not_configured}

      repo ->
        repo_instance_for_repo(repo, Keyword.get(opts, :instance))
    end
  end

  defp repo_instance_for_repo(repo, nil) do
    case Instance.running_for_repo(repo) do
      {:ok, instance} -> {:ok, instance}
      :none -> {:ok, Instance.new(name: :activity_operations, repo: repo)}
      {:error, _reason} = error -> error
    end
  end

  defp repo_instance_for_repo(repo, %Instance{} = instance) do
    if instance.repo == repo, do: {:ok, instance}, else: {:error, :instance_repo_mismatch}
  end

  defp repo_instance_for_repo(repo, instance_name) do
    repo_instance_for_repo(repo, Instance.lookup(instance_name))
  end
end
