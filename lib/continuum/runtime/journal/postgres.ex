defmodule Continuum.Runtime.Journal.Postgres do
  @moduledoc """
  Durable journal adapter backed by Postgres via Ecto.

  Implements the `Continuum.Runtime.Journal` behaviour. Every append
  operation is transactional and CAS-guarded by the lease state on the
  run row. Appends lock the run row, validate the lease token, assign a
  sequence number, and insert the event in one transaction.

  The replay loop and engine code are identical whether this adapter or
  `InMemory` is in use — the only difference is durability and the
  fencing-token enforcement on writes.
  """

  @behaviour Continuum.Runtime.Journal

  import Ecto.Query

  alias Continuum.Runtime.{Instance, Snapshotter}
  alias Continuum.Schema.{ActivityResult, ActivityTask, Event, Run, Signal, Snapshot, Timer}
  alias Continuum.Telemetry

  @impl true
  def start_run(%Instance{} = instance, run_id, workflow, input, opts \\ []) do
    with_repo(instance, fn -> start_run_with_repo(run_id, workflow, input, opts) end)
  end

  defp start_run_with_repo(run_id, workflow, input, opts) do
    metadata = workflow_metadata(workflow)

    changeset =
      %Run{}
      |> Ecto.Changeset.change(%{
        id: run_id,
        workflow: metadata.workflow,
        version_hash: metadata.version_hash,
        namespace: normalize_namespace(Keyword.get(opts, :namespace, "default")),
        state: "running",
        input: encode_term(input),
        attributes: normalize_attributes(Keyword.get(opts, :attributes, %{})),
        correlation_id: run_id,
        trace_context: Keyword.get(opts, :trace_context)
      })

    case repo().insert(changeset) do
      {:ok, _} -> :ok
      {:error, changeset} -> {:error, changeset}
    end
  end

  defp workflow_metadata(workflow) do
    case Continuum.VersionRegistry.ensure_registered(workflow) do
      {:ok, metadata} ->
        %{workflow: metadata.workflow_string, version_hash: metadata.version_hash}

      {:error, reason} ->
        raise ArgumentError,
              "expected #{inspect(workflow)} to use Continuum.Workflow before starting a durable run, got: #{inspect(reason)}"
    end
  end

  defp normalize_attributes(nil), do: %{}

  defp normalize_attributes(attributes) when is_map(attributes) do
    case Jason.encode(attributes) do
      {:ok, json} ->
        Jason.decode!(json)

      {:error, reason} ->
        raise ArgumentError,
              "expected :attributes to be JSON-encodable map data, got: #{inspect(reason)}"
    end
  end

  defp normalize_attributes(other) do
    raise ArgumentError, "expected :attributes to be a map, got: #{inspect(other)}"
  end

  defp normalize_namespace(nil), do: "default"

  defp normalize_namespace(namespace) when is_binary(namespace) and byte_size(namespace) > 0,
    do: namespace

  defp normalize_namespace(other) do
    raise ArgumentError, "expected :namespace to be a non-empty binary, got: #{inspect(other)}"
  end

  @impl true
  def append!(%Instance{} = instance, run_id, event, lease_token) do
    :ok = with_repo(instance, fn -> append_with_repo!(run_id, event, lease_token) end)
    maybe_snapshot_after_event(instance, run_id, event, lease_token)
  end

  defp append_with_repo!(run_id, event, lease_token) do
    {event_type, payload} = encode_event(event)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        seq = event[:seq] || next_seq(run_id)

        changeset =
          %Event{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            seq: seq,
            event_type: event_type,
            payload: payload,
            inserted_at: DateTime.utc_now()
          })

        case repo().insert(changeset) do
          {:ok, _} -> :ok
          {:error, changeset} -> repo().rollback({:insert_failed, changeset})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres append! failed: #{inspect(reason)}"
    end
  end

  @impl true
  def load(%Instance{} = instance, run_id) do
    with_repo(instance, fn -> load_with_repo(run_id) end)
  end

  defp load_with_repo(run_id) do
    events =
      repo().all(
        from(e in Event,
          where: e.run_id == ^run_id,
          order_by: [asc: e.seq]
        )
      )

    Enum.map(events, &decode_event/1)
  end

  @impl true
  def load_with_snapshot(%Instance{} = instance, run_id, lease_token) do
    with_repo(instance, fn -> load_with_snapshot_with_repo(run_id, lease_token) end)
  end

  defp load_with_snapshot_with_repo(run_id, lease_token) do
    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        snapshot = latest_snapshot(run_id)
        through_seq = if snapshot, do: snapshot.through_seq, else: -1
        {snapshot, load_events_after(run_id, through_seq)}
      end)

    case result do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres load_with_snapshot failed: #{inspect(reason)}"
    end
  end

  @impl true
  def take_snapshot!(%Instance{} = instance, %Continuum.Snapshot{} = snapshot) do
    with_repo(instance, fn -> take_snapshot_with_repo!(snapshot) end)
  end

  defp take_snapshot_with_repo!(%Continuum.Snapshot{} = snapshot) do
    repo().insert_all(
      Snapshot,
      [
        %{
          run_id: snapshot.run_id,
          through_seq: snapshot.through_seq,
          version_hash: snapshot.version_hash,
          format_version: Continuum.Snapshot.format_version(),
          payload: Continuum.Snapshot.encode(snapshot),
          taken_at: snapshot.taken_at
        }
      ],
      on_conflict: :nothing,
      conflict_target: [:run_id, :through_seq]
    )

    :ok
  end

  def schedule_activity!(%Instance{} = instance, run_id, event, task, lease_token) do
    with_repo(instance, fn -> schedule_activity_with_repo!(run_id, event, task, lease_token) end)
  end

  defp schedule_activity_with_repo!(run_id, event, task, lease_token) do
    {event_type, payload} = encode_event(event)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)
        now = DateTime.utc_now()

        event_changeset =
          %Event{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            seq: event.seq,
            event_type: event_type,
            payload: payload,
            inserted_at: now
          })

        task_changeset =
          %ActivityTask{}
          |> Ecto.Changeset.change(%{
            id: task.id,
            run_id: run_id,
            seq: task.seq,
            mfa: encode_term(task),
            attempt: 1,
            state: "available"
          })

        with {:ok, _event} <- repo().insert(event_changeset),
             {:ok, _task} <- repo().insert(task_changeset) do
          :ok
        else
          {:error, changeset} -> repo().rollback({:activity_schedule_failed, changeset})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres schedule_activity! failed: #{inspect(reason)}"
    end
  end

  @doc """
  Start a child workflow run and journal `child_started` to the parent.

  In one transaction (CAS-guarded by the parent's lease): insert the child run
  row with `parent_run_id`/`parent_command_id`/`correlation_id` set and append
  the `child_started` event to the parent's history. The child run is left
  runnable for the dispatcher to claim.
  """
  def start_child!(%Instance{} = instance, parent_run_id, child, lease_token) do
    with_repo(instance, fn -> start_child_with_repo!(parent_run_id, child, lease_token) end)
  end

  defp start_child_with_repo!(parent_run_id, child, lease_token) do
    metadata = workflow_metadata(child.workflow)
    {event_type, payload} = encode_event(child.started_event)
    now = DateTime.utc_now()

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(parent_run_id, lease_token)

        child_changeset =
          %Run{}
          |> Ecto.Changeset.change(%{
            id: child.child_run_id,
            workflow: metadata.workflow,
            version_hash: metadata.version_hash,
            state: "running",
            input: encode_term(child.input),
            parent_run_id: parent_run_id,
            parent_command_id: child.parent_command_id,
            correlation_id: child.child_run_id,
            trace_context: child.trace_context
          })

        event_changeset =
          %Event{}
          |> Ecto.Changeset.change(%{
            run_id: parent_run_id,
            seq: child.started_event.seq,
            event_type: event_type,
            payload: payload,
            inserted_at: now
          })

        with {:ok, _child} <- repo().insert(child_changeset),
             {:ok, _event} <- repo().insert(event_changeset) do
          :ok
        else
          {:error, changeset} -> repo().rollback({:start_child_failed, changeset})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres start_child! failed: #{inspect(reason)}"
    end
  end

  @doc """
  Resolve a child's terminal state into the parent's history.

  Locks the parent (CAS by lease). If the child run is terminal, appends the
  matching `child_completed`/`child_failed`/`child_cancelled` event to the
  parent and returns the decoded outcome; otherwise returns `:pending`.
  """
  def await_child_terminal!(
        %Instance{} = instance,
        parent_run_id,
        child_run_id,
        command_id,
        seq,
        lease_token
      ) do
    with_repo(instance, fn ->
      await_child_terminal_with_repo!(parent_run_id, child_run_id, command_id, seq, lease_token)
    end)
  end

  defp await_child_terminal_with_repo!(parent_run_id, child_run_id, command_id, seq, lease_token) do
    result =
      repo().transaction(fn ->
        lock_and_validate_run!(parent_run_id, lease_token)

        case child_terminal_state(child_run_id) do
          {:completed, child_result} ->
            event = %{
              type: :child_completed,
              child_run_id: child_run_id,
              result: child_result,
              command_id: command_id,
              seq: seq
            }

            {:completed, child_result, insert_event!(parent_run_id, event)}

          {:failed, error} ->
            event = %{
              type: :child_failed,
              child_run_id: child_run_id,
              error: error,
              command_id: command_id,
              seq: seq
            }

            {:failed, error, insert_event!(parent_run_id, event)}

          {:cancelled} ->
            event = %{
              type: :child_cancelled,
              child_run_id: child_run_id,
              command_id: command_id,
              seq: seq
            }

            {:cancelled, insert_event!(parent_run_id, event)}

          :pending ->
            :pending
        end
      end)

    case result do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres await_child_terminal! failed: #{inspect(reason)}"
    end
  end

  defp child_terminal_state(child_run_id) do
    # Follow a `continue_as_new` chain forward to its terminal run so a parent
    # never sees an intermediate `{:continued, _}` marker as the child result.
    terminal_id = follow_continued_chain(child_run_id)

    case repo().one(
           from(r in Run, where: r.id == ^terminal_id, select: {r.state, r.result, r.error})
         ) do
      nil ->
        :pending

      {"completed", result, _error} ->
        case decode_term(result) do
          {:continued, _next_run_id} -> :pending
          decoded -> {:completed, decoded}
        end

      {"failed", _result, error} ->
        decoded = decode_term(error)
        if decoded in [:cancelled, :parent_cancelled], do: {:cancelled}, else: {:failed, decoded}

      {_state, _result, _error} ->
        :pending
    end
  end

  @doc """
  Resolve a run id to the live tip of its `continue_as_new` chain.

  External callers hold the chain-root id; a run with no successor resolves
  to itself. Used by signal delivery, cancel, and await so operations on a
  continued run reach the current incarnation instead of the dead root.
  """
  def resolve_chain_tip(%Instance{} = instance, run_id) do
    with_repo(instance, fn -> follow_continued_chain(run_id) end)
  end

  defp follow_continued_chain(run_id) do
    sql = """
    WITH RECURSIVE chain AS (
      SELECT id, 0 AS depth FROM continuum_runs WHERE id = $1
      UNION ALL
      SELECT c.id, ch.depth + 1
      FROM continuum_runs c
      JOIN chain ch ON c.continued_from_run_id = ch.id
    )
    SELECT id::text FROM chain ORDER BY depth DESC LIMIT 1
    """

    case repo().query(sql, [Ecto.UUID.dump!(run_id)]) do
      {:ok, %{rows: [[terminal_id]]}} -> terminal_id
      _ -> run_id
    end
  end

  @doc """
  Complete the current run as `{:continued, next_run_id}` and insert the fresh
  continuation run, in one lease-CAS-guarded transaction.

  The new run carries `continued_from_run_id`, the chain's `correlation_id`
  (the chain root's id), and any `parent_run_id`/`parent_command_id` so a
  continued child stays a child.
  """
  def continue_as_new!(
        %Instance{} = instance,
        run_id,
        next_run_id,
        next_input,
        event,
        lease_token
      ) do
    with_repo(instance, fn ->
      continue_as_new_with_repo!(run_id, next_run_id, next_input, event, lease_token)
    end)
  end

  defp continue_as_new_with_repo!(run_id, next_run_id, next_input, event, lease_token) do
    {event_type, payload} = encode_event(event)
    now = DateTime.utc_now()

    result =
      repo().transaction(fn ->
        run = repo().one(from(r in Run, where: r.id == ^run_id, lock: "FOR UPDATE"))

        case run do
          nil ->
            repo().rollback({:run_not_found, run_id})

          %Run{state: state} when state not in ["running", "suspended"] ->
            repo().rollback({:run_not_active, state})

          %Run{} = run ->
            :ok = validate_lease!(run, lease_token)
            correlation = run.correlation_id || run.id

            event_changeset =
              %Event{}
              |> Ecto.Changeset.change(%{
                run_id: run_id,
                seq: event.seq,
                event_type: event_type,
                payload: payload,
                inserted_at: now
              })

            next_changeset =
              %Run{}
              |> Ecto.Changeset.change(%{
                id: next_run_id,
                workflow: run.workflow,
                version_hash: run.version_hash,
                state: "running",
                input: encode_term(next_input),
                correlation_id: correlation,
                continued_from_run_id: run_id,
                parent_run_id: run.parent_run_id,
                parent_command_id: run.parent_command_id,
                trace_context: run.trace_context
              })

            with {:ok, _event} <- repo().insert(event_changeset),
                 {:ok, _next} <- repo().insert(next_changeset),
                 :ok <-
                   cas_update_run(run_id, lease_token, %{
                     state: "completed",
                     result: encode_term({:continued, next_run_id}),
                     correlation_id: correlation,
                     completed_at: now
                   }) do
              correlation
            else
              {:error, changeset} -> repo().rollback({:continue_as_new_failed, changeset})
            end
        end
      end)

    case result do
      {:ok, correlation} ->
        correlation

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres continue_as_new! failed: #{inspect(reason)}"
    end
  end

  @doc """
  Schedule a compensation activity task.

  Reuses the activity-task append path: the `compensation_scheduled` event and
  the worker task are inserted under the run lease in one transaction. The task
  carries `kind: :compensation` and `target_activity_id` so the worker journals
  `compensation_completed`/`compensation_failed` on completion.
  """
  def schedule_compensation!(%Instance{} = instance, run_id, event, task, lease_token) do
    with_repo(instance, fn -> schedule_activity_with_repo!(run_id, event, task, lease_token) end)
  end

  def schedule_compensations!(%Instance{} = instance, run_id, scheduled, lease_token) do
    with_repo(instance, fn ->
      schedule_compensations_with_repo!(run_id, scheduled, lease_token)
    end)
  end

  defp schedule_compensations_with_repo!(run_id, scheduled, lease_token) do
    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)
        now = DateTime.utc_now()

        Enum.each(scheduled, fn %{event: event, task: task} ->
          {event_type, payload} = encode_event(event)

          event_changeset =
            %Event{}
            |> Ecto.Changeset.change(%{
              run_id: run_id,
              seq: event.seq,
              event_type: event_type,
              payload: payload,
              inserted_at: now
            })

          task_changeset =
            %ActivityTask{}
            |> Ecto.Changeset.change(%{
              id: task.id,
              run_id: run_id,
              seq: task.seq,
              mfa: encode_term(task),
              attempt: 1,
              state: "available"
            })

          with {:ok, _event} <- repo().insert(event_changeset),
               {:ok, _task} <- repo().insert(task_changeset) do
            :ok
          else
            {:error, changeset} ->
              repo().rollback({:compensation_batch_schedule_failed, changeset})
          end
        end)

        :ok
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres schedule_compensations! failed: #{inspect(reason)}"
    end
  end

  def complete_compensation_task!(%Instance{} = instance, task, result, lease_token, opts \\ []) do
    with_repo(instance, fn ->
      complete_compensation_task_with_repo!(task, result, lease_token, opts)
    end)
  end

  defp complete_compensation_task_with_repo!(task, result, lease_token, opts) do
    idempotency = Keyword.get(opts, :idempotency)

    tx_result =
      repo().transaction(fn ->
        lock_and_validate_active_run!(task.run_id, lease_token)
        lock_and_validate_activity_task!(task)

        committed_result = maybe_commit_idempotency_result(task, result, idempotency)
        event = compensation_completed_event(task, committed_result)

        with %{} <- insert_event!(task.run_id, event),
             {1, _} <-
               repo().update_all(
                 from(t in ActivityTask,
                   where:
                     t.id == ^task.id and t.run_id == ^task.run_id and t.state == "leased" and
                       t.lease_owner == ^task.lease_owner
                 ),
                 set: [state: "completed", result: encode_term(committed_result)]
               ) do
          :ok
        else
          {0, _} -> repo().rollback({:compensation_task_result_failed, :task_lease_mismatch})
        end
      end)

    case tx_result do
      {:ok, :ok} ->
        Snapshotter.maybe_snapshot(task.instance, task.run_id, lease_token)
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres compensation task result failed: #{inspect(reason)}"
    end
  end

  defp compensation_completed_event(task, result) do
    %{
      type: :compensation_completed,
      target_activity_id: task.target_activity_id,
      result: result,
      command_id: Map.get(task, :command_id),
      seq: compensation_terminal_seq(task)
    }
  end

  def fail_compensation_task!(%Instance{} = instance, task, error, lease_token) do
    with_repo(instance, fn ->
      event = %{
        type: :compensation_failed,
        target_activity_id: task.target_activity_id,
        error: error,
        attempt: task.attempt,
        command_id: Map.get(task, :command_id),
        seq: compensation_terminal_seq(task)
      }

      activity_task_result!(
        task,
        event,
        [state: "discarded", error: encode_term(error)],
        lease_token
      )
    end)
  end

  def get_activity_result(%Instance{} = instance, activity_module, idempotency_key) do
    with_repo(instance, fn -> get_activity_result_with_repo(activity_module, idempotency_key) end)
  end

  defp get_activity_result_with_repo(activity_module, idempotency_key) do
    activity_module = activity_module_key(activity_module)

    case repo().one(
           from(r in ActivityResult,
             where:
               r.activity_module == ^activity_module and r.idempotency_key == ^idempotency_key
           )
         ) do
      nil -> :miss
      %ActivityResult{} = result -> {:ok, decode_term(result.result)}
    end
  end

  def complete_activity_task!(%Instance{} = instance, task, result, lease_token, opts \\ []) do
    with_repo(instance, fn ->
      complete_activity_task_with_repo!(task, result, lease_token, opts)
    end)
  end

  defp complete_activity_task_with_repo!(task, result, lease_token, opts) do
    idempotency = Keyword.get(opts, :idempotency)

    tx_result =
      repo().transaction(fn ->
        lock_and_validate_active_run!(task.run_id, lease_token)
        lock_and_validate_activity_task!(task)

        committed_result = maybe_commit_idempotency_result(task, result, idempotency)
        event = activity_completed_event(task, committed_result)

        with %{} <- insert_event!(task.run_id, event),
             {1, _} <-
               repo().update_all(
                 from(t in ActivityTask,
                   where:
                     t.id == ^task.id and t.run_id == ^task.run_id and t.state == "leased" and
                       t.lease_owner == ^task.lease_owner
                 ),
                 set: [state: "completed", result: encode_term(committed_result)]
               ) do
          :ok
        else
          {0, _} -> repo().rollback({:activity_task_result_failed, :task_lease_mismatch})
        end
      end)

    case tx_result do
      {:ok, :ok} ->
        Snapshotter.maybe_snapshot(task.instance, task.run_id, lease_token)
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres activity task result failed: #{inspect(reason)}"
    end
  end

  defp activity_completed_event(task, result) do
    %{
      type: :activity_completed,
      mfa: task.mfa,
      payload: result,
      command_id: Map.get(task, :command_id),
      seq: task.seq + 1
    }
  end

  def fail_activity_task!(%Instance{} = instance, task, error, lease_token) do
    with_repo(instance, fn -> fail_activity_task_with_repo!(task, error, lease_token) end)
  end

  defp fail_activity_task_with_repo!(task, error, lease_token) do
    event = %{
      type: :activity_failed,
      mfa: task.mfa,
      error: error,
      attempt: task.attempt,
      command_id: Map.get(task, :command_id),
      seq: task.seq + 1
    }

    activity_task_result!(
      task,
      event,
      [state: "discarded", error: encode_term(error)],
      lease_token
    )
  end

  def retry_activity_task!(%Instance{} = instance, task, error, retry_at, lease_token) do
    with_repo(instance, fn ->
      retry_activity_task_with_repo!(task, error, retry_at, lease_token)
    end)
  end

  defp retry_activity_task_with_repo!(task, error, retry_at, lease_token) do
    result =
      repo().transaction(fn ->
        lock_and_validate_active_run!(task.run_id, lease_token)
        lock_and_validate_activity_task!(task)

        case repo().update_all(
               from(t in ActivityTask,
                 where:
                   t.id == ^task.id and t.run_id == ^task.run_id and t.state == "leased" and
                     t.lease_owner == ^task.lease_owner
               ),
               set: [
                 state: "available",
                 attempt: task.attempt + 1,
                 available_at: retry_at,
                 lease_owner: nil,
                 lease_expires_at: nil,
                 error: encode_term(error)
               ]
             ) do
          {1, _} -> :ok
          {0, _} -> repo().rollback({:activity_task_retry_failed, :task_lease_mismatch})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres activity task retry failed: #{inspect(reason)}"
    end
  end

  def cancel_run!(%Instance{} = instance, run_id, lease_token) do
    with_repo(instance, fn -> cancel_run_with_instance!(run_id, lease_token, instance) end)
  end

  defp cancel_run_with_instance!(run_id, lease_token, instance) do
    result =
      repo().transaction(fn ->
        lock_and_validate_active_run!(run_id, lease_token)

        repo().update_all(
          from(t in ActivityTask,
            where: t.run_id == ^run_id and t.state in ["available", "leased"]
          ),
          set: [
            state: "discarded",
            lease_owner: nil,
            lease_expires_at: nil,
            error: encode_term(:cancelled)
          ]
        )

        repo().update_all(
          from(t in Timer, where: t.run_id == ^run_id and t.fired == false),
          set: [fired: true]
        )

        # Cascade: cancel all in-flight descendant child runs, bounded by depth.
        cancel_descendants!(run_id)

        case repo().update_all(
               leased_run_query(run_id, lease_token),
               set: [
                 state: "failed",
                 error: encode_term(:cancelled),
                 completed_at: DateTime.utc_now(),
                 next_wakeup_at: nil
               ]
             ) do
          {1, _} -> maybe_wake_parent(run_id)
          {0, _} -> repo().rollback({:cancel_failed, :lease_mismatch})
        end
      end)

    case result do
      {:ok, parent_run_id} ->
        Continuum.Runtime.Engine.broadcast_run_finished(instance, run_id, :failed, :cancelled)
        wake_parent(instance, parent_run_id)
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres cancel_run! failed: #{inspect(reason)}"
    end
  end

  defp cancel_descendants!(run_id) do
    case descendant_run_ids(run_id) do
      [] ->
        :ok

      descendant_ids ->
        cancelled = encode_term(:parent_cancelled)
        now = DateTime.utc_now()

        repo().update_all(
          from(t in ActivityTask,
            where: t.run_id in ^descendant_ids and t.state in ["available", "leased"]
          ),
          set: [state: "discarded", lease_owner: nil, lease_expires_at: nil, error: cancelled]
        )

        repo().update_all(
          from(t in Timer, where: t.run_id in ^descendant_ids and t.fired == false),
          set: [fired: true]
        )

        # Clear the lease so any live descendant engine fails its next write and
        # stops cleanly — no post-cancel child events can be appended.
        repo().update_all(
          from(r in Run, where: r.id in ^descendant_ids and r.state in ["running", "suspended"]),
          set: [
            state: "failed",
            error: cancelled,
            completed_at: now,
            next_wakeup_at: nil,
            lease_owner: nil,
            lease_token: nil,
            lease_expires_at: nil
          ]
        )

        :ok
    end
  end

  defp descendant_run_ids(run_id) do
    max_depth = Application.get_env(:continuum, :max_child_depth, 10)

    sql = """
    WITH RECURSIVE descendants AS (
      SELECT id, 1 AS depth FROM continuum_runs WHERE parent_run_id = $1
      UNION ALL
      SELECT c.id, d.depth + 1
      FROM continuum_runs c
      JOIN descendants d ON c.parent_run_id = d.id
      WHERE d.depth < $2
    )
    SELECT id::text FROM descendants
    """

    case repo().query(sql, [Ecto.UUID.dump!(run_id), max_depth]) do
      {:ok, %{rows: rows}} -> Enum.map(rows, fn [id] -> id end)
      _ -> []
    end
  end

  defp maybe_wake_parent(run_id) do
    case repo().one(from(r in Run, where: r.id == ^run_id, select: r.parent_run_id)) do
      nil ->
        nil

      parent_run_id ->
        repo().update_all(
          from(r in Run, where: r.id == ^parent_run_id),
          set: [next_wakeup_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)]
        )

        repo().query("SELECT pg_notify('continuum_run_wake', $1)", [parent_run_id])
        parent_run_id
    end
  end

  defp wake_parent(_instance, nil), do: :ok

  defp wake_parent(instance, parent_run_id),
    do: Continuum.Runtime.Engine.wake(instance, parent_run_id)

  defp run_in_transaction!(fun) do
    case repo().transaction(fun) do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres transaction failed: #{inspect(reason)}"
    end
  end

  def schedule_timer!(%Instance{} = instance, run_id, event, timer, lease_token) do
    with_repo(instance, fn -> schedule_timer_with_repo!(run_id, event, timer, lease_token) end)
  end

  defp schedule_timer_with_repo!(run_id, event, timer, lease_token) do
    {event_type, payload} = encode_event(event)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        event_changeset =
          %Event{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            seq: event.seq,
            event_type: event_type,
            payload: payload,
            inserted_at: DateTime.utc_now()
          })

        timer_changeset =
          %Timer{}
          |> Ecto.Changeset.change(%{
            id: timer.id,
            run_id: run_id,
            fires_at: timer.fires_at,
            fired: false
          })

        with {:ok, _event} <- repo().insert(event_changeset),
             {:ok, _timer} <- repo().insert(timer_changeset),
             :ok <- notify_timer_armed_with_repo(run_id, timer.fires_at),
             {1, _} <-
               repo().update_all(
                 leased_run_query(run_id, lease_token),
                 set: [next_wakeup_at: timer.fires_at]
               ) do
          :ok
        else
          {0, _} -> repo().rollback({:timer_schedule_failed, :lease_mismatch})
          {:error, changeset} -> repo().rollback({:timer_schedule_failed, changeset})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres schedule_timer! failed: #{inspect(reason)}"
    end
  end

  @doc false
  def notify_timer_armed!(%Instance{} = instance, run_id, fires_at) do
    with_repo(instance, fn -> notify_timer_armed_with_repo(run_id, fires_at) end)
  end

  def schedule_signal_await!(%Instance{} = instance, run_id, event, lease_token) do
    with_repo(instance, fn -> schedule_signal_await_with_repo!(run_id, event, lease_token) end)
  end

  defp schedule_signal_await_with_repo!(run_id, event, lease_token) do
    {event_type, payload} = encode_event(event)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)
        now = DateTime.utc_now()

        changeset =
          %Event{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            seq: event.seq,
            event_type: event_type,
            payload: payload,
            inserted_at: now
          })

        with {:ok, _event} <- repo().insert(changeset),
             :ok <- maybe_insert_signal_timeout_timer(run_id, event),
             :ok <- maybe_set_signal_timeout_wakeup(run_id, event, lease_token) do
          :ok
        else
          {:error, changeset} -> repo().rollback({:signal_await_failed, changeset})
          {0, _} -> repo().rollback({:signal_await_failed, :lease_mismatch})
        end
      end)

    case result do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres schedule_signal_await! failed: #{inspect(reason)}"
    end
  end

  def resolve_signal_await(%Instance{} = instance, run_id, await_event, lease_token) do
    value =
      with_repo(instance, fn ->
        resolve_signal_await_with_repo(run_id, await_event, lease_token)
      end)

    maybe_snapshot_after_signal_resolution(instance, run_id, value, lease_token)
    value
  end

  @doc false
  def consume_pending_signal!(%Instance{} = instance, run_id, name, command_id, seq, lease_token) do
    value =
      with_repo(instance, fn ->
        consume_pending_signal_with_repo!(run_id, name, command_id, seq, lease_token)
      end)

    maybe_snapshot_after_signal_resolution(instance, run_id, value, lease_token)
    value
  end

  defp consume_pending_signal_with_repo!(run_id, name, command_id, seq, lease_token) do
    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        case pending_signal(run_id, name) do
          nil -> :none
          %Signal{} = signal -> consume_signal_row!(run_id, name, signal, command_id, seq)
        end
      end)

    case result do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres consume_pending_signal! failed: #{inspect(reason)}"
    end
  end

  defp resolve_signal_await_with_repo(run_id, await_event, lease_token) do
    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        case signal_await_winner(run_id, await_event) do
          :none -> consume_signal_or_timeout(run_id, await_event)
          result -> result
        end
      end)

    case result do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres resolve_signal_await failed: #{inspect(reason)}"
    end
  end

  def deliver_signal!(%Instance{} = instance, run_id, name, payload) do
    with_repo(instance, fn -> deliver_signal_with_repo!(run_id, name, payload) end)
  end

  defp deliver_signal_with_repo!(run_id, name, payload) do
    signal_name = Atom.to_string(name)
    now = DateTime.utc_now()

    result =
      repo().transaction(fn ->
        # Callers of a continued run hold the chain-root id; deliver into the
        # live tip's mailbox so the signal is not lost in the dead root's.
        run_id = follow_continued_chain(run_id)

        changeset =
          %Signal{}
          |> Ecto.Changeset.change(%{
            run_id: run_id,
            name: signal_name,
            payload: encode_term(payload),
            delivered: false,
            inserted_at: now
          })

        with {:ok, _signal} <- repo().insert(changeset),
             {_count, _} <-
               repo().update_all(
                 from(r in Run, where: r.id == ^run_id),
                 set: [next_wakeup_at: now]
               ),
             {:ok, _} <- repo().query("SELECT pg_notify('continuum_signal', $1)", [run_id]) do
          run_id
        else
          {:error, reason} -> repo().rollback({:signal_delivery_failed, reason})
        end
      end)

    case result do
      {:ok, delivered_run_id} ->
        Telemetry.execute([:continuum, :signal, :delivered], %{}, %{
          run_id: delivered_run_id,
          signal_name: name,
          durable?: true
        })

        {:ok, delivered_run_id}

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres deliver_signal! failed: #{inspect(reason)}"
    end
  end

  def consume_signal(%Instance{} = instance, run_id, name, lease_token) do
    value = with_repo(instance, fn -> consume_signal_with_repo(run_id, name, lease_token) end)
    maybe_snapshot_after_signal_resolution(instance, run_id, value, lease_token)
    value
  end

  defp consume_signal_with_repo(run_id, name, lease_token) do
    signal_name = Atom.to_string(name)

    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        signal =
          repo().one(
            from(s in Signal,
              where: s.run_id == ^run_id and s.name == ^signal_name and s.delivered == false,
              order_by: [asc: s.inserted_at, asc: s.id],
              limit: 1,
              lock: "FOR UPDATE SKIP LOCKED"
            )
          )

        case signal do
          nil ->
            :none

          %Signal{} = signal ->
            payload = decode_term(signal.payload)

            event = %{
              type: :signal_received,
              name: name,
              payload: payload,
              seq: next_seq(run_id)
            }

            {event_type, event_payload} = encode_event(event)

            with {:ok, _event} <-
                   %Event{}
                   |> Ecto.Changeset.change(%{
                     run_id: run_id,
                     seq: event.seq,
                     event_type: event_type,
                     payload: event_payload,
                     inserted_at: DateTime.utc_now()
                   })
                   |> repo().insert(),
                 {1, _} <-
                   repo().update_all(
                     from(s in Signal, where: s.id == ^signal.id),
                     set: [delivered: true]
                   ) do
              {:ok, payload}
            else
              {0, _} -> repo().rollback({:signal_consume_failed, :already_delivered})
              {:error, changeset} -> repo().rollback({:signal_consume_failed, changeset})
            end
        end
      end)

    case result do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres consume_signal failed: #{inspect(reason)}"
    end
  end

  def fire_timer!(%Instance{} = instance, run_id, timer_id, lease_token) do
    :ok = with_repo(instance, fn -> fire_timer_with_repo!(run_id, timer_id, lease_token) end)
    Snapshotter.maybe_snapshot(instance, run_id, lease_token)
  end

  defp fire_timer_with_repo!(run_id, timer_id, lease_token) do
    result =
      repo().transaction(fn ->
        lock_and_validate_run!(run_id, lease_token)

        case timer_winner(run_id, timer_id) do
          {:pending, timer_event, winner_seq} ->
            event = %{
              type: :timer_fired,
              timer_id: timer_id,
              command_id: Map.get(timer_event, :command_id),
              seq: winner_seq
            }

            winner_event = insert_event!(run_id, event)
            mark_timer_resolved(run_id, timer_id, lease_token)
            {:ok, winner_event}

          {:already_fired, winner_event} ->
            mark_timer_resolved(run_id, timer_id, lease_token)
            {:ok, winner_event}

          {:already_resolved, _winner_event} ->
            mark_timer_resolved(run_id, timer_id, lease_token)
            :already_resolved

          :not_found ->
            repo().rollback({:timer_fire_failed, :not_found})

          :mismatch ->
            repo().rollback({:timer_fire_failed, :winner_mismatch})
        end
      end)

    case result do
      {:ok, _value} ->
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres fire_timer! failed: #{inspect(reason)}"
    end
  end

  def clear_next_wakeup!(%Instance{} = instance, run_id, lease_token) do
    with_repo(instance, fn -> clear_next_wakeup_with_repo!(run_id, lease_token) end)
  end

  defp clear_next_wakeup_with_repo!(run_id, lease_token) do
    cas_update_run(run_id, lease_token, %{next_wakeup_at: nil})
  end

  defp activity_task_result!(task, event, task_updates, lease_token) do
    result =
      repo().transaction(fn ->
        lock_and_validate_active_run!(task.run_id, lease_token)
        lock_and_validate_activity_task!(task)

        with %{} <- insert_event!(task.run_id, event),
             {1, _} <-
               repo().update_all(
                 from(t in ActivityTask,
                   where:
                     t.id == ^task.id and t.run_id == ^task.run_id and t.state == "leased" and
                       t.lease_owner == ^task.lease_owner
                 ),
                 set: task_updates
               ) do
          :ok
        else
          {0, _} -> repo().rollback({:activity_task_result_failed, :task_lease_mismatch})
        end
      end)

    case result do
      {:ok, :ok} ->
        Snapshotter.maybe_snapshot(task.instance, task.run_id, lease_token)
        :ok

      {:error, reason} ->
        raise "Continuum.Runtime.Journal.Postgres activity task result failed: #{inspect(reason)}"
    end
  end

  defp maybe_commit_idempotency_result(_task, result, nil), do: result

  defp maybe_commit_idempotency_result(task, result, idempotency) do
    activity_module = activity_module_key(Keyword.fetch!(idempotency, :module))
    idempotency_key = Keyword.fetch!(idempotency, :key)
    now = DateTime.utc_now()

    {count, _} =
      repo().insert_all(
        ActivityResult,
        [
          %{
            activity_module: activity_module,
            idempotency_key: idempotency_key,
            run_id: task.run_id,
            seq: task.seq + 1,
            result: encode_term(result),
            completed_at: now
          }
        ],
        on_conflict: :nothing,
        conflict_target: [:activity_module, :idempotency_key]
      )

    case count do
      1 -> result
      0 -> fetch_activity_result!(activity_module, idempotency_key)
    end
  end

  defp fetch_activity_result!(activity_module, idempotency_key) do
    case repo().one(
           from(r in ActivityResult,
             where:
               r.activity_module == ^activity_module and r.idempotency_key == ^idempotency_key
           )
         ) do
      %ActivityResult{} = result ->
        decode_term(result.result)

      nil ->
        repo().rollback({:activity_result_conflict_failed, {activity_module, idempotency_key}})
    end
  end

  defp activity_module_key(module) when is_atom(module), do: Atom.to_string(module)
  defp activity_module_key(module) when is_binary(module), do: module

  defp lock_and_validate_active_run!(run_id, lease_token) do
    run =
      repo().one(
        from(r in Run,
          where: r.id == ^run_id,
          lock: "FOR UPDATE"
        )
      )

    case run do
      nil ->
        repo().rollback({:run_not_found, run_id})

      %Run{state: state} = run when state in ["running", "suspended"] ->
        validate_lease!(run, lease_token)

      %Run{state: state} ->
        repo().rollback({:run_not_active, state})
    end
  end

  defp lock_and_validate_activity_task!(task) do
    db_task =
      repo().one(
        from(t in ActivityTask,
          where: t.id == ^task.id,
          lock: "FOR UPDATE"
        )
      )

    cond do
      is_nil(db_task) ->
        repo().rollback({:activity_task_not_found, task.id})

      db_task.run_id != task.run_id ->
        repo().rollback({:activity_task_run_mismatch, task.id})

      db_task.state != "leased" ->
        repo().rollback({:activity_task_not_leased, db_task.state})

      db_task.lease_owner != task.lease_owner ->
        repo().rollback(
          {:activity_task_lease_mismatch, expected: task.lease_owner, actual: db_task.lease_owner}
        )

      is_nil(db_task.lease_expires_at) ->
        repo().rollback({:activity_task_lease_missing_expiry, task.id})

      DateTime.compare(db_task.lease_expires_at, DateTime.utc_now()) == :lt ->
        repo().rollback({:activity_task_lease_expired, task.id})

      true ->
        :ok
    end
  end

  defp maybe_insert_signal_timeout_timer(run_id, %{
         timeout_timer_id: timer_id,
         timeout_at: fires_at
       }) do
    changeset =
      %Timer{}
      |> Ecto.Changeset.change(%{
        id: timer_id,
        run_id: run_id,
        fires_at: fires_at,
        fired: false
      })

    case repo().insert(changeset) do
      {:ok, _timer} -> notify_timer_armed_with_repo(run_id, fires_at)
      {:error, changeset} -> {:error, changeset}
    end
  end

  defp maybe_insert_signal_timeout_timer(_run_id, _event), do: :ok

  defp maybe_set_signal_timeout_wakeup(run_id, %{timeout_at: timeout_at}, lease_token) do
    case repo().update_all(
           leased_run_query(run_id, lease_token),
           set: [next_wakeup_at: timeout_at]
         ) do
      {1, _} -> :ok
      other -> other
    end
  end

  defp maybe_set_signal_timeout_wakeup(_run_id, _event, _lease_token), do: :ok

  defp notify_timer_armed_with_repo(run_id, fires_at) do
    payload = "#{run_id}|#{DateTime.to_iso8601(fires_at)}"

    case repo().query("SELECT pg_notify('continuum_timer_armed', $1)", [payload]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp signal_await_winner(run_id, await_event) do
    winner_seq = await_event.seq + 1
    await_name = await_event.name
    timeout_timer_id = Map.get(await_event, :timeout_timer_id)

    run_id
    |> event_at(winner_seq)
    |> case do
      nil ->
        :none

      %{type: :signal_received, name: ^await_name, payload: payload} = winner_event ->
        {:ok, payload, winner_event}

      %{type: :timer_fired, timer_id: ^timeout_timer_id} = winner_event
      when not is_nil(timeout_timer_id) ->
        {:timeout, winner_event}

      _other ->
        repo().rollback({:signal_await_failed, :winner_mismatch})
    end
  end

  defp consume_signal_or_timeout(run_id, await_event) do
    case pending_signal(run_id, await_event.name) do
      nil ->
        maybe_timeout_signal_await(run_id, await_event)

      %Signal{} = signal ->
        {:ok, payload, winner_event} =
          consume_signal_row!(
            run_id,
            await_event.name,
            signal,
            Map.get(await_event, :command_id),
            await_event.seq + 1
          )

        mark_signal_timeout_resolved(run_id, await_event)
        {:ok, payload, winner_event}
    end
  end

  defp consume_signal_row!(run_id, name, %Signal{} = signal, command_id, seq) do
    payload = decode_term(signal.payload)

    winner_event =
      insert_event!(run_id, %{
        type: :signal_received,
        name: name,
        payload: payload,
        command_id: command_id,
        seq: seq
      })

    with {1, _} <-
           repo().update_all(
             from(s in Signal, where: s.id == ^signal.id and s.delivered == false),
             set: [delivered: true]
           ) do
      {:ok, payload, winner_event}
    else
      {0, _} -> repo().rollback({:signal_consume_failed, :already_delivered})
    end
  end

  defp maybe_timeout_signal_await(
         run_id,
         %{timeout_timer_id: timer_id, timeout_at: timeout_at} = event
       ) do
    if DateTime.compare(DateTime.utc_now(), timeout_at) in [:gt, :eq] do
      winner_event =
        insert_event!(run_id, %{
          type: :timer_fired,
          timer_id: timer_id,
          command_id: Map.get(event, :command_id),
          seq: event.seq + 1
        })

      mark_timer_resolved(run_id, timer_id, nil)
      {:timeout, winner_event}
    else
      :none
    end
  end

  defp maybe_timeout_signal_await(_run_id, _event), do: :none

  defp pending_signal(run_id, name) do
    signal_name = Atom.to_string(name)

    repo().one(
      from(s in Signal,
        where: s.run_id == ^run_id and s.name == ^signal_name and s.delivered == false,
        order_by: [asc: s.inserted_at, asc: s.id],
        limit: 1,
        lock: "FOR UPDATE SKIP LOCKED"
      )
    )
  end

  defp mark_signal_timeout_resolved(run_id, %{timeout_timer_id: timer_id}) do
    mark_timer_resolved(run_id, timer_id, nil)
  end

  defp mark_signal_timeout_resolved(_run_id, _event), do: :ok

  defp timer_winner(run_id, timer_id) do
    events = load_events(run_id)

    case Enum.find(events, &timer_owner?(&1, timer_id)) do
      nil ->
        :not_found

      timer_event ->
        winner_seq = timer_event.seq + 1

        case Enum.find(events, &(&1.seq == winner_seq)) do
          nil ->
            {:pending, timer_event, winner_seq}

          %{type: :timer_fired, timer_id: ^timer_id} = winner_event ->
            {:already_fired, winner_event}

          %{type: :signal_received} = winner_event when timer_event.type == :signal_awaited ->
            {:already_resolved, winner_event}

          _other ->
            :mismatch
        end
    end
  end

  defp timer_owner?(%{type: :timer_started, timer_id: event_timer_id}, timer_id)
       when event_timer_id == timer_id,
       do: true

  defp timer_owner?(%{type: :signal_awaited, timeout_timer_id: event_timer_id}, timer_id)
       when event_timer_id == timer_id,
       do: true

  defp timer_owner?(_event, _timer_id), do: false

  defp event_at(run_id, seq) do
    repo().one(
      from(e in Event,
        where: e.run_id == ^run_id and e.seq == ^seq
      )
    )
    |> case do
      nil -> nil
      event -> decode_event(event)
    end
  end

  defp load_events(run_id) do
    repo().all(
      from(e in Event,
        where: e.run_id == ^run_id,
        order_by: [asc: e.seq]
      )
    )
    |> Enum.map(&decode_event/1)
  end

  defp load_events_after(run_id, through_seq) do
    repo().all(
      from(e in Event,
        where: e.run_id == ^run_id and e.seq > ^through_seq,
        order_by: [asc: e.seq]
      )
    )
    |> Enum.map(&decode_event/1)
  end

  defp latest_snapshot(run_id) do
    repo().one(
      from(s in Snapshot,
        where: s.run_id == ^run_id,
        order_by: [desc: s.through_seq],
        limit: 1
      )
    )
    |> case do
      nil -> nil
      snapshot -> decode_snapshot(snapshot)
    end
  end

  defp insert_event!(run_id, event) do
    {event_type, payload} = encode_event(event)
    seq = event.seq || next_seq(run_id)

    changeset =
      %Event{}
      |> Ecto.Changeset.change(%{
        run_id: run_id,
        seq: seq,
        event_type: event_type,
        payload: payload,
        inserted_at: DateTime.utc_now()
      })

    case repo().insert(changeset) do
      {:ok, event_record} -> decode_event(event_record)
      {:error, changeset} -> repo().rollback({:event_insert_failed, changeset})
    end
  end

  defp maybe_snapshot_after_event(instance, run_id, event, lease_token) do
    if advancing_event?(Map.get(event, :type)) do
      Snapshotter.maybe_snapshot(instance, run_id, lease_token)
    else
      :ok
    end
  end

  defp maybe_snapshot_after_signal_resolution(_instance, _run_id, :none, _lease_token), do: :ok

  defp maybe_snapshot_after_signal_resolution(instance, run_id, _value, lease_token) do
    Snapshotter.maybe_snapshot(instance, run_id, lease_token)
  end

  defp advancing_event?(type) do
    type in [
      :side_effect,
      :activity_completed,
      :activity_failed,
      :signal_received,
      :timer_fired,
      :patched,
      :compensation_completed,
      :compensation_failed
    ]
  end

  defp mark_timer_resolved(run_id, timer_id, lease_token) do
    repo().update_all(
      from(t in Timer, where: t.run_id == ^run_id and t.id == ^timer_id),
      set: [fired: true]
    )

    run_query =
      case lease_token do
        nil -> from(r in Run, where: r.id == ^run_id)
        token -> leased_run_query(run_id, token)
      end

    repo().update_all(run_query, set: [next_wakeup_at: nil])
    :ok
  end

  @impl true
  def suspend!(%Instance{} = instance, run_id, lease_token) do
    with_repo(instance, fn -> suspend_with_repo!(run_id, lease_token) end)
  end

  defp suspend_with_repo!(run_id, lease_token) do
    cas_update_run(run_id, lease_token, %{state: "suspended"})
  end

  @impl true
  def complete!(%Instance{} = instance, run_id, result, lease_token) do
    parent_run_id =
      with_repo(instance, fn ->
        parent =
          run_in_transaction!(fn ->
            :ok =
              cas_update_run(run_id, lease_token, %{
                state: "completed",
                result: encode_term(result),
                completed_at: DateTime.utc_now()
              })

            maybe_wake_parent(run_id)
          end)

        Snapshotter.maybe_snapshot(instance, run_id, lease_token)
        Continuum.Runtime.Engine.broadcast_run_finished(instance, run_id, :completed, result)
        parent
      end)

    wake_parent(instance, parent_run_id)
    :ok
  end

  @impl true
  def fail!(%Instance{} = instance, run_id, error, lease_token) do
    parent_run_id =
      with_repo(instance, fn ->
        parent =
          run_in_transaction!(fn ->
            :ok =
              cas_update_run(run_id, lease_token, %{
                state: "failed",
                error: encode_term(error),
                completed_at: DateTime.utc_now()
              })

            maybe_wake_parent(run_id)
          end)

        Snapshotter.maybe_snapshot(instance, run_id, lease_token)
        broadcast_failed(instance, run_id, error)
        parent
      end)

    wake_parent(instance, parent_run_id)
    :ok
  end

  def mark_unknown_version!(%Instance{} = instance, run_id, error, lease_token) do
    with_repo(instance, fn ->
      :ok =
        cas_update_run(run_id, lease_token, %{
          state: "stuck_unknown_version",
          error: encode_term(error)
        })
    end)
  end

  @impl true
  def get_run(%Instance{} = instance, run_id) do
    with_repo(instance, fn -> get_run_with_repo(run_id) end)
  end

  defp get_run_with_repo(run_id) do
    case repo().one(from(r in Run, where: r.id == ^run_id)) do
      nil -> nil
      run -> decode_run(run)
    end
  end

  defp decode_snapshot(%Snapshot{payload: payload}) do
    Continuum.Snapshot.decode(payload)
  end

  defp compensation_terminal_seq(%{parallel_batch?: true}), do: nil
  defp compensation_terminal_seq(task), do: task.seq + 1

  defp cas_update_run(run_id, lease_token, updates) do
    query = leased_run_query(run_id, lease_token)

    case repo().update_all(query, set: Map.to_list(updates)) do
      {1, _} ->
        :ok

      {0, _} ->
        raise "Continuum.Runtime.Journal.Postgres CAS update failed for run #{inspect(run_id)} — lease token mismatch or run not found"
    end
  end

  defp validate_lease!(%Run{lease_token: nil, lease_owner: nil}, nil), do: :ok

  defp validate_lease!(%Run{lease_token: token}, token) when not is_nil(token), do: :ok

  defp validate_lease!(%Run{} = run, lease_token) do
    repo().rollback(
      {:lease_mismatch,
       expected: lease_token,
       actual: %{lease_owner: run.lease_owner, lease_token: run.lease_token}}
    )
  end

  defp lock_and_validate_run!(run_id, lease_token) do
    run =
      repo().one(
        from(r in Run,
          where: r.id == ^run_id,
          lock: "FOR UPDATE"
        )
      )

    case run do
      nil -> repo().rollback({:run_not_found, run_id})
      %Run{} = run -> validate_lease!(run, lease_token)
    end
  end

  defp leased_run_query(run_id, nil) do
    from(r in Run,
      where: r.id == ^run_id and is_nil(r.lease_owner) and is_nil(r.lease_token)
    )
  end

  defp leased_run_query(run_id, lease_token) do
    from(r in Run,
      where: r.id == ^run_id and r.lease_token == ^lease_token
    )
  end

  defp next_seq(run_id) do
    case repo().one(
           from(e in Event,
             where: e.run_id == ^run_id,
             select: max(e.seq)
           )
         ) do
      nil -> 0
      seq -> seq + 1
    end
  end

  defp encode_event(%{type: type} = event) do
    payload =
      event
      |> Map.delete(:type)
      |> Map.delete(:seq)
      |> encode_term()

    {Atom.to_string(type), payload}
  end

  defp decode_event(%Event{event_type: event_type, payload: payload, seq: seq}) do
    decoded = decode_term(payload)
    type = String.to_atom(event_type)

    decoded
    |> Map.put(:type, type)
    |> Map.put(:seq, seq)
    |> atomize_keys_by_type(type)
  end

  defp atomize_keys_by_type(map, :side_effect) do
    map
    |> maybe_atomize(:kind)
  end

  defp atomize_keys_by_type(map, :activity_completed) do
    map
    |> maybe_decode_mfa()
  end

  defp atomize_keys_by_type(map, :signal_received) do
    map
    |> maybe_atomize(:name)
  end

  defp atomize_keys_by_type(map, _type), do: map

  defp maybe_atomize(map, key) do
    case Map.get(map, key) || Map.get(map, to_string(key)) do
      nil -> map
      val when is_binary(val) -> Map.put(map, key, String.to_atom(val))
      _ -> map
    end
  end

  defp maybe_decode_mfa(map) do
    key = :mfa
    str_key = "mfa"

    case Map.get(map, key) || Map.get(map, str_key) do
      [mod, fun, args] when is_binary(mod) and is_binary(fun) and is_list(args) ->
        Map.put(map, key, {String.to_atom("Elixir." <> mod), String.to_atom(fun), args})

      [mod, fun, args] when is_atom(mod) and is_atom(fun) and is_list(args) ->
        Map.put(map, key, {mod, fun, args})

      _ ->
        map
    end
  end

  defp decode_run(%Run{} = run) do
    %{
      run_id: run.id,
      workflow: run.workflow,
      state: String.to_atom(run.state),
      result: decode_term(run.result),
      error: decode_term(run.error),
      input: decode_term(run.input),
      attributes: run.attributes || %{},
      namespace: run.namespace || "default",
      version_hash: run.version_hash,
      trace_context: run.trace_context
    }
  end

  defp encode_term(nil), do: nil
  defp encode_term(term), do: :erlang.term_to_binary(term)

  defp decode_term(nil), do: nil
  defp decode_term(binary) when is_binary(binary), do: :erlang.binary_to_term(binary)
  defp decode_term(other), do: other

  defp broadcast_failed(_instance, _run_id, {_kind, _reason, stacktrace})
       when is_list(stacktrace),
       do: :ok

  defp broadcast_failed(instance, run_id, error) do
    Continuum.Runtime.Engine.broadcast_run_finished(instance, run_id, :failed, error)
  end

  defp with_repo(%Instance{} = instance, fun) do
    previous = Process.get(:continuum_repo)
    Process.put(:continuum_repo, instance.repo)

    try do
      fun.()
    after
      if is_nil(previous),
        do: Process.delete(:continuum_repo),
        else: Process.put(:continuum_repo, previous)
    end
  end

  defp repo do
    Process.get(:continuum_repo) || Application.fetch_env!(:continuum, :repo)
  end
end
