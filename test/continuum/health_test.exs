defmodule Continuum.HealthTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Schema.{ActivityTask, Event, HealthReview, Run, Signal, Timer, WorkflowVersion}

  defmodule RegistrationFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: input
  end

  defmodule UnrelatedFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: input
  end

  setup do
    Repo.delete_all(HealthReview)
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Timer)
    Repo.delete_all(Signal)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "reports every F1 operational health category" do
    past = database_timestamp(-120)
    future = database_timestamp(120)

    live_run =
      insert_run!(
        lease_owner: "live",
        lease_token: 101,
        lease_expires_at: future,
        next_wakeup_at: past
      )

    expired_run = insert_run!(lease_owner: "dead", lease_token: 102, lease_expires_at: past)

    Repo.update_all(from(r in Run, where: r.id in ^[live_run, expired_run]),
      set: [lease_acquired_at: timestamp(-240), lease_heartbeat_at: timestamp(-120)]
    )

    Repo.insert!(%Event{
      run_id: live_run,
      seq: 0,
      event_type: "signal_awaited",
      payload: :erlang.term_to_binary(%{}),
      inserted_at: past
    })

    Repo.insert!(%Timer{id: Ecto.UUID.generate(), run_id: live_run, fires_at: past, fired: false})
    insert_task!(live_run, state: "available", attempt: 2, scheduled_at: past)

    expired_task =
      insert_task!(live_run,
        state: "leased",
        attempt: 3,
        lease_owner: "dead-worker",
        lease_expires_at: past,
        scheduled_at: past
      )

    dead_letter =
      insert_task!(live_run,
        state: "discarded",
        attempt: 4,
        error: :erlang.term_to_binary(:exhausted),
        scheduled_at: past
      )

    Repo.insert!(%Signal{
      run_id: live_run,
      name: "approve",
      payload: :erlang.term_to_binary(%{}),
      delivered: false,
      inserted_at: past
    })

    assert {:ok, report} =
             Continuum.Health.report(repo: Repo, partition_months: 1, lost_wake_after_ms: 0)

    assert report.status == :degraded
    assert %{instance: Continuum, state: :ready, ready?: true} = report.runtime
    assert is_list(report.partitions.present)
    assert report.workflow_versions.loaded_count >= 0
    assert Enum.any?(report.runs.groups, &(&1.reason == "signal"))
    assert Enum.any?(report.runs.lost_wake_candidates, &(&1.run_id == live_run))
    assert Enum.any?(report.timers.overdue, &(&1.run_id == live_run))
    assert Enum.any?(report.leases.entries, &(&1.run_id == expired_run and &1.expired))
    assert report.activities.pending_count == 1
    assert report.activities.leased_count == 1
    assert Enum.any?(report.activities.expired_leases, &(&1.task_id == expired_task))
    assert Enum.any?(report.activities.dead_letter_candidates, &(&1.task_id == dead_letter))

    assert report.activities.retry_distribution == [
             %{attempt: 2, count: 1},
             %{attempt: 3, count: 1}
           ]

    assert report.signals.pending_count == 1
    assert report.signals.catch_up_lag_ms >= 120_000
  end

  test "repairs are dry-run by default, fenced, executable, and idempotent" do
    past = timestamp(-120)
    future = timestamp(120)
    run_id = insert_run!(lease_owner: "owner", lease_token: 201, lease_expires_at: future)

    assert {:ok, %{status: :planned}} =
             Continuum.Health.repair(:wake, run_id, repo: Repo, lease_token: 201)

    assert Repo.get!(Run, run_id).next_wakeup_at == nil

    assert {:error, :stale_or_inactive} =
             Continuum.Health.repair(:wake, run_id,
               repo: Repo,
               lease_token: 999,
               execute: true
             )

    assert {:ok, %{status: :executed}} =
             Continuum.Health.repair(:wake, run_id,
               repo: Repo,
               lease_token: 201,
               execute: true
             )

    assert %DateTime{} = Repo.get!(Run, run_id).next_wakeup_at

    expired_run =
      insert_run!(lease_owner: "expired-owner", lease_token: 202, lease_expires_at: past)

    release_opts = [repo: Repo, lease_owner: "expired-owner", lease_token: 202]

    assert {:ok, %{status: :planned}} =
             Continuum.Health.repair(:release_expired_lease, expired_run, release_opts)

    assert {:ok, %{status: :executed}} =
             Continuum.Health.repair(
               :release_expired_lease,
               expired_run,
               Keyword.put(release_opts, :execute, true)
             )

    assert {:ok, %{status: :already_applied}} =
             Continuum.Health.repair(
               :release_expired_lease,
               expired_run,
               Keyword.put(release_opts, :execute, true)
             )

    task_id =
      insert_task!(run_id,
        state: "leased",
        attempt: 3,
        lease_owner: "worker",
        lease_expires_at: past
      )

    requeue_opts = [repo: Repo, lease_owner: "worker", attempt: 3]

    assert {:ok, %{status: :planned}} =
             Continuum.Health.repair(:requeue_activity, task_id, requeue_opts)

    assert {:ok, %{status: :executed}} =
             Continuum.Health.repair(
               :requeue_activity,
               task_id,
               Keyword.put(requeue_opts, :execute, true)
             )

    assert %ActivityTask{state: "available", attempt: 4, lease_owner: nil} =
             Repo.get!(ActivityTask, task_id)

    assert {:ok, %{status: :already_applied}} =
             Continuum.Health.repair(
               :requeue_activity,
               task_id,
               Keyword.put(requeue_opts, :execute, true)
             )
  end

  test "review fingerprints acknowledge only the observed finding" do
    subject_id = Ecto.UUID.generate()
    fingerprint = String.duplicate("a", 64)

    opts = [
      repo: Repo,
      finding_type: "dead_letter_activity",
      fingerprint: fingerprint,
      reviewed_by: "ops"
    ]

    assert {:ok, %{status: :planned}} =
             Continuum.Health.repair(:mark_reviewed, subject_id, opts)

    refute Repo.exists?(HealthReview)

    assert {:ok, %{status: :executed}} =
             Continuum.Health.repair(
               :mark_reviewed,
               subject_id,
               Keyword.put(opts, :execute, true)
             )

    assert %HealthReview{fingerprint: ^fingerprint, reviewed_by: "ops"} =
             Repo.one!(HealthReview)
  end

  test "reviewed findings do not hide later actionable candidates" do
    run_id = insert_run!([])

    first =
      insert_task!(run_id,
        state: "discarded",
        error: :erlang.term_to_binary(:first_failure),
        scheduled_at: timestamp(-120)
      )

    second =
      insert_task!(run_id,
        state: "discarded",
        error: :erlang.term_to_binary(:second_failure),
        scheduled_at: timestamp(-60)
      )

    assert {:ok, initial} = Continuum.Health.report(repo: Repo, partition_months: 1, limit: 1)
    assert [%{task_id: ^first} = finding] = initial.activities.dead_letter_candidates

    assert {:ok, %{status: :executed}} =
             Continuum.Health.repair(:mark_reviewed, finding.subject_id,
               repo: Repo,
               finding_type: finding.finding_type,
               fingerprint: finding.fingerprint,
               reviewed_by: "ops",
               execute: true
             )

    assert {:ok, report} = Continuum.Health.report(repo: Repo, partition_months: 1, limit: 1)
    assert [%{task_id: ^second, reviewed: false}] = report.activities.dead_letter_candidates
    assert report.activities.dead_letter_count == 2
  end

  test "cancelled tasks do not consume the dead-letter candidate limit" do
    run_id = insert_run!([])

    insert_task!(run_id,
      state: "discarded",
      error: :erlang.term_to_binary(:cancelled),
      scheduled_at: timestamp(-120)
    )

    actionable =
      insert_task!(run_id,
        state: "discarded",
        error: :erlang.term_to_binary(:exhausted),
        scheduled_at: timestamp(-60)
      )

    assert {:ok, report} = Continuum.Health.report(repo: Repo, partition_months: 1, limit: 1)
    assert report.activities.dead_letter_count == 1
    assert [%{task_id: ^actionable}] = report.activities.dead_letter_candidates
  end

  test "a degraded runtime makes the aggregate health report degraded" do
    heartbeater = Process.whereis(Continuum.Runtime.Lease.Heartbeater)
    original_lifecycle = :sys.get_state(heartbeater).lifecycle

    on_exit(fn ->
      if Process.alive?(heartbeater) do
        :sys.replace_state(heartbeater, &%{&1 | lifecycle: original_lifecycle})
      end
    end)

    :sys.replace_state(heartbeater, &%{&1 | lifecycle: :degraded})

    assert {:ok, report} = Continuum.Health.report(repo: Repo, partition_months: 1)
    assert report.runtime.state == :degraded
    assert report.status == :degraded
  end

  test "retry repair idempotently restores a loaded durable workflow registration" do
    {:ok, entry} = Continuum.VersionRegistry.ensure_registered(RegistrationFlow)

    Repo.delete_all(
      from(v in WorkflowVersion,
        where: v.workflow == ^entry.workflow_string and v.version_hash == ^entry.version_hash
      )
    )

    assert {:ok, %{status: :planned, versions: 1}} =
             Continuum.Health.repair(:retry, entry.workflow_string, repo: Repo)

    refute Repo.exists?(
             from(v in WorkflowVersion,
               where:
                 v.workflow == ^entry.workflow_string and v.version_hash == ^entry.version_hash
             )
           )

    assert {:ok, %{status: :executed}} =
             Continuum.Health.repair(:retry, entry.workflow_string, repo: Repo, execute: true)

    assert Repo.exists?(
             from(v in WorkflowVersion,
               where:
                 v.workflow == ^entry.workflow_string and v.version_hash == ^entry.version_hash
             )
           )

    assert {:ok, %{status: :executed}} =
             Continuum.Health.repair(:retry, entry.workflow_string, repo: Repo, execute: true)
  end

  test "workflow version health is scoped to the target instance" do
    {:ok, configured} = Continuum.VersionRegistry.ensure_registered(RegistrationFlow)
    {:ok, unrelated} = Continuum.VersionRegistry.ensure_registered(UnrelatedFlow)

    now = timestamp(0)

    Repo.insert_all(
      WorkflowVersion,
      [
        %{
          workflow: configured.workflow_string,
          version_hash: configured.version_hash,
          entrypoint: inspect(configured.entrypoint),
          registered_at: now
        },
        %{
          workflow: unrelated.workflow_string,
          version_hash: unrelated.version_hash,
          entrypoint: inspect(unrelated.entrypoint),
          registered_at: now
        }
      ],
      on_conflict: :nothing,
      conflict_target: [:workflow, :version_hash]
    )

    instance =
      Continuum.Runtime.Instance.new(
        name: :health_scoped,
        repo: Repo,
        workflow_modules: [RegistrationFlow]
      )

    assert {:ok, report} =
             Continuum.Health.report(repo: Repo, instance: instance, partition_months: 1)

    assert report.workflow_versions.loaded_count == 1
    assert report.workflow_versions.durable_count == 1
    assert report.workflow_versions.missing_in_database == []
    assert report.workflow_versions.registered_without_loaded_code == []
  end

  defp insert_run!(opts) do
    now = timestamp(0)
    id = Ecto.UUID.generate()

    defaults = %{
      id: id,
      workflow: "HealthFlow",
      version_hash: <<1>>,
      state: "suspended",
      input: :erlang.term_to_binary(%{}),
      correlation_id: id,
      started_at: Keyword.get(opts, :started_at, timestamp(-300))
    }

    attrs =
      opts
      |> Enum.into(%{})
      |> Map.put_new(:lease_acquired_at, now)
      |> Map.put_new(:lease_heartbeat_at, now)

    Repo.insert!(struct!(Run, Map.merge(defaults, attrs)))
    id
  end

  defp insert_task!(run_id, opts) do
    id = Ecto.UUID.generate()

    defaults = %{
      id: id,
      run_id: run_id,
      seq: System.unique_integer([:positive]),
      mfa: :erlang.term_to_binary(%{}),
      attempt: 1,
      state: "available",
      scheduled_at: timestamp(-30),
      available_at: timestamp(-30)
    }

    Repo.insert!(struct!(ActivityTask, Map.merge(defaults, Enum.into(opts, %{}))))
    id
  end

  defp timestamp(offset_seconds) do
    DateTime.utc_now()
    |> DateTime.add(offset_seconds, :second)
    |> DateTime.truncate(:microsecond)
  end

  defp database_timestamp(offset_seconds) do
    %{rows: [[timestamp]]} =
      Repo.query!(
        "SELECT clock_timestamp() + ($1::integer * interval '1 second')",
        [offset_seconds]
      )

    timestamp
  end
end
