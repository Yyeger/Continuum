defmodule Continuum.Runtime.Journal.SQLite do
  @moduledoc """
  Durable journal adapter backed by SQLite via `ecto_sqlite3`.

  This adapter has the same history, fencing, and replay semantics as
  `Continuum.Runtime.Journal.Postgres`; both are served by one Ecto
  implementation that selects SQL by the repo's adapter. Every append still
  validates the run's lease token in the same transaction that writes the
  event. SQLite transactions take the database writer lock before their first
  read (`BEGIN IMMEDIATE`), which gives the read-validate-write sequence the
  isolation that row locks provide on PostgreSQL.

  SQLite is a single-node backend. Wakeups that PostgreSQL delivers with
  `LISTEN`/`NOTIFY` are delivered to local processes after commit, and the
  dispatcher, timer refresh, and signal catch-up polls remain the backstop.
  Clustering, event partitions, and PostgreSQL upgrade migrations are not
  available. See the SQLite guide for setup and limits.

  Requires the optional `:ecto_sqlite3` dependency and a repo that uses
  `Ecto.Adapters.SQLite3`.
  """
  @moduledoc since: "0.9.0"

  @behaviour Continuum.Runtime.Journal

  alias Continuum.Runtime.Journal.Postgres

  @impl true
  defdelegate start_run(instance, run_id, workflow, input), to: Postgres

  @impl true
  defdelegate start_run(instance, run_id, workflow, input, opts), to: Postgres

  @doc false
  defdelegate enqueue_run(instance, run_id, workflow, input, opts), to: Postgres

  @impl true
  defdelegate append!(instance, run_id, event, lease_token), to: Postgres

  @impl true
  defdelegate load(instance, run_id), to: Postgres

  @impl true
  defdelegate load_with_snapshot(instance, run_id, lease_token), to: Postgres

  defdelegate load_for_replay(instance, run_id), to: Postgres

  @impl true
  defdelegate take_snapshot!(instance, snapshot), to: Postgres

  defdelegate schedule_activity!(instance, run_id, event, task, lease_token), to: Postgres

  @impl true
  defdelegate start_child!(instance, parent_run_id, child, lease_token), to: Postgres

  @impl true
  defdelegate await_child_terminal!(
                instance,
                parent_run_id,
                child_run_id,
                command_id,
                seq,
                lease_token
              ),
              to: Postgres

  defdelegate resolve_chain_tip(instance, run_id), to: Postgres

  defdelegate continue_as_new!(instance, run_id, next_run_id, next_input, event, lease_token),
    to: Postgres

  defdelegate schedule_compensation!(instance, run_id, event, task, lease_token), to: Postgres

  defdelegate schedule_compensations!(instance, run_id, scheduled, lease_token), to: Postgres

  defdelegate schedule_activity_batch!(instance, run_id, scheduled, lease_token), to: Postgres

  defdelegate complete_compensation_task!(instance, task, result, lease_token, opts \\ []),
    to: Postgres

  defdelegate fail_compensation_task!(instance, task, error, lease_token), to: Postgres

  defdelegate get_activity_result(instance, activity_module, idempotency_key), to: Postgres

  defdelegate complete_activity_task!(instance, task, result, lease_token, opts \\ []),
    to: Postgres

  defdelegate fail_activity_task!(instance, task, error, lease_token), to: Postgres

  defdelegate retry_activity_task!(instance, task, error, backoff_ms, lease_token), to: Postgres

  @doc false
  defdelegate retry_discarded_activity!(instance, task_id, policy, operator, reason),
    to: Postgres

  defdelegate cancel_run!(instance, run_id, lease_token), to: Postgres

  defdelegate schedule_timer!(instance, run_id, event, timer, lease_token), to: Postgres

  @doc false
  defdelegate notify_timer_armed!(instance, run_id, fires_at), to: Postgres

  defdelegate schedule_signal_await!(instance, run_id, event, lease_token), to: Postgres

  defdelegate resolve_signal_await(instance, run_id, await_event, lease_token), to: Postgres

  @doc false
  defdelegate consume_pending_signal!(instance, run_id, name, command_id, seq, lease_token),
    to: Postgres

  @impl true
  defdelegate deliver_signal!(instance, run_id, name, payload), to: Postgres

  @impl true
  defdelegate deliver_signal!(instance, run_id, name, payload, opts), to: Postgres

  defdelegate fire_timer!(instance, run_id, timer_id, lease_token), to: Postgres

  defdelegate clear_next_wakeup!(instance, run_id, lease_token), to: Postgres

  @impl true
  defdelegate suspend!(instance, run_id, lease_token), to: Postgres

  @impl true
  defdelegate complete!(instance, run_id, result, lease_token), to: Postgres

  @impl true
  defdelegate fail!(instance, run_id, error, lease_token), to: Postgres

  defdelegate release_unknown_version!(instance, run_id, lease_token), to: Postgres

  @impl true
  defdelegate get_run(instance, run_id), to: Postgres

  @doc false
  defdelegate decode_event(event), to: Postgres
end
