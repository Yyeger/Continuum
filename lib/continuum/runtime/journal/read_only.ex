defmodule Continuum.Runtime.Journal.ReadOnly do
  @moduledoc """
  A journal adapter that refuses every callback.

  This is the guard that makes offline replay structurally read-only rather
  than read-only by convention. `Continuum.Replay` builds a
  `Continuum.Runtime.Context` whose history is loaded up front and whose
  `:journal` is this module, so a replay has nothing to write *through* — and
  `Continuum.Runtime.Effect` refuses to compute a live tail against it, so it
  has nothing to write *about*.

  Both stock journals mutate on paths the code calls replay, which is why a
  flag would not be enough:

    * the in-memory adapter executes a real activity body inline when the
      workflow steps past the journaled tail, so pointing a replay at a
      production history could charge a card from an operator's laptop;
    * the Postgres adapter resolves a tail `signal_awaited` by opening a
      transaction and **consuming a pending signal row**.

  `Effect` discriminates both of those by journal module identity, so a
  distinct module — not `Postgres` with a flag set — is what closes them.

  Every callback raises `Continuum.ReadOnlyJournalError`, reads included: a
  read-only context is handed its history and snapshot before the workflow
  starts, so a call that reaches for the journal at all has taken a path that
  assumed a live run.
  """
  @moduledoc since: "0.8.0"

  @behaviour Continuum.Runtime.Journal

  alias Continuum.ReadOnlyJournalError

  @impl true
  def start_run(_instance, run_id, _workflow, _input), do: refuse!(:start_run, run_id)

  @impl true
  def start_run(_instance, run_id, _workflow, _input, _opts), do: refuse!(:start_run, run_id)

  @impl true
  def append!(_instance, run_id, _event, _lease_token), do: refuse!(:append!, run_id)

  @impl true
  def load(_instance, run_id), do: refuse!(:load, run_id)

  @impl true
  def load_with_snapshot(_instance, run_id, _lease_token),
    do: refuse!(:load_with_snapshot, run_id)

  @impl true
  def take_snapshot!(_instance, snapshot), do: refuse!(:take_snapshot!, snapshot.run_id)

  @impl true
  def suspend!(_instance, run_id, _lease_token), do: refuse!(:suspend!, run_id)

  @impl true
  def complete!(_instance, run_id, _result, _lease_token), do: refuse!(:complete!, run_id)

  @impl true
  def fail!(_instance, run_id, _error, _lease_token), do: refuse!(:fail!, run_id)

  @impl true
  def deliver_signal!(_instance, run_id, _name, _payload), do: refuse!(:deliver_signal!, run_id)

  @impl true
  def deliver_signal!(_instance, run_id, _name, _payload, _opts),
    do: refuse!(:deliver_signal!, run_id)

  @impl true
  def get_run(_instance, run_id), do: refuse!(:get_run, run_id)

  defp refuse!(operation, run_id) do
    raise ReadOnlyJournalError, operation: operation, run_id: run_id
  end
end
