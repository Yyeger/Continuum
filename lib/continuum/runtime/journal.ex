defmodule Continuum.Runtime.Journal do
  @moduledoc """
  Behaviour for the event-history journal.

  Two adapters ship with v0.1:

    * `Continuum.Runtime.Journal.InMemory` — process-level state for tests and
      single-node hello-world. No durability.
    * `Continuum.Runtime.Journal.Postgres` — Ecto-backed durable journal
      (skeleton; full implementation lands with the schema migration).

  All append operations carry a `lease_token` (or `nil` for the in-memory
  adapter). The Postgres adapter rejects appends whose lease token does not
  match the current owner of the run row — the **fencing token** described
  in the plan.
  """

  @callback start_run(run_id :: binary(), workflow :: module(), input :: term()) ::
              :ok | {:error, term()}

  @callback append!(run_id :: binary(), event :: map(), lease_token :: integer() | nil) :: :ok

  @callback load(run_id :: binary()) :: [map()]

  @callback complete!(run_id :: binary(), result :: term(), lease_token :: integer() | nil) ::
              :ok

  @callback fail!(run_id :: binary(), error :: term(), lease_token :: integer() | nil) :: :ok
end
