defmodule Continuum.Runtime.Journal do
  @moduledoc """
  Behaviour for the event-history journal.

  Two adapters ship with v0.1:

    * `Continuum.Runtime.Journal.InMemory` — process-level state for tests and
      single-node hello-world. No durability.
    * `Continuum.Runtime.Journal.Postgres` — Ecto-backed durable journal
      (skeleton; full implementation lands with the schema migration).

  All append operations carry a `lease_token` (or `nil` for unleased
  in-memory / pre-dispatch execution). The Postgres adapter rejects writes
  whose token does not match the run row's fencing token; `nil` only writes
  to unleased rows.
  """

  @callback start_run(run_id :: binary(), workflow :: module(), input :: term()) ::
              :ok | {:error, term()}

  @callback append!(run_id :: binary(), event :: map(), lease_token :: integer() | nil) :: :ok

  @callback load(run_id :: binary()) :: [map()]

  @callback complete!(run_id :: binary(), result :: term(), lease_token :: integer() | nil) ::
              :ok

  @callback fail!(run_id :: binary(), error :: term(), lease_token :: integer() | nil) :: :ok

  @doc """
  Look up the run record. Returns `nil` if no such run, or a map with at
  least `:state`, `:result`, `:error` keys (atoms / terms — already decoded).
  """
  @callback get_run(run_id :: binary()) :: nil | map()

  @doc "Returns the configured default journal adapter."
  def default do
    Application.get_env(:continuum, :journal, Continuum.Runtime.Journal.InMemory)
  end
end
