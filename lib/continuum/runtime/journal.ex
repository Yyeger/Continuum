defmodule Continuum.Runtime.Journal do
  @moduledoc """
  Behaviour for the event-history journal.

  Two adapters ship with v0.1:

    * `Continuum.Runtime.Journal.InMemory` — process-level state for tests and
      single-node hello-world. No durability.
    * `Continuum.Runtime.Journal.Postgres` — Ecto-backed durable journal with
      transactional appends and lease-token fencing.

  All append operations carry a `lease_token` (or `nil` for unleased
  in-memory / pre-dispatch execution). The Postgres adapter rejects writes
  whose token does not match the run row's fencing token; `nil` only writes
  to unleased rows.
  """

  @callback start_run(
              instance :: Continuum.Runtime.Instance.t(),
              run_id :: binary(),
              workflow :: module(),
              input :: term()
            ) :: :ok | {:error, term()}

  @callback append!(
              instance :: Continuum.Runtime.Instance.t(),
              run_id :: binary(),
              event :: map(),
              lease_token :: integer() | nil
            ) :: :ok

  @callback load(instance :: Continuum.Runtime.Instance.t(), run_id :: binary()) :: [map()]

  @callback load_with_snapshot(
              instance :: Continuum.Runtime.Instance.t(),
              run_id :: binary(),
              lease_token :: integer() | nil
            ) :: {Continuum.Snapshot.t() | nil, [map()]}

  @callback take_snapshot!(
              instance :: Continuum.Runtime.Instance.t(),
              snapshot :: Continuum.Snapshot.t()
            ) :: :ok

  @callback suspend!(
              instance :: Continuum.Runtime.Instance.t(),
              run_id :: binary(),
              lease_token :: integer() | nil
            ) :: :ok

  @callback complete!(
              instance :: Continuum.Runtime.Instance.t(),
              run_id :: binary(),
              result :: term(),
              lease_token :: integer() | nil
            ) :: :ok

  @callback fail!(
              instance :: Continuum.Runtime.Instance.t(),
              run_id :: binary(),
              error :: term(),
              lease_token :: integer() | nil
            ) :: :ok

  @doc """
  Look up the run record. Returns `nil` if no such run, or a map with at
  least `:state`, `:result`, `:error` keys (atoms / terms — already decoded).
  """
  @callback get_run(instance :: Continuum.Runtime.Instance.t(), run_id :: binary()) :: nil | map()

  @doc "Returns the configured default journal adapter."
  def default do
    Application.get_env(:continuum, :journal, Continuum.Runtime.Journal.InMemory)
  end
end
