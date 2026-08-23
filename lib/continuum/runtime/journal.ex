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

  @callback start_run(
              instance :: Continuum.Runtime.Instance.t(),
              run_id :: binary(),
              workflow :: module(),
              input :: term(),
              opts :: keyword()
            ) :: :ok | {:ok, Continuum.Runtime.Lease.t()} | {:error, term()}

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

  @callback deliver_signal!(
              instance :: Continuum.Runtime.Instance.t(),
              run_id :: binary(),
              name :: atom(),
              payload :: term()
            ) :: :ok | {:ok, binary()} | {:error, term()}

  @callback deliver_signal!(
              instance :: Continuum.Runtime.Instance.t(),
              run_id :: binary(),
              name :: atom(),
              payload :: term(),
              opts :: keyword()
            ) :: {:ok, binary(), :delivered | :duplicate} | {:error, term()}

  @doc """
  Start a child run and journal `child.started_event` on the parent.

  Optional: a journal that does not export it makes `await child` and
  `start_child/3` unavailable, and the effect raises with that explanation
  rather than silently doing nothing.

  `child` carries `:child_run_id`, `:workflow`, `:input`, `:parent_command_id`,
  `:trace_context`, and `:started_event`.
  """
  @doc since: "0.8.0"
  @callback start_child!(
              instance :: Continuum.Runtime.Instance.t(),
              parent_run_id :: binary(),
              child :: map(),
              lease_token :: integer() | nil
            ) :: :ok

  @doc """
  Journal the parent's view of a child's terminal state, or report `:pending`.

  Returns the winner event alongside the outcome so the parent can advance its
  cursor over exactly the event that was appended.
  """
  @doc since: "0.8.0"
  @callback await_child_terminal!(
              instance :: Continuum.Runtime.Instance.t(),
              parent_run_id :: binary(),
              child_run_id :: binary(),
              command_id :: term(),
              seq :: non_neg_integer(),
              lease_token :: integer() | nil
            ) ::
              {:completed, term(), map()}
              | {:failed, term(), map()}
              | {:cancelled, map()}
              | :pending

  @doc """
  Look up the run record. Returns `nil` if no such run, or a map with at
  least `:state`, `:result`, `:error` keys (atoms / terms — already decoded).
  """
  @callback get_run(instance :: Continuum.Runtime.Instance.t(), run_id :: binary()) :: nil | map()

  @optional_callbacks start_run: 5,
                      deliver_signal!: 4,
                      deliver_signal!: 5,
                      start_child!: 4,
                      await_child_terminal!: 6

  @doc "Returns the configured default journal adapter."
  def default do
    Application.get_env(:continuum, :journal, Continuum.Runtime.Journal.InMemory)
  end
end
