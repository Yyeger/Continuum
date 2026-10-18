defmodule Continuum.Runtime.SQL do
  @moduledoc false

  # Dialect seam for the durable runtime. PostgreSQL remains the reference
  # dialect; SQLite is supported for single-node deployments through
  # `ecto_sqlite3`. Everything that differs between the two lives here so the
  # journal, dispatchers, and lease code keep one control flow:
  #
  #   * SQLite has no row locks. A transaction that reads, validates, and then
  #     writes must take SQLite's single write lock up front (`BEGIN IMMEDIATE`),
  #     which serializes it against every other writer exactly as the
  #     PostgreSQL `FOR UPDATE` lock serializes it against the rows it touches.
  #   * SQLite has no clock functions with PostgreSQL's semantics. The database
  #     and the application share one host, so "database time" is the BEAM's
  #     UTC clock, encoded the way `ecto_sqlite3` stores `utc_datetime_usec`.
  #   * SQLite has no sequences. Lease tokens come from a one-row counter that
  #     is advanced inside the writer's transaction.
  #   * SQLite has no LISTEN/NOTIFY. Local wakeups are delivered after commit.

  require Ecto.Query

  @sqlite_adapter Ecto.Adapters.SQLite3
  @after_commit_key {__MODULE__, :after_commit}

  @type dialect :: :postgres | :sqlite

  @doc false
  @spec dialect(module() | nil) :: dialect()
  def dialect(nil), do: :postgres

  def dialect(repo) when is_atom(repo) do
    if repo.__adapter__() == @sqlite_adapter, do: :sqlite, else: :postgres
  end

  @doc false
  @spec sqlite?(module() | nil) :: boolean()
  def sqlite?(repo), do: dialect(repo) == :sqlite

  @doc false
  @spec postgres?(module() | nil) :: boolean()
  def postgres?(repo), do: dialect(repo) == :postgres

  @doc """
  Run `fun` in a transaction that is safe for read-validate-write sequences.

  PostgreSQL transactions rely on explicit row locks. SQLite transactions start
  as `BEGIN IMMEDIATE`, so the writer lock is held before the first read.
  Callbacks registered with `after_commit/1` run once the outermost
  transaction commits and are dropped when it rolls back.
  """
  @spec transaction(module(), (-> term()), keyword()) :: {:ok, term()} | {:error, term()}
  def transaction(repo, fun, opts \\ []) when is_function(fun, 0) do
    if repo.in_transaction?() do
      repo.transaction(fun, opts)
    else
      outer_transaction(repo, fun, opts)
    end
  end

  defp outer_transaction(repo, fun, opts) do
    previous = Process.put(@after_commit_key, [])

    try do
      result = repo.transaction(fun, transaction_opts(repo, opts))
      callbacks = Process.get(@after_commit_key, [])
      restore_after_commit(previous)

      case result do
        {:ok, _value} -> Enum.each(Enum.reverse(callbacks), & &1.())
        {:error, _reason} -> :ok
      end

      result
    rescue
      exception ->
        restore_after_commit(previous)
        reraise exception, __STACKTRACE__
    catch
      kind, reason ->
        restore_after_commit(previous)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp transaction_opts(repo, opts) do
    if sqlite?(repo), do: Keyword.put_new(opts, :mode, :immediate), else: opts
  end

  defp restore_after_commit(nil), do: Process.delete(@after_commit_key)
  defp restore_after_commit(previous), do: Process.put(@after_commit_key, previous)

  @doc """
  Run `fun` after the enclosing `transaction/3` commits.

  Outside a `transaction/3`, `fun` runs immediately. Use this only for
  latency hints, such as local wakeups: durable evidence must already be in
  the committed rows.
  """
  @spec after_commit((-> term())) :: :ok
  def after_commit(fun) when is_function(fun, 0) do
    case Process.get(@after_commit_key) do
      nil ->
        fun.()
        :ok

      callbacks ->
        Process.put(@after_commit_key, [fun | callbacks])
        :ok
    end
  end

  @doc """
  Apply `FOR UPDATE` on PostgreSQL. SQLite serializes writers instead.
  """
  @spec for_update(Ecto.Queryable.t(), module()) :: Ecto.Query.t()
  def for_update(query, repo) do
    query = Ecto.Queryable.to_query(query)
    if sqlite?(repo), do: query, else: Ecto.Query.lock(query, "FOR UPDATE")
  end

  @doc """
  Apply `FOR UPDATE SKIP LOCKED` on PostgreSQL. SQLite serializes writers.
  """
  @spec for_update_skip_locked(Ecto.Queryable.t(), module()) :: Ecto.Query.t()
  def for_update_skip_locked(query, repo) do
    query = Ecto.Queryable.to_query(query)
    if sqlite?(repo), do: query, else: Ecto.Query.lock(query, "FOR UPDATE SKIP LOCKED")
  end

  @doc """
  Current time on the clock that writes Continuum's timestamp columns.

  PostgreSQL reports `clock_timestamp()`. SQLite shares a host with the
  application, so its clock is the application's UTC clock.
  """
  @spec now(module()) :: DateTime.t()
  def now(repo) do
    case dialect(repo) do
      :sqlite ->
        utc_now()

      :postgres ->
        {:ok, %{rows: [[now]]}} = repo.query("SELECT clock_timestamp()")
        now
    end
  end

  @doc false
  @spec utc_now() :: DateTime.t()
  def utc_now, do: DateTime.utc_now() |> usec()

  @doc """
  Encode a timestamp as a raw-query parameter.

  `ecto_sqlite3` stores `utc_datetime_usec` as ISO 8601 text with six
  fractional digits and a `Z` suffix. Raw `exqlite` parameters would bind a
  `DateTime` without the suffix, which sorts before every stored value with
  the same digits, so SQLite parameters are encoded explicitly.
  """
  @spec timestamp(module(), DateTime.t() | nil) :: DateTime.t() | binary() | nil
  def timestamp(_repo, nil), do: nil

  def timestamp(repo, %DateTime{} = datetime) do
    case dialect(repo) do
      :postgres -> datetime
      :sqlite -> encode_sqlite_timestamp!(datetime)
    end
  end

  @doc false
  def encode_sqlite_timestamp!(%DateTime{} = datetime) do
    ensure_iso8601_storage!()

    datetime
    |> DateTime.shift_zone!("Etc/UTC")
    |> usec()
    |> DateTime.to_iso8601()
  end

  @doc """
  Decode a timestamp returned by a raw query.
  """
  @spec decode_timestamp(term()) :: DateTime.t() | NaiveDateTime.t() | nil
  def decode_timestamp(nil), do: nil
  def decode_timestamp(%DateTime{} = datetime), do: datetime
  def decode_timestamp(%NaiveDateTime{} = datetime), do: datetime

  def decode_timestamp(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        datetime

      {:error, _reason} ->
        value |> NaiveDateTime.from_iso8601!() |> DateTime.from_naive!("Etc/UTC")
    end
  end

  @doc """
  Encode a run id for a raw query parameter.
  """
  @spec uuid(module(), binary()) :: binary()
  def uuid(repo, id) when is_binary(id) do
    case dialect(repo) do
      :postgres -> Ecto.UUID.dump!(id)
      :sqlite -> id
    end
  end

  @doc """
  Allocate the next fencing token.

  PostgreSQL uses `continuum_lease_token_seq`. SQLite advances the
  `continuum_sequences` counter, which must happen inside the transaction
  that stores the token so allocation and use commit together.
  """
  @spec next_lease_token!(module()) :: pos_integer()
  def next_lease_token!(repo) do
    sql =
      case dialect(repo) do
        :postgres ->
          "SELECT nextval('continuum_lease_token_seq')"

        :sqlite ->
          """
          UPDATE continuum_sequences
          SET value = value + 1
          WHERE name = 'lease_token'
          RETURNING value
          """
      end

    case repo.query!(sql, []) do
      %{rows: [[token]]} ->
        token

      %{rows: []} ->
        raise ArgumentError,
              "continuum_sequences has no lease_token row; generate the Continuum " <>
                "SQLite migration with mix continuum.gen.migration"
    end
  end

  @doc """
  True when `error` is a database driver error rather than a programming bug.
  """
  @spec database_error?(term()) :: boolean()
  def database_error?(%Postgrex.Error{}), do: true
  def database_error?(%DBConnection.ConnectionError{}), do: true
  def database_error?(error) when is_struct(error, Exqlite.Error), do: true
  def database_error?(_error), do: false

  defp usec(%DateTime{microsecond: {microsecond, _precision}} = datetime) do
    %{datetime | microsecond: {microsecond, 6}}
  end

  defp ensure_iso8601_storage! do
    case Application.get_env(:ecto_sqlite3, :datetime_type, :iso8601) do
      :iso8601 ->
        :ok

      other ->
        raise ArgumentError,
              "Continuum requires ecto_sqlite3's default :iso8601 datetime storage; " <>
                "got config :ecto_sqlite3, datetime_type: #{inspect(other)}"
    end
  end
end
