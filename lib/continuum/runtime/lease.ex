defmodule Continuum.Runtime.Lease do
  @moduledoc """
  Postgres lease acquisition and renewal for workflow runs.

  The lease token is the fencing token used by the Postgres journal. Acquiring
  a lease performs the roadmap's `UPDATE ... RETURNING` CAS: only unowned or
  expired running runs can be claimed, and every successful claim receives a
  fresh monotonic token from `continuum_lease_token_seq`.

  On SQLite the same CAS runs inside an immediate transaction, tokens come from
  the `continuum_sequences` counter, and lease times use the host clock that
  SQLite shares with the application.
  """

  defstruct [:run_id, :owner, :token, :cancel_requested_at]

  alias Continuum.Runtime.SQL
  alias Continuum.Telemetry

  @default_ttl_seconds 30

  @type t :: %__MODULE__{
          run_id: binary(),
          owner: binary(),
          token: integer(),
          cancel_requested_at: DateTime.t() | NaiveDateTime.t() | nil
        }

  @doc """
  Build the owner string used in lease rows.
  """
  @spec owner() :: binary()
  def owner do
    owner(Continuum)
  end

  def owner(instance_name) do
    "#{node()}/#{instance_name}/#{System.unique_integer([:positive, :monotonic])}"
  end

  @doc """
  Acquire a lease for a running or suspended run.

  Returns `{:error, :not_acquired}` when another owner still holds the lease.
  """
  @spec acquire(binary(), keyword()) :: {:ok, t()} | {:error, term()}
  def acquire(run_id, opts \\ []) do
    owner = Keyword.get_lazy(opts, :owner, &owner/0)
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)

    case acquire_row(repo(opts), run_id, owner, ttl_seconds) do
      {:ok, [token, cancel_requested_at]} ->
        Telemetry.execute([:continuum, :lease, :acquired], %{}, %{
          run_id: run_id,
          owner: owner,
          lease_token: token
        })

        {:ok,
         %__MODULE__{
           run_id: run_id,
           owner: owner,
           token: token,
           cancel_requested_at: cancel_requested_at
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp acquire_row(repo, run_id, owner, ttl_seconds) do
    case SQL.dialect(repo) do
      :postgres -> acquire_row_postgres(repo, run_id, owner, ttl_seconds)
      :sqlite -> acquire_row_sqlite(repo, run_id, owner, ttl_seconds)
    end
  end

  defp acquire_row_postgres(repo, run_id, owner, ttl_seconds) do
    sql = """
    UPDATE continuum_runs
    SET lease_owner = $1,
        lease_token = nextval('continuum_lease_token_seq'),
        lease_acquired_at = clock_timestamp(),
        lease_heartbeat_at = clock_timestamp(),
        lease_expires_at = now() + make_interval(secs => $2)
    WHERE id = $3::text::uuid
      AND state IN ('running', 'suspended')
      AND (lease_owner IS NULL OR lease_expires_at < now())
    RETURNING lease_token, cancel_requested_at
    """

    case repo.query(sql, [owner, ttl_seconds, run_id]) do
      {:ok, %{rows: [row]}} -> {:ok, row}
      {:ok, %{rows: []}} -> {:error, :not_acquired}
      {:error, reason} -> {:error, reason}
    end
  end

  # The token is allocated only after the CAS matched, in the same writer
  # transaction, so a refused acquisition leaves the counter untouched.
  defp acquire_row_sqlite(repo, run_id, owner, ttl_seconds) do
    now = SQL.utc_now()

    SQL.transaction(repo, fn ->
      claimable =
        repo.query(
          """
          SELECT 1 FROM continuum_runs
          WHERE id = ?1
            AND state IN ('running', 'suspended')
            AND (lease_owner IS NULL OR lease_expires_at < ?2)
          """,
          [run_id, SQL.timestamp(repo, now)]
        )

      case claimable do
        {:ok, %{rows: [_row]}} -> lease_sqlite_row!(repo, run_id, owner, ttl_seconds, now)
        {:ok, %{rows: []}} -> repo.rollback(:not_acquired)
        {:error, reason} -> repo.rollback(reason)
      end
    end)
  end

  @doc false
  # Stamp a fresh lease on a run the caller has already selected inside an
  # immediate SQLite transaction. Returns `[token, cancel_requested_at]`.
  def lease_sqlite_row!(repo, run_id, owner, ttl_seconds, now) do
    token = SQL.next_lease_token!(repo)

    %{rows: [[token, cancel_requested_at]]} =
      repo.query!(
        """
        UPDATE continuum_runs
        SET lease_owner = ?1,
            lease_token = ?2,
            lease_acquired_at = ?3,
            lease_heartbeat_at = ?3,
            lease_expires_at = ?4
        WHERE id = ?5
        RETURNING lease_token, cancel_requested_at
        """,
        [
          owner,
          token,
          SQL.timestamp(repo, now),
          SQL.timestamp(repo, DateTime.add(now, ttl_seconds, :second)),
          run_id
        ]
      )

    [token, SQL.decode_timestamp(cancel_requested_at)]
  end

  @doc """
  Acquire a lease or raise.
  """
  @spec acquire!(binary(), keyword()) :: t()
  def acquire!(run_id, opts \\ []) do
    case acquire(run_id, opts) do
      {:ok, lease} -> lease
      {:error, reason} -> raise "Continuum.Runtime.Lease acquire failed: #{inspect(reason)}"
    end
  end

  @doc """
  Renew a lease by owner and token.

  Returns `{:error, :lost}` when the row is gone or the owner/token no longer
  match, which means another process acquired the fencing token. Returns
  `{:ok, :cancel_requested}` when the renewal succeeded but a durable cancel
  request is pending on the run (set by `Continuum.cancel/2` when the owning
  engine was unreachable).
  """
  @spec renew(binary(), binary(), integer(), keyword()) ::
          :ok | {:ok, :cancel_requested} | {:error, :lost} | {:error, term()}
  def renew(run_id, owner, token, opts \\ []) do
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)
    repo = repo(opts)

    case repo.query(renew_sql(repo), renew_params(repo, run_id, owner, token, ttl_seconds)) do
      {:ok, %{rows: [[cancel_requested_at]]}} ->
        Telemetry.execute([:continuum, :lease, :renewed], %{}, %{
          run_id: run_id,
          owner: owner,
          lease_token: token
        })

        # A durable cancel request recorded while no caller could reach the
        # owning engine; the owner must honor it.
        if is_nil(cancel_requested_at), do: :ok, else: {:ok, :cancel_requested}

      {:ok, %{rows: []}} ->
        Telemetry.execute([:continuum, :lease, :lost], %{}, %{
          run_id: run_id,
          owner: owner,
          lease_token: token
        })

        {:error, :lost}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp renew_sql(repo) do
    case SQL.dialect(repo) do
      :postgres ->
        """
        UPDATE continuum_runs
        SET lease_heartbeat_at = clock_timestamp(),
            lease_expires_at = now() + make_interval(secs => $4)
        WHERE id = $1::text::uuid
          AND lease_owner = $2
          AND lease_token = $3
          AND state IN ('running', 'suspended')
        RETURNING cancel_requested_at
        """

      :sqlite ->
        """
        UPDATE continuum_runs
        SET lease_heartbeat_at = ?4,
            lease_expires_at = ?5
        WHERE id = ?1
          AND lease_owner = ?2
          AND lease_token = ?3
          AND state IN ('running', 'suspended')
        RETURNING cancel_requested_at
        """
    end
  end

  defp renew_params(repo, run_id, owner, token, ttl_seconds) do
    case SQL.dialect(repo) do
      :postgres ->
        [run_id, owner, token, ttl_seconds]

      :sqlite ->
        now = SQL.utc_now()

        [
          run_id,
          owner,
          token,
          SQL.timestamp(repo, now),
          SQL.timestamp(repo, DateTime.add(now, ttl_seconds, :second))
        ]
    end
  end

  @doc false
  def renew_batch(entries, opts) do
    case SQL.dialect(repo(opts)) do
      :postgres -> renew_batch_postgres(entries, opts)
      :sqlite -> renew_batch_sqlite(entries, opts)
    end
  end

  defp renew_batch_postgres(entries, opts) do
    ids = Enum.map(entries, fn {run_id, _entry} -> Ecto.UUID.dump!(run_id) end)
    owners = Enum.map(entries, fn {_run_id, entry} -> entry.owner end)
    tokens = Enum.map(entries, fn {_run_id, entry} -> entry.token end)
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)

    sql = """
    UPDATE continuum_runs AS run
    SET lease_heartbeat_at = clock_timestamp(),
        lease_expires_at = clock_timestamp() + make_interval(secs => $4)
    FROM unnest($1::uuid[], $2::text[], $3::bigint[]) AS claim(id, owner, token)
    WHERE run.id = claim.id
      AND run.lease_owner = claim.owner
      AND run.lease_token = claim.token
      AND run.state IN ('running', 'suspended')
    RETURNING run.id::text, run.cancel_requested_at
    """

    case repo(opts).query(sql, [ids, owners, tokens, ttl_seconds]) do
      {:ok, %{rows: rows}} -> {:ok, batch_outcomes(entries, rows)}
      {:error, reason} -> {:error, reason}
    end
  end

  # One statement, like the PostgreSQL `unnest` join: the claims travel as a
  # JSON array so every renewal is fenced by its own owner and token. SQLite's
  # RETURNING names columns of the updated table unqualified.
  defp renew_batch_sqlite(entries, opts) do
    repo = repo(opts)
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)
    now = SQL.utc_now()

    claims =
      Jason.encode!(
        Enum.map(entries, fn {run_id, entry} -> [run_id, entry.owner, entry.token] end)
      )

    sql = """
    UPDATE continuum_runs AS run
    SET lease_heartbeat_at = ?1,
        lease_expires_at = ?2
    FROM (
      SELECT json_extract(value, '$[0]') AS id,
             json_extract(value, '$[1]') AS owner,
             json_extract(value, '$[2]') AS token
      FROM json_each(?3)
    ) AS claim
    WHERE run.id = claim.id
      AND run.lease_owner = claim.owner
      AND run.lease_token = claim.token
      AND run.state IN ('running', 'suspended')
    RETURNING id, cancel_requested_at
    """

    params = [
      SQL.timestamp(repo, now),
      SQL.timestamp(repo, DateTime.add(now, ttl_seconds, :second)),
      claims
    ]

    case repo.query(sql, params) do
      {:ok, %{rows: rows}} -> {:ok, batch_outcomes(entries, rows)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp batch_outcomes(entries, rows) do
    renewed = Map.new(rows, fn [id, cancel_requested_at] -> {id, cancel_requested_at} end)

    Map.new(entries, fn {run_id, entry} ->
      outcome = batch_outcome(Map.fetch(renewed, run_id))
      event = if outcome == {:error, :lost}, do: :lost, else: :renewed

      Telemetry.execute([:continuum, :lease, event], %{}, %{
        run_id: run_id,
        owner: entry.owner,
        lease_token: entry.token
      })

      {run_id, outcome}
    end)
  end

  defp batch_outcome({:ok, nil}), do: :ok
  defp batch_outcome({:ok, _cancel_requested_at}), do: {:ok, :cancel_requested}
  defp batch_outcome(:error), do: {:error, :lost}

  @doc """
  Release a lease only while its owner and fencing token still match.

  Clearing the token fences the old engine before another dispatcher can
  acquire the run. `{:error, :lost}` means the run is terminal or its lease was
  already released or replaced.
  """
  @spec release(binary(), binary(), integer(), keyword()) :: :ok | {:error, term()}
  def release(run_id, owner, token, opts \\ []) do
    sql = """
    UPDATE continuum_runs
    SET lease_owner = NULL,
        lease_token = NULL,
        lease_expires_at = NULL
    WHERE id = $1#{if SQL.postgres?(repo(opts)), do: "::text::uuid"}
      AND lease_owner = $2
      AND lease_token = $3
      AND state IN ('running', 'suspended')
    """

    case SQL.query(repo(opts), sql, [run_id, owner, token]) do
      {:ok, %{num_rows: 1}} ->
        Telemetry.execute([:continuum, :lease, :released], %{}, %{
          run_id: run_id,
          owner: owner,
          lease_token: token
        })

        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, :lost}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp repo(opts) do
    Keyword.get(opts, :repo) || Application.fetch_env!(:continuum, :repo)
  end
end
