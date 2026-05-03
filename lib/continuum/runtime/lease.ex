defmodule Continuum.Runtime.Lease do
  @moduledoc """
  Postgres lease acquisition and renewal for workflow runs.

  The lease token is the fencing token used by the Postgres journal. Acquiring
  a lease performs the roadmap's `UPDATE ... RETURNING` CAS: only unowned or
  expired running runs can be claimed, and every successful claim receives a
  fresh monotonic token from `continuum_lease_token_seq`.
  """

  defstruct [:run_id, :owner, :token]

  alias Continuum.Telemetry

  @default_ttl_seconds 30

  @type t :: %__MODULE__{
          run_id: binary(),
          owner: binary(),
          token: integer()
        }

  @doc """
  Build the owner string used in lease rows.
  """
  @spec owner() :: binary()
  def owner do
    "#{node()}/#{System.unique_integer([:positive, :monotonic])}"
  end

  @doc """
  Acquire a lease for a running or suspended run.

  Returns `{:error, :not_acquired}` when another owner still holds the lease.
  """
  @spec acquire(binary(), keyword()) :: {:ok, t()} | {:error, term()}
  def acquire(run_id, opts \\ []) do
    owner = Keyword.get_lazy(opts, :owner, &owner/0)
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)

    sql = """
    UPDATE continuum_runs
    SET lease_owner = $1,
        lease_token = nextval('continuum_lease_token_seq'),
        lease_expires_at = now() + make_interval(secs => $2)
    WHERE id = $3::text::uuid
      AND state IN ('running', 'suspended')
      AND (lease_owner IS NULL OR lease_expires_at < now())
    RETURNING lease_token
    """

    case repo().query(sql, [owner, ttl_seconds, run_id]) do
      {:ok, %{rows: [[token]]}} ->
        Telemetry.execute([:continuum, :lease, :acquired], %{}, %{
          run_id: run_id,
          owner: owner,
          lease_token: token
        })

        {:ok, %__MODULE__{run_id: run_id, owner: owner, token: token}}

      {:ok, %{rows: []}} ->
        {:error, :not_acquired}

      {:error, reason} ->
        {:error, reason}
    end
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
  match, which means another process acquired the fencing token.
  """
  @spec renew(binary(), binary(), integer(), keyword()) ::
          :ok | {:error, :lost} | {:error, term()}
  def renew(run_id, owner, token, opts \\ []) do
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)

    sql = """
    UPDATE continuum_runs
    SET lease_expires_at = now() + make_interval(secs => $4)
    WHERE id = $1::text::uuid
      AND lease_owner = $2
      AND lease_token = $3
      AND state IN ('running', 'suspended')
    RETURNING id
    """

    case repo().query(sql, [run_id, owner, token, ttl_seconds]) do
      {:ok, %{rows: [[_run_id]]}} ->
        Telemetry.execute([:continuum, :lease, :renewed], %{}, %{
          run_id: run_id,
          owner: owner,
          lease_token: token
        })

        :ok

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

  defp repo do
    Application.fetch_env!(:continuum, :repo)
  end
end
