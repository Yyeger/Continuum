defmodule Continuum.Schema.Run do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "continuum_runs" do
    field(:workflow, :string)
    field(:version_hash, :binary)
    field(:state, :string)
    field(:input, :binary)
    field(:result, :binary)
    field(:error, :binary)
    field(:trace_context, :binary)
    field(:started_at, :utc_datetime_usec)
    field(:completed_at, :utc_datetime_usec)
    field(:lease_owner, :string)
    field(:lease_token, :integer)
    field(:lease_expires_at, :utc_datetime_usec)
    field(:next_wakeup_at, :utc_datetime_usec)
    field(:retention_until, :utc_datetime_usec)
    field(:parent_run_id, :binary_id)
    field(:parent_command_id, :binary)
    field(:correlation_id, :binary_id)
    field(:continued_from_run_id, :binary_id)
  end
end
