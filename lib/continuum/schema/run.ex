defmodule Continuum.Schema.Run do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "continuum_runs" do
    field(:workflow, :string)
    field(:version_hash, :binary)
    field(:state, :string)
    field(:input, :map)
    field(:result, :map)
    field(:error, :map)
    field(:started_at, :utc_datetime_usec)
    field(:completed_at, :utc_datetime_usec)
    field(:lease_owner, :string)
    field(:lease_token, :integer)
    field(:lease_expires_at, :utc_datetime_usec)
    field(:next_wakeup_at, :utc_datetime_usec)
    field(:retention_until, :utc_datetime_usec)
  end
end
