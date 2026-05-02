defmodule Continuum.Schema.ActivityTask do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "continuum_activity_tasks" do
    field(:run_id, :binary_id)
    field(:seq, :integer)
    field(:mfa, :map)
    field(:attempt, :integer, default: 1)
    field(:state, :string)
    field(:scheduled_at, :utc_datetime_usec)
    field(:available_at, :utc_datetime_usec)
    field(:lease_owner, :string)
    field(:lease_expires_at, :utc_datetime_usec)
    field(:result, :map)
    field(:error, :map)
  end
end
