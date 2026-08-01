defmodule Continuum.Schema.Schedule do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "continuum_schedules" do
    field(:run_id, :binary_id)
    field(:workflow, :string)
    field(:version_hash, :binary)
    field(:input, :binary)
    field(:namespace, :string, default: "default")
    field(:attributes, :map, default: %{})
    field(:trace_context, :binary)
    field(:scheduled_at, :utc_datetime_usec)
    field(:state, :string)
    field(:attempt, :integer, default: 0)
    field(:claimed_at, :utc_datetime_usec)
    field(:started_at, :utc_datetime_usec)
    field(:last_error, :string)
    field(:inserted_at, :utc_datetime_usec)
  end
end
