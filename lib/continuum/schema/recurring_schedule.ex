defmodule Continuum.Schema.RecurringSchedule do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: false}
  schema "continuum_recurring_schedules" do
    field(:workflow, :string)
    field(:version_hash, :binary)
    field(:input, :binary)
    field(:namespace, :string, default: "default")
    field(:attributes, :map, default: %{})
    field(:trace_context, :binary)
    field(:every_ms, :integer)
    field(:next_occurrence_at, :utc_datetime_usec)
    field(:overlap_policy, :string)
    field(:missed_policy, :string)
    field(:max_catch_up, :integer, default: 10)
    field(:state, :string, default: "active")
    field(:inserted_at, :utc_datetime_usec)
  end
end
