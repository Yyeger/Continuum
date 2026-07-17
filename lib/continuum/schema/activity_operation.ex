defmodule Continuum.Schema.ActivityOperation do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "continuum_activity_operations" do
    field(:task_id, :binary_id)
    field(:successor_task_id, :binary_id)
    field(:run_id, :binary_id)
    field(:lineage_id, :binary_id)
    field(:action, :string)
    field(:classification, :string)
    field(:operator, :string)
    field(:reason, :string)
    field(:retry_policy, :binary)
    field(:inserted_at, :utc_datetime_usec)
  end
end
