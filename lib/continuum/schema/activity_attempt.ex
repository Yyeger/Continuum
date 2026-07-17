defmodule Continuum.Schema.ActivityAttempt do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:id, :id, autogenerate: true}
  @foreign_key_type :binary_id

  schema "continuum_activity_attempts" do
    field(:task_id, :binary_id)
    field(:run_id, :binary_id)
    field(:lineage_id, :binary_id)
    field(:attempt, :integer)
    field(:outcome, :string)
    field(:error, :binary)
    field(:recorded_at, :utc_datetime_usec)
  end
end
