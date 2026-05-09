defmodule Continuum.Schema.ActivityResult do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @foreign_key_type :binary_id

  schema "continuum_activity_results" do
    field(:activity_module, :string, primary_key: true)
    field(:idempotency_key, :string, primary_key: true)
    field(:run_id, :binary_id)
    field(:seq, :integer)
    field(:result, :binary)
    field(:completed_at, :utc_datetime_usec)
  end
end
