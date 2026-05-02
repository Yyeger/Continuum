defmodule Continuum.Schema.Timer do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "continuum_timers" do
    field(:run_id, :binary_id)
    field(:fires_at, :utc_datetime_usec)
    field(:fired, :boolean, default: false)
  end
end
