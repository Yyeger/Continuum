defmodule Continuum.Schema.Signal do
  @moduledoc false
  use Ecto.Schema

  @foreign_key_type :binary_id

  schema "continuum_signals" do
    field :run_id, :binary_id
    field :name, :string
    field :payload, :map
    field :delivered, :boolean, default: false
    field :inserted_at, :utc_datetime_usec
  end
end
