defmodule Continuum.Schema.Event do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @foreign_key_type :binary_id

  schema "continuum_events" do
    field :run_id, :binary_id, primary_key: true
    field :seq, :integer, primary_key: true
    field :event_type, :string
    field :payload, :map
    field :inserted_at, :utc_datetime_usec
  end
end
