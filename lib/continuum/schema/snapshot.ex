defmodule Continuum.Schema.Snapshot do
  @moduledoc false
  use Ecto.Schema

  @foreign_key_type :binary_id

  schema "continuum_snapshots" do
    field(:run_id, :binary_id)
    field(:through_seq, :integer)
    field(:version_hash, :binary)
    field(:payload, :binary)
    field(:taken_at, :utc_datetime_usec)
  end
end
