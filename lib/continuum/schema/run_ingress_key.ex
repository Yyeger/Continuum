defmodule Continuum.Schema.RunIngressKey do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @foreign_key_type :binary_id

  schema "continuum_run_ingress_keys" do
    field(:namespace, :string, primary_key: true)
    field(:workflow, :string, primary_key: true)
    field(:idempotency_key, :string, primary_key: true)
    field(:run_id, :binary_id)
    field(:created_at, :utc_datetime_usec)
  end
end
