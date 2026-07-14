defmodule Continuum.Schema.HealthReview do
  @moduledoc false
  use Ecto.Schema

  @primary_key false

  schema "continuum_health_reviews" do
    field(:finding_type, :string, primary_key: true)
    field(:subject_id, :string, primary_key: true)
    field(:fingerprint, :string, primary_key: true)
    field(:reviewed_by, :string)
    field(:reason, :string)
    field(:reviewed_at, :utc_datetime_usec)
  end
end
