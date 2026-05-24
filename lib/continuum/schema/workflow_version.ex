defmodule Continuum.Schema.WorkflowVersion do
  @moduledoc false
  use Ecto.Schema

  @primary_key false

  schema "continuum_workflow_versions" do
    field(:workflow, :string, primary_key: true)
    field(:version_hash, :binary, primary_key: true)
    field(:entrypoint, :string)
    field(:registered_at, :utc_datetime_usec)
  end
end
