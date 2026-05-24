defmodule Continuum.UnknownVersionError do
  @moduledoc """
  Raised when a durable run references a workflow version that is not loaded.
  """

  defexception [:workflow, :version_hash, :run_id]

  def message(%{workflow: workflow, version_hash: version_hash, run_id: run_id}) do
    "Continuum run #{inspect(run_id)} references unknown workflow version " <>
      "#{inspect(workflow)} / #{inspect(version_hash)}"
  end
end
