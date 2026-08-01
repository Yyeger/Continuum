defmodule Continuum.Readiness do
  @moduledoc "Stable local runtime readiness and drain status."

  @type t :: %__MODULE__{
          instance: atom(),
          state: :ready | :draining | :drained | :degraded | :not_started,
          ready?: boolean(),
          drained?: boolean(),
          active_run_count: non_neg_integer(),
          pending_claim_count: non_neg_integer(),
          last_drain: map() | nil
        }

  @derive Jason.Encoder
  defstruct [
    :instance,
    :state,
    :last_drain,
    ready?: false,
    drained?: false,
    active_run_count: 0,
    pending_claim_count: 0
  ]
end
