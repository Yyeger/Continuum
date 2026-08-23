defmodule Continuum.Run do
  @moduledoc """
  Stable public projection of one durable workflow run.

  This struct is returned by `Continuum.get_run/2` and in
  `Continuum.list_runs/1` pages. Internal Ecto schemas remain private.
  """

  @type t :: %__MODULE__{
          id: binary(),
          run_id: binary(),
          workflow: binary(),
          state: atom(),
          input: term(),
          attributes: map(),
          namespace: binary(),
          idempotency_key: binary() | nil,
          result: term(),
          error: term(),
          error_stacktrace: term(),
          trace_context: binary() | nil,
          started_at: DateTime.t() | nil,
          completed_at: DateTime.t() | nil,
          lease_owner: binary() | nil,
          lease_token: integer() | nil,
          lease_acquired_at: DateTime.t() | nil,
          lease_heartbeat_at: DateTime.t() | nil,
          lease_expires_at: DateTime.t() | nil,
          next_wakeup_at: DateTime.t() | nil,
          retention_until: DateTime.t() | nil,
          parent_run_id: binary() | nil,
          correlation_id: binary() | nil,
          continued_from_run_id: binary() | nil
        }

  @derive Jason.Encoder
  defstruct [
    :id,
    :run_id,
    :workflow,
    :state,
    :input,
    :attributes,
    :namespace,
    :idempotency_key,
    :result,
    :error,
    :error_stacktrace,
    :trace_context,
    :started_at,
    :completed_at,
    :lease_owner,
    :lease_token,
    :lease_acquired_at,
    :lease_heartbeat_at,
    :lease_expires_at,
    :next_wakeup_at,
    :retention_until,
    :parent_run_id,
    :correlation_id,
    :continued_from_run_id
  ]
end
