defmodule Continuum.Activity.Context do
  @moduledoc """
  Runtime context passed to activities declared with `context: true`.

  Use `heartbeat/2` to persist bounded progress and `cancelled?/1` between
  chunks of external work to cooperate with workflow cancellation.
  """

  @derive {Inspect, only: [:task_id, :run_id, :attempt, :mode]}
  @enforce_keys [:run_id, :instance, :mode]
  defstruct [:task_id, :run_id, :attempt, :instance, :lease_owner, :mode]

  @type t :: %__MODULE__{
          task_id: binary(),
          run_id: binary(),
          attempt: pos_integer(),
          instance: Continuum.Runtime.Instance.t(),
          lease_owner: binary() | nil,
          mode: :durable | :inline
        }

  @doc "Persists the latest progress details using the activity task's lease fence."
  @spec heartbeat(t(), term()) :: :ok | {:error, :cancelled | :lease_lost}
  def heartbeat(%__MODULE__{mode: :inline}, details) do
    Continuum.DurableTerm.validate!(details, :activity_heartbeat)

    if byte_size(:erlang.term_to_binary(details)) > 16_384 do
      raise ArgumentError, "activity heartbeat details exceed 16384 encoded bytes"
    end

    :ok
  end

  def heartbeat(%__MODULE__{} = context, details) do
    Continuum.Runtime.ActivityWorker.record_heartbeat(context, details)
  end

  @doc "Returns true when the run was cancelled or this worker lost its task lease."
  @spec cancelled?(t()) :: boolean()
  def cancelled?(%__MODULE__{mode: :inline}), do: false

  def cancelled?(%__MODULE__{} = context) do
    Continuum.Runtime.ActivityWorker.activity_cancelled?(context)
  end

  @doc false
  def from_task(task) do
    %__MODULE__{
      task_id: task.id,
      run_id: task.run_id,
      attempt: task.attempt,
      instance: task.instance,
      lease_owner: task.lease_owner,
      mode: :durable
    }
  end

  @doc false
  def inline(runtime_context) do
    %__MODULE__{
      run_id: runtime_context.run_id,
      instance: runtime_context.instance,
      mode: :inline
    }
  end
end
