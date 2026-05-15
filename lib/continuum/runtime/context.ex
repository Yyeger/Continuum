defmodule Continuum.Runtime.Context do
  @moduledoc """
  Per-run effect context, kept in the workflow process's process dictionary.

  The context holds the journal handle, the current cursor (how many events
  have been replayed so far), per-callsite command ordinals, the run id, and
  the lease token. It is
  established by `Continuum.Runtime.Engine` before invoking the user's
  `run/1` and unset on suspend or completion.
  """

  @key :continuum_ctx

  defstruct [
    :run_id,
    :history,
    :cursor,
    :workflow_module,
    :lease_token,
    :trace_context,
    :instance,
    :journal,
    command_counts: %{},
    snapshot_steps: %{},
    history_offset: 0
  ]

  @type t :: %__MODULE__{
          run_id: binary(),
          history: list(),
          cursor: non_neg_integer(),
          workflow_module: module(),
          lease_token: integer() | nil,
          trace_context: binary() | nil,
          instance: Continuum.Runtime.Instance.t() | nil,
          journal: module(),
          command_counts: map(),
          snapshot_steps: map(),
          history_offset: non_neg_integer()
        }

  @doc "Set the current context for this process."
  def put(%__MODULE__{} = ctx), do: Process.put(@key, ctx)

  @doc "Read the current context, or nil if not in a workflow process."
  def get, do: Process.get(@key)

  @doc "Clear the context."
  def clear, do: Process.delete(@key)

  @doc "Are we currently inside a workflow process?"
  def active?, do: not is_nil(get())

  @doc """
  Pop the next event from history if available; otherwise return :tail.

  Advances the cursor on a successful pop.
  """
  def next_event do
    ctx = get!()

    case Enum.at(ctx.history, ctx.cursor) do
      nil ->
        :tail

      event ->
        Process.put(@key, %{ctx | cursor: ctx.cursor + 1})
        {:ok, event}
    end
  end

  defp get! do
    case get() do
      %__MODULE__{} = ctx ->
        ctx

      nil ->
        raise Continuum.NotInWorkflowError,
              "this function must be called from inside a Continuum workflow process"
    end
  end
end

defmodule Continuum.NotInWorkflowError do
  @moduledoc "Raised when a workflow primitive is invoked outside a workflow process."
  defexception [:message]
end

defmodule Continuum.ReplayDriftError do
  @moduledoc """
  Raised when the journaled history at the current cursor doesn't match the
  effect that the workflow code is now requesting. Indicates a non-trivial
  code change between original execution and replay.
  """
  defexception [:message, :expected, :actual, :cursor, :run_id]

  def message(%{run_id: run_id, cursor: cursor, expected: expected, actual: actual}) do
    """
    Continuum replay drift detected on run #{inspect(run_id)} at event \
    cursor #{cursor}.

    Expected effect (from journaled history):
      #{inspect(expected)}

    Workflow code requested:
      #{inspect(actual)}

    The workflow code has diverged from the history that was originally
    journaled. The most common causes are:

      * Adding/removing/reordering activity, await, or timer calls.
      * Changing a conditional branch whose condition depends on a value
        that was journaled before the change.

    Use Continuum.patched?/1 to introduce backward-compatible changes, or
    bump @continuum_workflow_version and keep the old clause around for
    in-flight runs to finish on.
    """
  end
end
