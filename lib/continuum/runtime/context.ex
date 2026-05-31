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
    :history_index,
    :history_count,
    command_counts: %{},
    snapshot_steps: %{},
    history_offset: 0,
    compensation_stack: []
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
          history_index: :array.array() | nil,
          history_count: non_neg_integer() | nil,
          command_counts: map(),
          snapshot_steps: map(),
          history_offset: non_neg_integer(),
          compensation_stack: [{term(), {module(), atom(), list()}}]
        }

  @doc "Set the current context for this process."
  def put(%__MODULE__{} = ctx), do: Process.put(@key, ensure_history_index(ctx))

  @doc "Read the current context, or nil if not in a workflow process."
  def get do
    case Process.get(@key) do
      %__MODULE__{} = ctx ->
        ctx = ensure_history_index(ctx)
        Process.put(@key, ctx)
        ctx

      nil ->
        nil
    end
  end

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

    case history_event(ctx, ctx.cursor) do
      :compacted_gap ->
        :tail

      nil ->
        :tail

      event ->
        Process.put(@key, %{ctx | cursor: ctx.cursor + 1})
        {:ok, event}
    end
  end

  @doc false
  def ensure_history_index(%__MODULE__{history_index: index} = ctx) when not is_nil(index),
    do: ctx

  def ensure_history_index(%__MODULE__{} = ctx) do
    history = ctx.history || []

    %{
      ctx
      | history_index: :array.from_list(history, nil),
        history_count: length(history)
    }
  end

  @doc false
  def history_event(%__MODULE__{} = ctx, cursor) do
    ctx = ensure_history_index(ctx)
    offset = ctx.history_offset || 0
    index = cursor - offset

    cond do
      cursor < offset ->
        :compacted_gap

      index < 0 or index >= (ctx.history_count || 0) ->
        nil

      true ->
        :array.get(index, ctx.history_index)
    end
  end

  @doc false
  def append_history(%__MODULE__{} = ctx, event) do
    ctx = ensure_history_index(ctx)
    count = ctx.history_count || 0

    %{
      ctx
      | history_index: :array.set(count, event, ctx.history_index),
        history_count: count + 1
    }
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
