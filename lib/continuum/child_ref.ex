defmodule Continuum.ChildRef do
  @moduledoc """
  Handle for an asynchronously started child workflow.

  `start_child/3` returns a `%Continuum.ChildRef{}`; pass it to `await_child/1`
  to suspend until the child terminates and recover its result. The ref is
  deterministic across replays:

    * `child_run_id` — the child run's id, derived deterministically from the
      parent run id, the start command id, and any `id:` option, so the same
      parent at the same cursor never starts two children on replay.
    * `start_command_id` — the command identity of the `start_child` call site.
    * `workflow` — the child workflow module.
  """

  @type t :: %__MODULE__{
          child_run_id: binary(),
          start_command_id: term(),
          workflow: module()
        }

  @enforce_keys [:child_run_id, :start_command_id, :workflow]
  defstruct [:child_run_id, :start_command_id, :workflow]
end
