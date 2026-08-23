defmodule Continuum.ActivityStubError do
  @moduledoc """
  Raised when an activity stub itself is wrong, rather than the activity failing.

  A stub that *raises* is a legitimate way to exercise a workflow's failure
  branch, so `Continuum.Runtime.Effect` normalizes those into `{:error, _}`
  results the way the durable worker does. A stub with the wrong arity, or one
  returning a value that could never survive the journal, is a bug in the test
  instead — this exception is re-raised past that normalization so it surfaces
  as a test failure rather than as a plausible-looking activity error.
  """
  @moduledoc since: "0.8.0"

  defexception [:message]

  @type t :: %__MODULE__{message: String.t()}
end
