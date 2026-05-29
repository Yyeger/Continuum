defmodule Continuum.ActivityRef do
  @moduledoc """
  Handle for a successful activity that carries a compensation (saga DSL).

  When an `activity/2` call is given a `compensate:` MFA, a successful
  (`{:ok, value}`) result is wrapped as `{:ok, %Continuum.ActivityRef{}}` instead
  of being returned as a bare term. The ref carries:

    * `activity_id` — the activity's stable command identity, used to journal and
      match the compensation for this exact call.
    * `result` — the unwrapped success value (the `value` from `{:ok, value}`).
    * `raw_result` — the activity's raw return (`{:ok, value}`).
    * `mfa` — the activity's `{module, function, args}`.
    * `compensate` — the compensation `{module, function, args}` to run on rollback.

  Pass a ref (or `{:ok, ref}`) to `compensate/1`. Use `Continuum.unwrap/1` to
  recover the raw activity return when you don't need the compensation handle.

  Activities **without** a `compensate:` option are unchanged: they return a
  bare term, exactly as in v0.2.
  """

  @type t :: %__MODULE__{
          activity_id: term(),
          result: term(),
          raw_result: term(),
          mfa: {module(), atom(), list()},
          compensate: {module(), atom(), list()}
        }

  @enforce_keys [:activity_id, :result, :raw_result, :mfa, :compensate]
  defstruct [:activity_id, :result, :raw_result, :mfa, :compensate]
end
