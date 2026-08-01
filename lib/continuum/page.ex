defmodule Continuum.Page do
  @moduledoc """
  A stable cursor page returned by Continuum read APIs.

  `next_cursor` is opaque to callers and should only be passed back to the API
  that produced it.
  """

  @type t(entry) :: %__MODULE__{
          entries: [entry],
          per_page: pos_integer() | nil,
          next_cursor: term() | nil
        }

  @derive Jason.Encoder
  defstruct entries: [], per_page: nil, next_cursor: nil
end
