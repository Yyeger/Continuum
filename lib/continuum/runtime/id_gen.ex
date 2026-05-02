defmodule Continuum.Runtime.IdGen do
  @moduledoc false

  @doc "Generate a fresh run id (v4 UUID, externally generated — not journaled)."
  def run_id do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)

    <<a::32, b::16, c::16, d::16, e::48>> =
      <<u0::48, 4::4, u1::12, 2::2, u2::62>>

    :io_lib.format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [a, b, c, d, e])
    |> IO.iodata_to_binary()
  end
end
