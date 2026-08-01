defmodule Continuum.Observer.Path do
  @moduledoc false

  def runs(base), do: normalize_base(base)
  def run(base, run_id), do: normalize_base(base) <> "/runs/" <> to_string(run_id)
  def health(base), do: normalize_base(base) <> "/health"

  defp normalize_base(base) do
    case String.trim_trailing(base, "/") do
      "" -> "/"
      normalized -> normalized
    end
  end
end
