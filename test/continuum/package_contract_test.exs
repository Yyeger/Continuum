defmodule Continuum.PackageContractTest do
  use ExUnit.Case, async: true

  test "Hex package ships runtime assets without test migrations" do
    files = Mix.Project.config() |> Keyword.fetch!(:package) |> Keyword.fetch!(:files)

    assert "priv/static" in files
    refute "priv" in files
  end
end
