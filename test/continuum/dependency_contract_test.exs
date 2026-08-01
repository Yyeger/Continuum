defmodule Continuum.DependencyContractTest do
  use ExUnit.Case, async: true

  test "Postgrex is required because public runtime modules reference its structs" do
    assert {:postgrex, requirement} =
             Mix.Project.config()
             |> Keyword.fetch!(:deps)
             |> Enum.find(&match?({:postgrex, _}, &1))

    assert requirement =~ "0.19"
  end
end
