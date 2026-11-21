defmodule Continuum.DependencyContractTest do
  use ExUnit.Case, async: true

  test "Postgrex is required because public runtime modules reference its structs" do
    assert {:postgrex, requirement} =
             Mix.Project.config()
             |> Keyword.fetch!(:deps)
             |> Enum.find(&match?({:postgrex, _}, &1))

    assert requirement =~ "0.22.4"
  end

  test "advisory-affected dependency floors stay above their patched releases" do
    deps = Mix.Project.config() |> Keyword.fetch!(:deps)

    assert {:postgrex, postgrex} = Enum.find(deps, &match?({:postgrex, _}, &1))
    assert postgrex =~ "0.22.4"

    assert {:phoenix_live_view, live_view, _opts} =
             Enum.find(deps, &match?({:phoenix_live_view, _, _}, &1))

    refute Version.match?("1.2.13", live_view)
    assert Version.match?("1.2.14", live_view)
  end
end
