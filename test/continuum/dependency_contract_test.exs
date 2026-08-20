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

    # postgrex < 0.22.4 carries EEF-CVE-2026-66838 (SQL injection via the
    # :comment option of Postgrex.stream/4) and phoenix_live_view < 1.2.9 carries
    # EEF-CVE-2026-64941 (open redirect in validate_local_url!/2). `mix hex.audit`
    # fails the build on either, so the floors must not regress.
    assert {:postgrex, postgrex} = Enum.find(deps, &match?({:postgrex, _}, &1))
    assert postgrex =~ "0.22.4"

    assert {:phoenix_live_view, live_view, _opts} =
             Enum.find(deps, &match?({:phoenix_live_view, _, _}, &1))

    assert live_view =~ "1.2.9"
  end
end
