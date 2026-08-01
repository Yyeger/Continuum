defmodule Continuum.ReleaseConsistencyTest do
  use ExUnit.Case, async: true

  test "release version and pre-1.0 install requirements stay aligned" do
    version = Mix.Project.config()[:version]
    requirement = ~s({:continuum, "~> #{version}"})

    assert File.read!("README.md") =~ requirement
    assert File.read!("README.zh-CN.md") =~ requirement
    assert File.read!("README.md") =~ "Continuum is **v#{version} (pre-1.0)**"
    assert File.read!("README.zh-CN.md") =~ "Continuum 当前为 **v#{version}（1.0 之前）**"
    assert File.read!("CHANGELOG.md") =~ "## v#{version} —"

    [major, minor, _patch] = version |> String.split(".") |> Enum.map(&String.to_integer/1)
    assert major == 0
    refute File.read!("README.md") =~ ~s({:continuum, "~> #{major}.#{minor}"}) <> ","
    refute File.read!("README.zh-CN.md") =~ ~s({:continuum, "~> #{major}.#{minor}"}) <> ","
  end
end
