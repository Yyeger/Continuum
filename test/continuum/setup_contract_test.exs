defmodule Continuum.SetupContractTest do
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)

  test "README, Compose, and development Repo use the same host port" do
    readme = File.read!(Path.join(@root, "README.md"))
    compose = File.read!(Path.join(@root, "docker-compose.yml"))
    dev_config = Config.Reader.read!(Path.join(@root, "config/dev.exs"))

    repo_config = get_in(dev_config, [:continuum, Continuum.Test.Repo])

    assert repo_config[:port] == 5_433
    assert compose =~ ~s("5433:5432")
    assert readme =~ "Postgres on localhost:5433"
    refute readme =~ "Postgres on localhost:5432"
  end

  test "README describes current unknown-version suspension semantics" do
    readme = File.read!(Path.join(@root, "README.md"))

    assert readme =~ "leaves a run suspended when code is missing"
    refute readme =~ "marks missing code `:stuck_unknown_version`"
  end
end
