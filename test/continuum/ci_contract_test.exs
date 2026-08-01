defmodule Continuum.CIContractTest do
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)

  test "regular workflows are read-only and every job has a timeout" do
    for workflow <- ["ci.yml", "resilience.yml"] do
      source = File.read!(Path.join([@root, ".github", "workflows", workflow]))

      assert source =~ "permissions:\n  contents: read"

      jobs_source = source |> String.split("jobs:\n", parts: 2) |> List.last()

      jobs =
        Regex.scan(~r/^  [a-z][a-z0-9-]+:\n(.*?)(?=^  [a-z][a-z0-9-]+:\n|\z)/ms, jobs_source)

      assert jobs != []
      assert Enum.all?(jobs, fn [_job, body] -> body =~ "timeout-minutes:" end)
    end
  end

  test "quality and release workflows gate packages and provenance" do
    ci = File.read!(Path.join([@root, ".github", "workflows", "ci.yml"]))
    release = File.read!(Path.join([@root, ".github", "workflows", "release-provenance.yml"]))

    assert ci =~ "mix hex.audit"
    assert ci =~ "mix deps.unlock --check-unused"
    assert ci =~ "mix docs.check"
    assert ci =~ "mix hex.build"

    assert release =~ "id-token: write"
    assert release =~ "attestations: write"
    assert release =~ "artifact-metadata: write"
    assert release =~ "actions/attest@v4"
    assert release =~ "subject-path: artifacts/*.tar"
  end
end
