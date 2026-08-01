defmodule Continuum.ConfigTest do
  use ExUnit.Case, async: true

  test "validates the unified children schema before building specs" do
    assert is_list(
             Continuum.children(
               name: :validated_config,
               repo: nil,
               activity_max_concurrency: 4,
               activity_queues: [payments: 2],
               dispatcher: [interval_ms: 500, batch_size: 20]
             )
           )

    assert_raise ArgumentError, ~r/unknown Continuum children options: \[:dispacher\]/, fn ->
      Continuum.children(dispacher: [])
    end

    assert_raise ArgumentError, ~r/dispatcher\.interval_ms/, fn ->
      Continuum.children(dispatcher: [interval_ms: 0])
    end

    assert_raise ArgumentError, ~r/duplicate Continuum children options/, fn ->
      Continuum.children(name: :one, name: :two)
    end
  end

  test "validates direct instance construction from the same schema" do
    assert_raise ArgumentError, ~r/unknown Continuum instance options/, fn ->
      Continuum.Runtime.Instance.new(name: :config_test, mystery: true)
    end

    assert_raise ArgumentError, ~r/activity_max_concurrency/, fn ->
      Continuum.Runtime.Instance.new(name: :config_test, activity_max_concurrency: 0)
    end
  end

  test "committed configuration reference matches the schema generator" do
    path = Path.expand("../../guides/configuration.md", __DIR__)
    assert File.read!(path) == Continuum.Config.reference_markdown()
    assert Continuum.Config.reference_markdown() =~ "`:builtin \\| {:oban, keyword}`"
  end
end
