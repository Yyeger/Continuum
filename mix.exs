defmodule Continuum.MixProject do
  use Mix.Project

  @version "0.6.1"
  @source_url "https://github.com/Yyeger/Continuum"

  def project do
    [
      app: :continuum,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "OTP-native durable execution engine for Elixir.",
      package: package(),
      docs: docs(),
      source_url: @source_url,
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Continuum.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:ecto_sql, "~> 3.12"},
      {:postgrex, "~> 0.19", optional: true},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:phoenix_pubsub, "~> 2.1"},
      {:phoenix, "~> 1.7", optional: true},
      {:phoenix_html, "~> 4.0", optional: true},
      {:phoenix_live_view, "~> 1.0", optional: true},
      {:oban, "~> 2.20", optional: true},
      {:plug_cowboy, "~> 2.7", only: [:dev, :test], optional: true},
      {:lazy_html, ">= 0.1.0", only: :test},
      {:stream_data, "~> 1.1", only: [:dev, :test]},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib priv guides mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "Continuum",
      extras: [
        "README.md",
        "guides/your-first-workflow.md",
        "guides/activities-retries-idempotency.md",
        "guides/idempotency.md",
        "guides/multi-instance.md",
        "guides/clustering.md",
        "guides/namespaces.md",
        "guides/search-and-query.md",
        "guides/sagas.md",
        "guides/child-workflows.md",
        "guides/long-running-workflows.md",
        "guides/patching.md",
        "guides/workflow-versioning.md",
        "guides/operations.md",
        "guides/auditing.md",
        "guides/oban-executor.md",
        "guides/observability.md",
        "guides/observer.md",
        "guides/snapshots.md",
        "guides/determinism-rules.md",
        "guides/migrations/MIGRATING_v0_1_to_v0_2.md",
        "guides/migrations/MIGRATING_v0_2_to_v0_3.md",
        "guides/migrations/MIGRATING_v0_3_to_v0_4.md",
        "guides/migrations/MIGRATING_v0_4_to_v0_5.md",
        "guides/migrations/MIGRATING_v0_5_to_v0_5_1.md",
        "guides/migrations/MIGRATING_v0_5_1_to_v0_6.md"
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*/
      ]
    ]
  end

  defp aliases do
    [
      "test.setup": ["ecto.drop --quiet", "ecto.create --quiet", "ecto.migrate --quiet"],
      "test.cluster": ["cmd env CONTINUUM_CLUSTER_TEST=1 mix test --only cluster test/cluster"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"]
    ]
  end
end
