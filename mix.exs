defmodule Continuum.MixProject do
  use Mix.Project

  @version "0.7.0"
  @source_url "https://github.com/Yyeger/Continuum"

  def project do
    [
      app: :continuum,
      version: @version,
      elixir: ">= 1.19.0 and < 1.21.0",
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
      {:ecto_sql, dependency_version("~> 3.12", "3.12.0")},
      {:postgrex, dependency_version("~> 0.19", "0.19.0")},
      {:jason, dependency_version("~> 1.4", "1.4.0")},
      {:telemetry, dependency_version("~> 1.2", "1.2.0")},
      {:phoenix_pubsub, dependency_version("~> 2.1", "2.1.0")},
      {:phoenix, dependency_version("~> 1.7", "1.7.0"), optional: true},
      {:phoenix_html, dependency_version("~> 4.0", "4.0.0"), optional: true},
      {:phoenix_live_view, dependency_version("~> 1.0", "1.0.0"), optional: true},
      {:oban, dependency_version("~> 2.20", "2.20.0"), optional: true},
      {:plug_cowboy, dependency_version("~> 2.7", "2.7.0"), only: [:dev, :test], optional: true},
      {:lazy_html, dependency_version(">= 0.1.0", "0.1.0"), only: :test},
      {:stream_data, dependency_version("~> 1.1", "1.1.0"), only: [:dev, :test]},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp dependency_version(supported, minimum) do
    if System.get_env("CONTINUUM_MINIMUM_DEPS") == "1", do: minimum, else: supported
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib priv/static guides mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "Continuum",
      skip_undefined_reference_warnings_on: [
        "README.md",
        "guides/multi-instance.md",
        "guides/observer.md",
        "Continuum",
        "Continuum.Runtime.Context",
        "Continuum.Runtime.Journal",
        "Continuum.VersionRegistry"
      ],
      extras: [
        "README.md",
        "guides/your-first-workflow.md",
        "guides/activities-retries-idempotency.md",
        "guides/idempotency.md",
        "guides/idempotent-ingress.md",
        "guides/retention-pruning.md",
        "guides/schedules.md",
        "guides/multi-instance.md",
        "guides/clustering.md",
        "guides/namespaces.md",
        "guides/signal-contracts.md",
        "guides/replay-safe-logging.md",
        "guides/public-types.md",
        "guides/configuration.md",
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
        "guides/migrations/MIGRATING_v0_5_1_to_v0_6.md",
        "guides/migrations/MIGRATING_v0_6_1_to_v0_6_2.md",
        "guides/migrations/MIGRATING_v0_6_4_to_v0_7_0.md"
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
      "docs.check": ["docs", "run ci/check_doc_links.exs"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"]
    ]
  end
end
