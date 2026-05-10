defmodule Continuum.MixProject do
  use Mix.Project

  @version "0.1.0-dev"
  @source_url "https://github.com/continuum-elixir/continuum"

  def project do
    [
      app: :continuum,
      version: @version,
      elixir: "~> 1.18",
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
      {:phoenix, "~> 1.7", only: [:dev, :test], optional: true},
      {:phoenix_html, "~> 4.0", only: [:dev, :test], optional: true},
      {:phoenix_live_view, "~> 1.0", only: [:dev, :test], optional: true},
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
      files: ~w(lib priv mix.exs README.md LICENSE CHANGELOG.md)
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
        "guides/observability.md",
        "guides/observer.md",
        "guides/determinism-rules.md"
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*/
      ]
    ]
  end

  defp aliases do
    [
      "test.setup": ["ecto.drop --quiet", "ecto.create --quiet", "ecto.migrate --quiet"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"]
    ]
  end
end
