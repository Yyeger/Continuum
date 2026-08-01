defmodule ContinuumDownstream.MixProject do
  use Mix.Project

  def project do
    [
      app: :continuum_downstream,
      version: "0.1.0",
      elixir: ">= 1.19.0 and < 1.21.0",
      start_permanent: false,
      deps: [{:continuum, path: "../.."}]
    ]
  end

  def application do
    [extra_applications: [:logger, :continuum]]
  end
end
