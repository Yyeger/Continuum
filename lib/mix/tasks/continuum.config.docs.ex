defmodule Mix.Tasks.Continuum.Config.Docs do
  use Mix.Task

  @shortdoc "Generate the Continuum runtime configuration reference"

  @impl true
  def run(args) do
    {opts, _argv, _invalid} =
      OptionParser.parse(args, strict: [check: :boolean, output: :string])

    output = Keyword.get(opts, :output, "guides/configuration.md")
    rendered = Continuum.Config.reference_markdown()

    if Keyword.get(opts, :check, false) do
      if File.read(output) == {:ok, rendered} do
        Mix.shell().info("Continuum configuration reference is current")
      else
        Mix.raise("Continuum configuration reference is stale; run mix continuum.config.docs")
      end
    else
      File.mkdir_p!(Path.dirname(output))
      File.write!(output, rendered)
      Mix.shell().info("Generated #{output}")
    end
  end
end
