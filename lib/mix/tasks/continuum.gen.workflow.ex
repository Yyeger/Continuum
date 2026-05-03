defmodule Mix.Tasks.Continuum.Gen.Workflow do
  @moduledoc """
  Generates a Continuum workflow module.

      mix continuum.gen.workflow MyApp.OrderFlow
      mix continuum.gen.workflow MyApp.OrderFlow --path lib

  The generated module uses `Continuum.Workflow` and starts with a minimal
  deterministic `run/1` function.
  """

  use Mix.Task

  @shortdoc "Generates a Continuum workflow module"

  @impl true
  def run(args) do
    {opts, argv, _} = OptionParser.parse(args, switches: [path: :string])

    module = parse_module!(argv)
    root = Keyword.get(opts, :path, "lib")
    file = Path.join([File.cwd!(), root, module_file(module)])

    if File.exists?(file), do: Mix.raise("#{Path.relative_to_cwd(file)} already exists")

    File.mkdir_p!(Path.dirname(file))
    File.write!(file, source(module))
    Mix.shell().info("Created #{Path.relative_to_cwd(file)}")
  end

  defp parse_module!([module_name]) do
    Module.concat([module_name])
  rescue
    ArgumentError -> Mix.raise("expected a valid module name, got: #{inspect(module_name)}")
  end

  defp parse_module!(_argv) do
    Mix.raise("usage: mix continuum.gen.workflow MyApp.OrderFlow")
  end

  defp module_file(module) do
    module
    |> Module.split()
    |> Enum.map_join("/", &Macro.underscore/1)
    |> Kernel.<>(".ex")
  end

  defp source(module) do
    """
    defmodule #{inspect(module)} do
      use Continuum.Workflow, version: 1

      def run(input) do
        {:ok, input}
      end
    end
    """
  end
end
