defmodule Mix.Tasks.Continuum.Partitions.List do
  @moduledoc """
  Lists managed `continuum_events` partitions and their row counts.

      mix continuum.partitions.list
      mix continuum.partitions.list --repo MyApp.Repo
  """
  use Mix.Task

  @shortdoc "Lists continuum_events partitions"

  @impl true
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string])
    Mix.Task.run("app.start")

    repo = parse_repo(opts)

    repo
    |> partitions()
    |> Enum.each(fn partition ->
      %{rows: [[count]]} = repo.query!("SELECT count(*) FROM #{quote_ident(partition)}")
      Mix.shell().info("#{partition}\t#{count}")
    end)
  end

  defp partitions(repo) do
    %{rows: rows} =
      repo.query!("""
      SELECT c.relname
      FROM pg_inherits i
      JOIN pg_class c ON c.oid = i.inhrelid
      JOIN pg_class p ON p.oid = i.inhparent
      WHERE p.relname = 'continuum_events'
      ORDER BY c.relname
      """)

    Enum.map(rows, fn [name] -> name end)
  end

  defp parse_repo(opts) do
    case opts[:repo] do
      nil ->
        Application.get_env(:continuum, :repo) ||
          Mix.raise("no repo configured. Pass --repo MyApp.Repo or set :continuum, :repo")

      repo ->
        Module.concat([repo])
    end
  end

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")
end
