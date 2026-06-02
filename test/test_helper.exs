ExUnit.start()

cluster_test? = System.get_env("CONTINUUM_CLUSTER_TEST") == "1"

unless cluster_test? do
  ExUnit.configure(exclude: [cluster: true])
end

if cluster_test? do
  config = Application.fetch_env!(:continuum, Continuum.Test.Repo)

  Application.put_env(
    :continuum,
    Continuum.Test.Repo,
    config
    |> Keyword.delete(:pool)
    |> Keyword.put(:pool_size, 20)
  )
end

case Continuum.Test.Repo.start_link() do
  {:ok, _pid} -> :ok
  {:error, {:already_started, _pid}} -> :ok
end

case Continuum.Test.ObserverEndpoint.start_link() do
  {:ok, _pid} -> :ok
  {:error, {:already_started, _pid}} -> :ok
end

unless cluster_test? do
  Ecto.Adapters.SQL.Sandbox.mode(Continuum.Test.Repo, :manual)
end

# `--paranoid` re-replay mode: when enabled, every completed run is re-replayed
# from its journaled history and asserted identical. Off by default so ordinary
# `mix test` stays fast. Enable with `CONTINUUM_PARANOID=1 mix test`.
if Continuum.Test.Paranoid.enabled?() do
  Continuum.Test.Paranoid.attach!()

  ExUnit.after_suite(fn _result ->
    case Continuum.Test.Paranoid.mismatches() do
      [] ->
        :ok

      mismatches ->
        IO.puts(:stderr, "\n#{length(mismatches)} Continuum paranoid replay mismatch(es):")
        Enum.each(mismatches, &IO.puts(:stderr, "  - #{inspect(&1)}"))
        System.at_exit(fn _ -> exit({:shutdown, 1}) end)
    end
  end)
end
