ExUnit.start()

case Continuum.Test.Repo.start_link() do
  {:ok, _pid} -> :ok
  {:error, {:already_started, _pid}} -> :ok
end

case Continuum.Test.ObserverEndpoint.start_link() do
  {:ok, _pid} -> :ok
  {:error, {:already_started, _pid}} -> :ok
end

Ecto.Adapters.SQL.Sandbox.mode(Continuum.Test.Repo, :manual)

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
