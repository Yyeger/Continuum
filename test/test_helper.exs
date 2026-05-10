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
