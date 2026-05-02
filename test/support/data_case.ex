defmodule Continuum.Test.DataCase do
  @moduledoc """
  ExUnit case template for tests that talk to `Continuum.Test.Repo`.

      use Continuum.Test.DataCase, async: true

  Each test checks out a Sandbox connection. Non-async tests use shared
  mode so workflow GenServers can use the test process' connection.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      alias Continuum.Test.Repo
      import Ecto.Query
    end
  end

  setup tags do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Continuum.Test.Repo)

    unless tags[:async] do
      Ecto.Adapters.SQL.Sandbox.mode(Continuum.Test.Repo, {:shared, self()})
    end

    :ok
  end
end
