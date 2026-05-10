defmodule Continuum.Test.ConnCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      @endpoint Continuum.Test.ObserverEndpoint

      use Continuum.Test.DataCase, async: false

      import Phoenix.ConnTest
      import Phoenix.LiveViewTest
      import Ecto.Query

      alias Continuum.Test.Repo
    end
  end
end
