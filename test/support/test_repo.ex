defmodule Continuum.Test.Repo do
  @moduledoc false

  use Ecto.Repo,
    otp_app: :continuum,
    adapter: Ecto.Adapters.Postgres
end
