defmodule Continuum.Test.SQLiteRepo do
  @moduledoc false

  use Ecto.Repo,
    otp_app: :continuum,
    adapter: Ecto.Adapters.SQLite3
end
