defmodule Continuum.Test.Repo.Migrations.PromoteLegacyCancelledRuns do
  use Ecto.Migration

  # Runs cancelled before v0.5.2 were stored as `failed` with an encoded
  # `:cancelled` error payload, so every read path had to recognise that shape
  # by comparing encoded bytes in SQL. Promote those rows to the canonical
  # terminal state, which makes the state column authoritative on its own and
  # keeps the classification correct once payload encoding is no longer
  # `:erlang.term_to_binary/1`.
  def up do
    execute(fn ->
      repo().query!(
        """
        UPDATE continuum_runs
        SET state = 'cancelled'
        WHERE state = 'failed' AND error = $1
        """,
        [:erlang.term_to_binary(:cancelled)]
      )
    end)
  end

  def down do
    execute(fn ->
      repo().query!(
        """
        UPDATE continuum_runs
        SET state = 'failed'
        WHERE state = 'cancelled' AND error = $1
        """,
        [:erlang.term_to_binary(:cancelled)]
      )
    end)
  end
end
