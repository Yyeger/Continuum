defmodule Continuum.SQLite.SQLTest do
  use Continuum.Test.SQLiteCase

  alias Continuum.Runtime.SQL
  alias Continuum.Schema.Run

  test "dialect follows the repo adapter" do
    assert SQL.dialect(Continuum.Test.Repo) == :postgres
    assert SQL.dialect(SQLiteRepo) == :sqlite
    assert SQL.dialect(nil) == :postgres
  end

  test "timestamp parameters compare exactly like values stored by Ecto" do
    stored = ~U[2026-10-18 12:00:00.123000Z]
    insert_run!(lease_expires_at: stored)

    before = DateTime.add(stored, -1, :microsecond)
    later = DateTime.add(stored, 1, :microsecond)

    assert count_expiring_before(later) == 1
    assert count_expiring_before(stored) == 0
    assert count_expiring_before(before) == 0
    assert SQL.timestamp(SQLiteRepo, stored) == "2026-10-18T12:00:00.123000Z"
  end

  test "raw timestamps decode back to UTC datetimes" do
    assert SQL.decode_timestamp("2026-10-18T12:00:00.123456Z") == ~U[2026-10-18 12:00:00.123456Z]
    assert SQL.decode_timestamp(nil) == nil
  end

  test "lease tokens are monotonic and allocated inside the caller's transaction" do
    first = SQL.next_lease_token!(SQLiteRepo)
    second = SQL.next_lease_token!(SQLiteRepo)
    assert second == first + 1

    assert {:error, :rollback} =
             SQL.transaction(SQLiteRepo, fn ->
               SQL.next_lease_token!(SQLiteRepo)
               SQLiteRepo.rollback(:rollback)
             end)

    assert SQL.next_lease_token!(SQLiteRepo) == second + 1
  end

  test "after-commit callbacks run once the outer transaction commits" do
    parent = self()

    assert {:ok, :done} =
             SQL.transaction(SQLiteRepo, fn ->
               SQL.after_commit(fn -> send(parent, :outer) end)

               {:ok, :nested} =
                 SQL.transaction(SQLiteRepo, fn ->
                   SQL.after_commit(fn -> send(parent, :nested) end)
                   :nested
                 end)

               refute_received _any
               :done
             end)

    assert_received :outer
    assert_received :nested
  end

  test "after-commit callbacks are dropped when the transaction rolls back" do
    parent = self()

    assert {:error, :nope} =
             SQL.transaction(SQLiteRepo, fn ->
               SQL.after_commit(fn -> send(parent, :committed) end)
               SQLiteRepo.rollback(:nope)
             end)

    assert_raise RuntimeError, fn ->
      SQL.transaction(SQLiteRepo, fn ->
        SQL.after_commit(fn -> send(parent, :committed) end)
        raise "boom"
      end)
    end

    refute_received :committed
    assert SQL.after_commit(fn -> send(parent, :immediate) end) == :ok
    assert_received :immediate
  end

  test "SQLite transactions take the writer lock before their first read" do
    parent = self()
    run_id = insert_run!([]).id

    holder =
      Task.async(fn ->
        SQL.transaction(SQLiteRepo, fn ->
          SQLiteRepo.one!(from(r in Run, where: r.id == ^run_id, select: r.state))
          send(parent, :locked)

          receive do
            :release -> :ok
          end

          SQLiteRepo.update_all(from(r in Run, where: r.id == ^run_id), set: [state: "first"])
        end)
      end)

    assert_receive :locked

    contender =
      Task.async(fn ->
        SQL.transaction(SQLiteRepo, fn ->
          state = SQLiteRepo.one!(from(r in Run, where: r.id == ^run_id, select: r.state))
          SQLiteRepo.update_all(from(r in Run, where: r.id == ^run_id), set: [state: "second"])
          state
        end)
      end)

    refute Task.yield(contender, 100)
    send(holder.pid, :release)

    assert {:ok, {1, nil}} = Task.await(holder)
    assert {:ok, "first"} = Task.await(contender)
  end

  defp count_expiring_before(datetime) do
    %{rows: [[count]]} =
      SQLiteRepo.query!(
        "SELECT count(*) FROM continuum_runs WHERE lease_expires_at < ?1",
        [SQL.timestamp(SQLiteRepo, datetime)]
      )

    count
  end

  defp insert_run!(fields) do
    id = Ecto.UUID.generate()

    SQLiteRepo.insert!(
      struct(
        %Run{
          id: id,
          workflow: "Elixir.Example",
          version_hash: <<1>>,
          state: "running",
          input: :erlang.term_to_binary(nil)
        },
        fields
      )
    )
  end
end
