defmodule Continuum.Runtime.JournalResolutionTest do
  @moduledoc """
  Regression tests for audit finding 2.2: the journal adapter is resolved
  through `Continuum.Runtime.Instance.journal/1` — one source of truth — so
  `Continuum.start/await/signal/cancel` without an explicit `journal:` opt
  all agree on which journal a run lives in.

  The README quickstart configures `config :continuum, journal: Postgres`
  and then calls bare `Continuum.start/2`; before the fix, the engine
  defaulted to the in-memory journal while `Continuum.await/2` polled
  Postgres and returned `{:error, :not_found}`.
  """

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.{Instance, Journal}
  alias Continuum.Schema.Run

  setup do
    previous_journal = Application.get_env(:continuum, :journal)
    Application.put_env(:continuum, :journal, Journal.Postgres)

    on_exit(fn ->
      restore_env(:journal, previous_journal)
    end)

    :ok
  end

  defmodule QuickstartFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      n = Continuum.side_effect(fn -> input.seed + 1 end)
      {:ok, n}
    end
  end

  test "bare Continuum.start with the README config produces a durable run" do
    {:ok, run_id} = Continuum.start(QuickstartFlow, %{seed: 41})

    assert {:ok, %{state: :completed, result: {:ok, 42}}} = Continuum.await(run_id, 1_000)

    assert %Run{state: "completed"} = Repo.get(Run, run_id)
    assert Journal.Postgres.load(Instance.default(), run_id) != []
  end

  test "bare Continuum.signal reaches the durable mailbox of a bare-started run" do
    defmodule SignalQuickstartFlow do
      use Continuum.Workflow, version: 1

      def run(_input) do
        decision = await(signal(:decision))
        {:ok, decision}
      end
    end

    {:ok, run_id} = Continuum.start(SignalQuickstartFlow, %{})
    :ok = Continuum.signal(run_id, :decision, :go)

    assert {:ok, %{state: :completed, result: {:ok, :go}}} = Continuum.await(run_id, 1_000)
  end

  test "the default instance follows config :continuum, :journal at call time" do
    assert Instance.journal(Instance.default()) == Journal.Postgres

    Application.put_env(:continuum, :journal, Journal.InMemory)
    assert Instance.journal(Instance.default()) == Journal.InMemory
  end

  test "a named instance with a repo defaults to the Postgres journal" do
    instance = Instance.new(name: :journal_res_pg, repo: Continuum.Test.Repo)
    assert Instance.journal(instance) == Journal.Postgres
  end

  test "a named instance without a repo defaults to the in-memory journal" do
    instance = Instance.new(name: :journal_res_mem, repo: nil)
    assert Instance.journal(instance) == Journal.InMemory
  end

  test "an explicit journal: option overrides the named-instance default" do
    instance =
      Instance.new(
        name: :journal_res_override,
        repo: Continuum.Test.Repo,
        journal: Journal.InMemory
      )

    assert Instance.journal(instance) == Journal.InMemory
  end

  defp restore_env(key, nil), do: Application.delete_env(:continuum, key)
  defp restore_env(key, value), do: Application.put_env(:continuum, key, value)
end
