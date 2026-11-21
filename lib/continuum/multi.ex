defmodule Continuum.Multi do
  @moduledoc """
  Atomically enqueue workflows alongside application changes in `Ecto.Multi`.

  The transaction's repo must be the selected Continuum instance's repo, and
  the instance must use a durable journal (PostgreSQL or SQLite).
  Enqueueing only inserts durable work: the dispatcher acquires a lease and
  starts an engine after commit. No engine or activity starts in the caller's
  transaction, and no after-commit callback is needed for recovery.

  On SQLite, run the transaction with `mode: :immediate` (or configure the
  repo's `default_transaction_mode: :immediate`) so it holds the writer lock
  from its first statement.
  """

  alias Continuum.Runtime.{Instance, Journal}

  @doc """
  Adds a workflow enqueue operation to a Multi.

  `input` and `opts` may each be a value or a one-argument function of prior
  Multi changes. Options are `:instance`, `:run_id`, `:idempotency_key`,
  `:namespace`, `:attributes`, and `:trace_context`.

      Ecto.Multi.new()
      |> Ecto.Multi.insert(:order, changeset)
      |> Continuum.Multi.enqueue(:workflow, OrderFlow,
        fn %{order: order} -> %{order_id: order.id} end,
        fn %{order: order} -> [idempotency_key: "order:\#{order.id}"] end)
      |> Repo.transaction()

  The named change is `%{run_id: id, status: :enqueued | :existing}`. Reusing
  an idempotency key returns the original root ID, including after pruning;
  it does not replace its input or metadata. Other Multi operations still run
  on a duplicate, so give business writes their own uniqueness constraints.

  Any later Multi failure rolls back both the run and its ingress key. An
  unleased row is intentional here: only the dispatcher may acquire its first
  lease after commit. Keep a dispatcher enabled for this repo. An instance
  without a durable journal returns `{:error, :postgres_journal_required}`.
  """
  @spec enqueue(Ecto.Multi.t(), Ecto.Multi.name(), module(), term(), keyword() | function()) ::
          Ecto.Multi.t()
  def enqueue(multi, name, workflow, input, opts \\ []) do
    Ecto.Multi.run(multi, name, fn repo, changes ->
      opts =
        opts
        |> resolve(changes)
        |> Keyword.validate!([
          :instance,
          :run_id,
          :idempotency_key,
          :namespace,
          :attributes,
          :trace_context
        ])

      instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

      with :ok <- validate_instance(repo, instance) do
        Instance.journal(instance).enqueue_run(
          instance,
          Keyword.get_lazy(opts, :run_id, &Ecto.UUID.generate/0),
          workflow,
          resolve(input, changes),
          opts
        )
      end
    end)
  end

  @doc """
  Schedules a one-shot workflow in the same transaction as application writes.

  The named change is the schedule ID. Input, UTC due time, and options may
  each be a value or a one-argument function of earlier Multi changes.
  Accepts the same options as `Continuum.Schedules.schedule_at/4`. The runner
  can dispatch a due schedule once the transaction commits. A later Multi
  error rolls back the schedule. On SQLite use `mode: :immediate`.
  """
  @doc since: "0.9.3"
  @spec schedule_at(
          Ecto.Multi.t(),
          Ecto.Multi.name(),
          module(),
          term(),
          DateTime.t() | function(),
          keyword() | function()
        ) :: Ecto.Multi.t()
  def schedule_at(multi, name, workflow, input, due_at, opts \\ []) do
    Ecto.Multi.run(multi, name, fn repo, changes ->
      opts =
        opts
        |> resolve(changes)
        |> Keyword.validate!([:instance, :id, :run_id, :namespace, :attributes, :trace_context])

      instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

      with :ok <- validate_instance(repo, instance) do
        Continuum.Schedules.schedule_at(
          workflow,
          resolve(input, changes),
          resolve(due_at, changes),
          Keyword.put(opts, :instance, instance)
        )
      end
    end)
  end

  defp validate_instance(repo, instance) do
    cond do
      instance.repo != repo -> {:error, :repo_mismatch}
      not Journal.durable?(Instance.journal(instance)) -> {:error, :postgres_journal_required}
      true -> :ok
    end
  end

  defp resolve(value, changes) when is_function(value, 1), do: value.(changes)
  defp resolve(value, _changes), do: value
end
