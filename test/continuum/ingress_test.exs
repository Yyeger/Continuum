defmodule Continuum.IngressTest do
  use Continuum.Test.DataCase, async: false

  import Ecto.Query

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.{InMemory, Postgres}
  alias Continuum.Schema.{Run, Signal}

  defmodule WaitingFlow do
    use Continuum.Workflow, version: 1

    def run(_input), do: await(signal(:continue))
  end

  defmodule ContinuingFlow do
    use Continuum.Workflow, version: 1

    def run(%{stage: :root}), do: continue_as_new(%{stage: :tail})
    def run(%{stage: :tail}), do: await(signal(:continue))
  end

  setup do
    previous_journal = Application.get_env(:continuum, :journal)

    on_exit(fn ->
      if is_nil(previous_journal) do
        Application.delete_env(:continuum, :journal)
      else
        Application.put_env(:continuum, :journal, previous_journal)
      end

      InMemory.reset()
    end)

    :ok
  end

  test "start uniqueness is atomic across concurrent Postgres callers" do
    key = "order-created:42"

    results =
      1..8
      |> Task.async_stream(
        fn _index ->
          Postgres.start_run(Instance.default(), Ecto.UUID.generate(), WaitingFlow, %{},
            namespace: "orders",
            idempotency_key: key
          )
        end,
        max_concurrency: 8,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == :ok)) == 1

    existing_id =
      Repo.one!(
        from(r in Run,
          where:
            r.namespace == "orders" and r.workflow == ^inspect(WaitingFlow) and
              r.idempotency_key == ^key,
          select: r.id
        )
      )

    assert Enum.count(results, &(&1 == {:error, {:already_started, existing_id}})) == 7
  end

  test "start_unique returns the existing in-memory run" do
    Application.put_env(:continuum, :journal, InMemory)

    assert {:ok, run_id, :started} =
             Continuum.start_unique(WaitingFlow, %{}, idempotency_key: "request-1")

    assert {:ok, ^run_id, :existing} =
             Continuum.start_unique(WaitingFlow, %{ignored: true}, idempotency_key: "request-1")

    assert {:error, :idempotency_key_required} = Continuum.start_unique(WaitingFlow, %{}, [])

    :ok = Continuum.signal(run_id, :continue, :done)
    assert {:ok, %{state: :completed, result: :done}} = Continuum.await(run_id)
  end

  test "signal delivery IDs create one durable mailbox entry" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Instance.default(), run_id, WaitingFlow, %{})

    assert {:ok, ^run_id, :delivered} =
             Postgres.deliver_signal!(Instance.default(), run_id, :continue, :go,
               delivery_id: "message-7"
             )

    assert {:ok, ^run_id, :duplicate} =
             Postgres.deliver_signal!(Instance.default(), run_id, :continue, :go,
               delivery_id: "message-7"
             )

    assert Repo.aggregate(from(s in Signal, where: s.run_id == ^run_id), :count) == 1
  end

  test "delivery without an ID always inserts a mailbox row per signal" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Instance.default(), run_id, WaitingFlow, %{})

    # The conflict target is scoped to the delivery-ID index, so repeated
    # keyless deliveries are distinct signals and each one must land. Reporting
    # `:delivered` for a row that was never inserted would lose the signal.
    for _ <- 1..3 do
      assert {:ok, ^run_id, :delivered} =
               Postgres.deliver_signal!(Instance.default(), run_id, :continue, :go, [])
    end

    assert Repo.aggregate(from(s in Signal, where: s.run_id == ^run_id), :count) == 3
  end

  test "signal_unique reports duplicate delivery without a second in-memory payload" do
    Application.put_env(:continuum, :journal, InMemory)
    run_id = Ecto.UUID.generate()
    :ok = InMemory.start_run(Instance.default(), run_id, WaitingFlow, %{})

    assert {:ok, :delivered} =
             Continuum.signal_unique(run_id, :continue, :go, "message-8")

    assert {:ok, :duplicate} =
             Continuum.signal_unique(run_id, :continue, :go, "message-8")

    assert {:ok, :go} = InMemory.consume_buffered_signal!(Instance.default(), run_id, :continue)
    assert :none = InMemory.consume_buffered_signal!(Instance.default(), run_id, :continue)
  end

  test "signal delivery IDs are scoped across a continued chain" do
    {:ok, root_id} =
      Continuum.Runtime.Engine.start_run(ContinuingFlow, %{stage: :root}, journal: Postgres)

    tail_id =
      eventually(fn ->
        Repo.one(from(r in Run, where: r.continued_from_run_id == ^root_id, select: r.id))
      end)

    assert {:ok, :delivered} =
             Continuum.signal_unique(root_id, :continue, :go, "message-chain", journal: Postgres)

    assert {:ok, :duplicate} =
             Continuum.signal_unique(root_id, :continue, :go, "message-chain", journal: Postgres)

    assert Repo.aggregate(from(s in Signal, where: s.run_id == ^tail_id), :count) == 1

    assert Repo.one!(from(s in Signal, where: s.run_id == ^tail_id, select: s.correlation_id)) ==
             root_id
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    case fun.() do
      nil ->
        Process.sleep(10)
        eventually(fun, attempts - 1)

      value ->
        value
    end
  end

  defp eventually(_fun, 0), do: flunk("condition did not become true")
end
