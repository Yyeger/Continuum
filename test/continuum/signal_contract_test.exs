defmodule Continuum.SignalContractTest do
  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run, Signal}

  defmodule Validators do
    def positive_integer?(value), do: is_integer(value) and value > 0
  end

  defmodule ContractFlow do
    use Continuum.Workflow,
      version: 1,
      signals: [approved: :map, score: {Validators, :positive_integer?}]

    def run(input) do
      await(signal(input.signal))
    end
  end

  setup do
    Repo.delete_all(Signal)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "rejects undeclared names and invalid payloads before mailbox writes" do
    {:ok, run_id} =
      Continuum.start(ContractFlow, %{signal: :score}, journal: Postgres)

    assert_eventually(fn -> Repo.get!(Run, run_id).state == "suspended" end)

    assert {:error, {:undeclared_signal, :scroe}} =
             Continuum.signal(run_id, :scroe, 10, journal: Postgres)

    assert {:error, {:invalid_signal_payload, :score, :validation_failed}} =
             Continuum.signal(run_id, :score, -1, journal: Postgres)

    assert Repo.aggregate(Signal, :count) == 0

    assert :ok = Continuum.signal(run_id, :score, 10, journal: Postgres)

    assert {:ok, %{state: :completed, result: 10}} =
             Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "exposes versioned contracts to Observer" do
    {:ok, run_id} =
      Continuum.start(ContractFlow, %{signal: :approved}, journal: Postgres)

    assert {:ok, contracts} = Continuum.Observer.signal_contracts(run_id)
    assert contracts.approved == :map
    assert contracts.score == {Validators, :positive_integer?}

    metadata = ContractFlow.__continuum_entrypoint__().__continuum_workflow__()
    assert metadata.signals == contracts

    assert :ok = Continuum.signal(run_id, :approved, %{}, journal: Postgres)
    assert {:ok, %{state: :completed}} = Continuum.await(run_id, 1_000, journal: Postgres)
  end

  test "compile-checks literal signal awaits" do
    suffix = System.unique_integer([:positive])

    source = """
    defmodule Continuum.UndeclaredSignal#{suffix} do
      use Continuum.Workflow, signals: [approved: :map]
      def run(_input), do: await(signal(:apprvoed))
    end
    """

    assert_raise CompileError, ~r/undeclared signal :apprvoed/, fn ->
      Code.compile_string(source, "undeclared_signal_#{suffix}.ex")
    end
  end

  test "workflows without declarations retain open delivery" do
    assert :ok = Continuum.SignalContract.validate(nil, :anything, self())
  end

  defp assert_eventually(fun, attempts \\ 100)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
