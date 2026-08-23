defmodule Continuum.ActivityStubTest do
  use ExUnit.Case, async: false

  alias Continuum.Runtime.Journal.InMemory
  alias Continuum.Test

  defmodule Payments do
    use Continuum.Activity

    def charge(order), do: raise("the real charger reached the network for #{inspect(order)}")
    def refund(charge_id), do: raise("the real refunder reached the network for #{charge_id}")
  end

  defmodule Shipping do
    use Continuum.Activity

    def book(order), do: raise("the real booker reached the network for #{inspect(order)}")
  end

  defmodule Checkout do
    use Continuum.Workflow, version: 1

    def run(order) do
      {:ok, charge} =
        activity(Payments.charge(order), compensate: {Payments, :refund, [order.id]})

      case activity(Shipping.book(order), compensate: :none) do
        {:ok, tracking} ->
          {:ok, %{charge: charge.result, tracking: tracking}}

        {:error, reason} ->
          compensate_all()
          {:error, reason}
      end
    end
  end

  setup do
    Test.reset_in_memory!()
    :ok
  end

  test "stubs stand in for activity bodies" do
    {:ok, run_id} =
      Test.start_synchronous(Checkout, %{id: "o-1"},
        activities: %{
          {Payments, :charge} => fn order -> {:ok, "ch_" <> order.id} end,
          {Shipping, :book} => {:ok, "trk_1"}
        }
      )

    assert {:ok, %{state: :completed, result: result}} = Continuum.await(run_id, 1_000)
    assert result == {:ok, %{charge: "ch_o-1", tracking: "trk_1"}}
  end

  test "stubs drive the compensation branch" do
    refunded = self()

    {:ok, run_id} =
      Test.start_synchronous(Checkout, %{id: "o-2"},
        activities: %{
          {Payments, :charge} => {:ok, "ch_o-2"},
          {Payments, :refund} => fn id -> send(refunded, {:refunded, id}) && :refunded end,
          {Shipping, :book} => {:error, :out_of_stock}
        }
      )

    assert {:ok, %{state: :completed, result: {:error, :out_of_stock}}} =
             Continuum.await(run_id, 1_000)

    assert_received {:refunded, "o-2"}
  end

  test "a stubbed run journals the same command ids as a real one" do
    {:ok, stubbed} =
      Test.start_synchronous(Checkout, %{id: "o-3"},
        activities: %{
          {Payments, :charge} => {:ok, "ch"},
          {Shipping, :book} => {:ok, "trk"}
        }
      )

    {:ok, _} = Continuum.await(stubbed, 1_000)

    {:ok, real} =
      Test.start_synchronous(Checkout, %{id: "o-3"},
        activities: %{
          {Payments, :charge} => {:ok, "different"},
          {Shipping, :book} => {:ok, "also different"}
        }
      )

    {:ok, _} = Continuum.await(real, 1_000)

    assert command_ids(stubbed) == command_ids(real)
  end

  test "an arity-keyed stub wins over the bare name" do
    {:ok, run_id} =
      Test.start_synchronous(Checkout, %{id: "o-4"},
        activities: %{
          {Payments, :charge} => {:ok, "bare"},
          {Payments, :charge, 1} => {:ok, "specific"},
          {Shipping, :book} => {:ok, "trk"}
        }
      )

    assert {:ok, %{result: {:ok, %{charge: "specific"}}}} = Continuum.await(run_id, 1_000)
  end

  test "a stub returning a non-durable term is rejected the way production would" do
    {:ok, run_id} =
      Test.start_synchronous(Checkout, %{id: "o-5"},
        activities: %{{Payments, :charge} => fn _order -> self() end}
      )

    assert {:error,
            %{
              state: :failed,
              error: %Continuum.RunFailure{
                reason: %Continuum.ActivityStubError{message: message}
              }
            }} = Continuum.await(run_id, 1_000)

    assert message =~ "the journal would reject"
    assert message =~ "non-durable PID"
  end

  test "a stub whose arity does not match the call is a loud error" do
    {:ok, run_id} =
      Test.start_synchronous(Checkout, %{id: "o-6"},
        activities: %{{Payments, :charge} => fn -> {:ok, "no args"} end}
      )

    assert {:error,
            %{
              state: :failed,
              error: %Continuum.RunFailure{
                reason: %Continuum.ActivityStubError{message: message}
              }
            }} = Continuum.await(run_id, 1_000)

    assert message =~ "activity stub takes 0 argument(s)"
  end

  test "stubs are refused on the Postgres journal" do
    assert_raise ArgumentError, ~r/only supported on the in-memory journal/, fn ->
      Continuum.Runtime.ActivityStubs.validate!(
        %{{Payments, :charge} => :ok},
        Continuum.Runtime.Journal.Postgres
      )
    end
  end

  test "an invalid stub key names itself" do
    assert_raise ArgumentError, ~r/invalid activity stub key/, fn ->
      Continuum.Runtime.ActivityStubs.validate!(%{"Payments.charge" => :ok}, InMemory)
    end
  end

  defp command_ids(run_id) do
    run_id
    |> Test.history(journal: InMemory)
    |> Enum.map(&Map.get(&1, :command_id))
  end
end
