defmodule Continuum.DurableTermTest do
  use ExUnit.Case, async: true

  alias Continuum.{DurableTerm, DurableTermError}

  test "accepts nested replay-safe ETF values" do
    value = %{
      atom: :ok,
      tuple: {:ok, 1},
      list: [1, %{date: ~U[2026-07-14 08:00:00Z]}],
      improper: [1 | :tail],
      bits: <<1::1>>
    }

    assert :ok = DurableTerm.validate(value, :input)
    assert DurableTerm.validate!(value, :input) == value
  end

  test "reports the path to a nested PID" do
    assert {:error, %DurableTermError{kind: :PID} = error} =
             DurableTerm.validate(%{customer: %{owners: [:primary, self()]}}, :input)

    assert Exception.message(error) =~ "non-durable PID at input.customer.owners[1]"
  end

  test "rejects references, ports, and functions" do
    port = Port.open({:spawn, "true"}, [])

    on_exit(fn ->
      if Port.info(port), do: Port.close(port)
    end)

    for {kind, value} <- [reference: make_ref(), port: port, function: fn -> :ok end] do
      assert {:error, %DurableTermError{kind: ^kind}} =
               DurableTerm.validate({:nested, value}, :effect)
    end
  end

  test "reports improper list tails" do
    assert_raise DurableTermError, ~r/signal.items.tail/, fn ->
      DurableTerm.validate!(%{items: [1 | self()]}, :signal)
    end
  end
end
