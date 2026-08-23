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

  test "round-trips every value the validator accepts" do
    value = %{
      atom: :ok,
      tuple: {:ok, 1},
      list: [1, %{date: ~U[2026-07-14 08:00:00Z]}],
      improper: [1 | :tail],
      bits: <<1::1>>,
      module: Continuum.DurableTerm
    }

    assert value |> :erlang.term_to_binary() |> DurableTerm.decode!() == value
  end

  test "refuses to create an atom the node has never defined" do
    # Built by hand as SMALL_ATOM_UTF8_EXT so the name never appears as an atom
    # literal in this file, which would define it before the assertion runs.
    name = "continuum_atom_that_has_never_existed"
    unknown = <<131, 119, byte_size(name), name::binary>>

    assert_raise DurableTermError, ~r/never defined/, fn -> DurableTerm.decode!(unknown) end
    assert {:error, %DurableTermError{kind: :undecodable}} = DurableTerm.decode(unknown)
  end

  test "reports corrupt bytes as an undecodable term, not an ArgumentError" do
    assert {:error, %DurableTermError{kind: :undecodable}} =
             DurableTerm.decode(<<131, 99, 0, 0>>)
  end
end
