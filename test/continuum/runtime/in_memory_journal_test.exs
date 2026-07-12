defmodule Continuum.Runtime.InMemoryJournalTest do
  use ExUnit.Case, async: false

  alias Continuum.Runtime.{Instance, JournalError}
  alias Continuum.Runtime.Journal.InMemory

  defmodule Flow do
    use Continuum.Workflow, version: 1

    def run(_input), do: :ok
  end

  setup do
    Continuum.Test.reset_in_memory!()
    :ok
  end

  test "terminal runs reject later writes without changing state" do
    instance = Instance.default()
    run_id = Ecto.UUID.generate()

    assert :ok = InMemory.start_run(instance, run_id, Flow, %{})
    assert :ok = InMemory.complete!(instance, run_id, :done, nil)

    writes = [
      fn -> InMemory.append!(instance, run_id, %{type: :side_effect}, nil) end,
      fn -> InMemory.suspend!(instance, run_id, nil) end,
      fn -> InMemory.complete!(instance, run_id, :again, nil) end,
      fn -> InMemory.fail!(instance, run_id, :late_failure, nil) end
    ]

    Enum.each(writes, fn write ->
      assert %JournalError{reason: {:run_not_active, :completed}} =
               assert_raise(JournalError, write)
    end)

    assert %{state: :completed, result: :done, error: nil, events: []} =
             InMemory.get_run(instance, run_id)
  end

  test "writes to an unknown run raise without creating a phantom run" do
    instance = Instance.default()
    run_id = Ecto.UUID.generate()

    writes = [
      fn -> InMemory.append!(instance, run_id, %{type: :side_effect}, nil) end,
      fn -> InMemory.suspend!(instance, run_id, nil) end,
      fn -> InMemory.complete!(instance, run_id, :done, nil) end,
      fn -> InMemory.fail!(instance, run_id, :failure, nil) end
    ]

    Enum.each(writes, fn write ->
      assert %JournalError{reason: {:run_not_found, ^run_id}} =
               assert_raise(JournalError, write)
    end)

    assert InMemory.get_run(instance, run_id) == nil
  end
end
