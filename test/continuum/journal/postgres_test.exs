defmodule Continuum.Journal.PostgresTest do
  @moduledoc """
  Tests for the Postgres journal adapter, covering:

    * Basic CRUD (start_run, append!, load, complete!, fail!, get_run)
    * CAS enforcement on lease_token
    * Event encoding/decoding round-trip fidelity
  """

  use Continuum.Test.DataCase, async: true

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run}

  describe "start_run/3 and get_run/1" do
    test "creates a run row and retrieves it" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{foo: :bar})

      run = Postgres.get_run(run_id)
      assert run.state == :running
      assert run.result == nil
      assert run.error == nil
      assert run.input == %{foo: :bar}
    end

    test "returns nil for unknown run_id" do
      assert Postgres.get_run(generate_uuid()) == nil
    end
  end

  describe "append!/3 and load/1" do
    test "appends and loads side_effect events" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      event = %{type: :side_effect, kind: :now, payload: ~U[2026-01-01 00:00:00Z], seq: 0}
      :ok = Postgres.append!(run_id, event, nil)

      [loaded] = Postgres.load(run_id)
      assert loaded.type == :side_effect
      assert loaded.kind == :now
      assert loaded.payload == ~U[2026-01-01 00:00:00Z]
      assert loaded.seq == 0
    end

    test "appends and loads activity_completed events" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      event = %{
        type: :activity_completed,
        mfa: {MyApp.Worker, :run, [1, 2]},
        payload: {:ok, 42},
        seq: 0
      }

      :ok = Postgres.append!(run_id, event, nil)

      [loaded] = Postgres.load(run_id)
      assert loaded.type == :activity_completed
      assert loaded.mfa == {MyApp.Worker, :run, [1, 2]}
      assert loaded.payload == {:ok, 42}
    end

    test "appends and loads signal_received events" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      event = %{type: :signal_received, name: :approved, payload: :go, seq: 0}
      :ok = Postgres.append!(run_id, event, nil)

      [loaded] = Postgres.load(run_id)
      assert loaded.type == :signal_received
      assert loaded.name == :approved
      assert loaded.payload == :go
    end

    test "appends and loads timer_fired events" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      event = %{type: :timer_fired, duration_ms: 5000, seq: 0}
      :ok = Postgres.append!(run_id, event, nil)

      [loaded] = Postgres.load(run_id)
      assert loaded.type == :timer_fired
      assert loaded.duration_ms == 5000
    end

    test "loads events in seq order" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      e0 = %{type: :side_effect, kind: :now, payload: 1, seq: 0}
      e1 = %{type: :side_effect, kind: :uuid4, payload: "abc", seq: 1}

      :ok = Postgres.append!(run_id, e0, nil)
      :ok = Postgres.append!(run_id, e1, nil)

      [l0, l1] = Postgres.load(run_id)
      assert l0.seq == 0
      assert l1.seq == 1
    end

    test "load returns empty list for unknown run" do
      assert Postgres.load(generate_uuid()) == []
    end
  end

  describe "complete!/3 and fail!/3" do
    test "marks a run as completed" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})
      :ok = Postgres.complete!(run_id, {:ok, 99}, nil)

      run = Postgres.get_run(run_id)
      assert run.state == :completed
      assert run.result == {:ok, 99}
    end

    test "marks a run as failed" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})
      :ok = Postgres.fail!(run_id, {:exit, :boom}, nil)

      run = Postgres.get_run(run_id)
      assert run.state == :failed
      assert run.error == {:exit, :boom}
    end
  end

  describe "CAS / lease token enforcement" do
    test "append! with matching lease_token succeeds" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      event = %{type: :side_effect, kind: :now, payload: 1, seq: 0}
      :ok = Postgres.append!(run_id, event, 42)
      assert length(Postgres.load(run_id)) == 1
    end

    test "append! with mismatched lease_token raises" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      event = %{type: :side_effect, kind: :now, payload: 1, seq: 0}

      assert_raise RuntimeError, ~r/lease_mismatch/, fn ->
        Postgres.append!(run_id, event, 99)
      end
    end

    test "complete! with mismatched lease_token raises" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      assert_raise RuntimeError, ~r/CAS update failed/, fn ->
        Postgres.complete!(run_id, {:ok, 1}, 99)
      end
    end

    test "complete! with nil lease_token raises for leased runs" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      assert_raise RuntimeError, ~r/CAS update failed/, fn ->
        Postgres.complete!(run_id, {:ok, 1}, nil)
      end
    end

    test "append! with nil lease_token succeeds for unleased runs" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      event = %{type: :side_effect, kind: :now, payload: 1, seq: 0}
      :ok = Postgres.append!(run_id, event, nil)
      assert length(Postgres.load(run_id)) == 1
    end

    test "append! with nil lease_token raises for leased runs" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      event = %{type: :side_effect, kind: :now, payload: 1, seq: 0}

      assert_raise RuntimeError, ~r/lease_mismatch/, fn ->
        Postgres.append!(run_id, event, nil)
      end
    end
  end

  describe "encoding fidelity" do
    test "stores opaque terms as bytea, not JSON wrappers" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{foo: :bar})

      event = %{type: :side_effect, kind: :user, payload: {:ok, 42}, seq: 0}
      :ok = Postgres.append!(run_id, event, nil)

      raw_input = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.input))
      raw_payload = Repo.one!(from(e in Event, where: e.run_id == ^run_id, select: e.payload))

      assert is_binary(raw_input)
      assert is_binary(raw_payload)
      assert :erlang.binary_to_term(raw_input) == %{foo: :bar}
      assert :erlang.binary_to_term(raw_payload).payload == {:ok, 42}
    end

    test "round-trips atom values through encode/decode" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      event = %{type: :side_effect, kind: :user, payload: :some_atom, seq: 0}
      :ok = Postgres.append!(run_id, event, nil)

      [loaded] = Postgres.load(run_id)
      assert loaded.payload == :some_atom
    end

    test "round-trips complex terms (tuples, maps, lists)" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

      complex = {:ok, %{id: 1, items: [1, 2, 3], nested: %{a: :b}}}
      event = %{type: :side_effect, kind: :user, payload: complex, seq: 0}
      :ok = Postgres.append!(run_id, event, nil)

      [loaded] = Postgres.load(run_id)
      assert loaded.payload == complex
    end
  end

  defp generate_uuid do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)
    <<a::32, b::16, c::16, d::16, e::48>> = <<u0::48, 4::4, u1::12, 2::2, u2::62>>

    :io_lib.format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [a, b, c, d, e])
    |> IO.iodata_to_binary()
  end
end

defmodule Continuum.Journal.PostgresConcurrencyTest do
  @moduledoc false

  use Continuum.Test.DataCase, async: false

  alias Continuum.Runtime.Journal.Postgres

  test "implicit sequence numbers serialize concurrent appends" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(run_id, SomeWorkflow, %{})

    1..20
    |> Task.async_stream(
      fn n ->
        Postgres.append!(run_id, %{type: :side_effect, kind: :user, payload: n, seq: nil}, nil)
      end,
      max_concurrency: 8,
      timeout: 5_000
    )
    |> Enum.each(fn result -> assert result == {:ok, :ok} end)

    seqs =
      run_id
      |> Postgres.load()
      |> Enum.map(& &1.seq)

    assert seqs == Enum.to_list(0..19)
  end
end

defmodule SomeWorkflow do
  @moduledoc false

  def __continuum_workflow__ do
    %{version: 1, version_hash: :crypto.hash(:sha256, "test"), module: __MODULE__}
  end
end
