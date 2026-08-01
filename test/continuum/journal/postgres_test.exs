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

  defmodule SomeWorkflow do
    @moduledoc false

    def __continuum_workflow__ do
      %{
        module: __MODULE__,
        entrypoint: __MODULE__,
        version: 1,
        version_hash: :crypto.hash(:sha256, "test")
      }
    end
  end

  defmodule RetainedWorkflow do
    @moduledoc false
    use Continuum.Workflow, retention: {:milliseconds, 5_000}

    def run(input), do: input
  end

  defmodule InfiniteWorkflow do
    @moduledoc false
    use Continuum.Workflow, retention: :infinity

    def run(input), do: input
  end

  describe "start_run/3 and get_run/1" do
    test "creates a run row and retrieves it" do
      run_id = generate_uuid()

      :ok =
        Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{
          foo: :bar
        })

      run = Postgres.get_run(Continuum.Runtime.Instance.default(), run_id)
      assert run.state == :running
      assert run.result == nil
      assert run.error == nil
      assert run.input == %{foo: :bar}
    end

    test "returns nil for unknown run_id" do
      assert Postgres.get_run(Continuum.Runtime.Instance.default(), generate_uuid()) == nil
    end

    test "rejects node-local identities in workflow input before insert" do
      run_id = generate_uuid()

      assert_raise Continuum.DurableTermError, ~r/input.customer.owner/, fn ->
        Postgres.start_run(
          Continuum.Runtime.Instance.default(),
          run_id,
          SomeWorkflow,
          %{customer: %{owner: self()}}
        )
      end

      assert Repo.get(Run, run_id) == nil
    end

    test "stores trace_context as opaque binary" do
      run_id = generate_uuid()
      trace_context = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

      :ok =
        Postgres.start_run(
          Continuum.Runtime.Instance.default(),
          run_id,
          SomeWorkflow,
          %{foo: :bar},
          trace_context: trace_context
        )

      raw_trace_context =
        Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.trace_context))

      assert raw_trace_context == trace_context

      assert Postgres.get_run(Continuum.Runtime.Instance.default(), run_id).trace_context ==
               trace_context
    end

    test "stores search attributes as JSONB metadata" do
      run_id = generate_uuid()

      :ok =
        Postgres.start_run(
          Continuum.Runtime.Instance.default(),
          run_id,
          SomeWorkflow,
          %{foo: :bar},
          attributes: %{region: "eu", customer_tier: 3}
        )

      run = Postgres.get_run(Continuum.Runtime.Instance.default(), run_id)
      assert run.attributes == %{"region" => "eu", "customer_tier" => 3}
    end
  end

  describe "append!/3 and load/1" do
    test "appends and loads side_effect events" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      event = %{type: :side_effect, kind: :now, payload: ~U[2026-01-01 00:00:00Z], seq: 0}
      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)

      [loaded] = Postgres.load(Continuum.Runtime.Instance.default(), run_id)
      assert loaded.type == :side_effect
      assert loaded.kind == :now
      assert loaded.payload == ~U[2026-01-01 00:00:00Z]
      assert loaded.seq == 0
    end

    test "rejects node-local identities in event payloads before append" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      event = %{type: :side_effect, kind: :user, payload: %{owner: self()}, seq: 0}

      assert_raise Continuum.DurableTermError, ~r/event.payload.owner/, fn ->
        Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)
      end

      assert Postgres.load(Continuum.Runtime.Instance.default(), run_id) == []
    end

    test "appends and loads activity_completed events" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      event = %{
        type: :activity_completed,
        mfa: {MyApp.Worker, :run, [1, 2]},
        payload: {:ok, 42},
        seq: 0
      }

      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)

      [loaded] = Postgres.load(Continuum.Runtime.Instance.default(), run_id)
      assert loaded.type == :activity_completed
      assert loaded.mfa == {MyApp.Worker, :run, [1, 2]}
      assert loaded.payload == {:ok, 42}
    end

    test "appends and loads signal_received events" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      event = %{type: :signal_received, name: :approved, payload: :go, seq: 0}
      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)

      [loaded] = Postgres.load(Continuum.Runtime.Instance.default(), run_id)
      assert loaded.type == :signal_received
      assert loaded.name == :approved
      assert loaded.payload == :go
    end

    test "appends and loads timer_fired events" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      event = %{type: :timer_fired, duration_ms: 5000, seq: 0}
      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)

      [loaded] = Postgres.load(Continuum.Runtime.Instance.default(), run_id)
      assert loaded.type == :timer_fired
      assert loaded.duration_ms == 5000
    end

    test "loads events in seq order" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      e0 = %{type: :side_effect, kind: :now, payload: 1, seq: 0}
      e1 = %{type: :side_effect, kind: :uuid4, payload: "abc", seq: 1}

      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, e0, nil)
      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, e1, nil)

      [l0, l1] = Postgres.load(Continuum.Runtime.Instance.default(), run_id)
      assert l0.seq == 0
      assert l1.seq == 1
    end

    test "load returns empty list for unknown run" do
      assert Postgres.load(Continuum.Runtime.Instance.default(), generate_uuid()) == []
    end
  end

  describe "complete!/3 and fail!/3" do
    test "marks a run as completed" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})
      :ok = Postgres.complete!(Continuum.Runtime.Instance.default(), run_id, {:ok, 99}, nil)

      run = Postgres.get_run(Continuum.Runtime.Instance.default(), run_id)
      assert run.state == :completed
      assert run.result == {:ok, 99}
    end

    test "marks a run as failed" do
      run_id = generate_uuid()
      stacktrace = [{SomeWorkflow, :run, 1, [file: ~c"workflow.ex", line: 12]}]
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      :ok =
        Postgres.fail!(
          Continuum.Runtime.Instance.default(),
          run_id,
          {:exit, :boom, stacktrace},
          nil
        )

      run = Postgres.get_run(Continuum.Runtime.Instance.default(), run_id)
      assert run.state == :failed
      assert run.error == %Continuum.RunFailure{kind: :exit, reason: :boom}
      assert run.error_stacktrace == stacktrace

      assert {:ok,
              %{
                error: %Continuum.RunFailure{kind: :exit, reason: :boom},
                error_stacktrace: ^stacktrace
              }} = Continuum.get_run(run_id)
    end

    test "sets terminal-relative retention for completed and failed runs" do
      for terminal <- [:complete, :fail] do
        run_id = generate_uuid()

        :ok =
          Postgres.start_run(
            Continuum.Runtime.Instance.default(),
            run_id,
            RetainedWorkflow,
            %{}
          )

        Repo.update_all(from(r in Run, where: r.id == ^run_id),
          set: [started_at: ~U[2000-01-01 00:00:00Z]]
        )

        case terminal do
          :complete ->
            :ok = Postgres.complete!(Continuum.Runtime.Instance.default(), run_id, :ok, nil)

          :fail ->
            :ok = Postgres.fail!(Continuum.Runtime.Instance.default(), run_id, :boom, nil)
        end

        run = Repo.get!(Run, run_id)
        assert DateTime.diff(run.retention_until, run.completed_at, :millisecond) in 4_000..6_000
        assert DateTime.compare(run.retention_until, DateTime.utc_now()) == :gt
      end
    end

    test "leaves retention disabled for infinity" do
      run_id = generate_uuid()

      :ok =
        Postgres.start_run(
          Continuum.Runtime.Instance.default(),
          run_id,
          InfiniteWorkflow,
          %{}
        )

      :ok = Postgres.complete!(Continuum.Runtime.Instance.default(), run_id, :ok, nil)

      assert Repo.get!(Run, run_id).retention_until == nil
    end

    test "sets retention when a run is cancelled" do
      run_id = generate_uuid()

      :ok =
        Postgres.start_run(
          Continuum.Runtime.Instance.default(),
          run_id,
          RetainedWorkflow,
          %{}
        )

      :ok = Postgres.cancel_run!(Continuum.Runtime.Instance.default(), run_id, nil)

      run = Repo.get!(Run, run_id)
      assert run.state == "cancelled"
      assert DateTime.diff(run.retention_until, run.completed_at, :millisecond) in 4_000..6_000
    end
  end

  describe "CAS / lease token enforcement" do
    test "append! with matching lease_token succeeds" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      event = %{type: :side_effect, kind: :now, payload: 1, seq: 0}
      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, 42)
      assert length(Postgres.load(Continuum.Runtime.Instance.default(), run_id)) == 1
    end

    test "append! with mismatched lease_token raises" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      event = %{type: :side_effect, kind: :now, payload: 1, seq: 0}

      assert_raise Continuum.Runtime.JournalError, ~r/lease_mismatch/, fn ->
        Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, 99)
      end
    end

    test "complete! with mismatched lease_token raises" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      assert_raise Continuum.Runtime.JournalError, ~r/cas_failed/, fn ->
        Postgres.complete!(Continuum.Runtime.Instance.default(), run_id, {:ok, 1}, 99)
      end
    end

    test "complete! with nil lease_token raises for leased runs" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      assert_raise Continuum.Runtime.JournalError, ~r/cas_failed/, fn ->
        Postgres.complete!(Continuum.Runtime.Instance.default(), run_id, {:ok, 1}, nil)
      end
    end

    test "fail! cannot flip a run that already completed" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      :ok = Postgres.complete!(Continuum.Runtime.Instance.default(), run_id, {:ok, 1}, 42)

      # A late raise with a still-matching token must not turn a completed
      # run into a failed one.
      assert_raise Continuum.Runtime.JournalError, ~r/cas_failed/, fn ->
        Postgres.fail!(Continuum.Runtime.Instance.default(), run_id, :late_boom, 42)
      end

      run = Repo.get!(Continuum.Schema.Run, run_id)
      assert run.state == "completed"
      assert run.error == nil
    end

    test "append! with nil lease_token succeeds for unleased runs" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      event = %{type: :side_effect, kind: :now, payload: 1, seq: 0}
      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)
      assert length(Postgres.load(Continuum.Runtime.Instance.default(), run_id)) == 1
    end

    test "append! with nil lease_token raises for leased runs" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      Repo.update_all(
        from(r in Continuum.Schema.Run, where: r.id == ^run_id),
        set: [lease_token: 42, lease_owner: "node-1"]
      )

      event = %{type: :side_effect, kind: :now, payload: 1, seq: 0}

      assert_raise Continuum.Runtime.JournalError, ~r/lease_mismatch/, fn ->
        Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)
      end
    end
  end

  describe "encoding fidelity" do
    test "stores opaque terms as bytea, not JSON wrappers" do
      run_id = generate_uuid()

      :ok =
        Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{
          foo: :bar
        })

      event = %{type: :side_effect, kind: :user, payload: {:ok, 42}, seq: 0}
      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)

      raw_input = Repo.one!(from(r in Run, where: r.id == ^run_id, select: r.input))
      raw_payload = Repo.one!(from(e in Event, where: e.run_id == ^run_id, select: e.payload))

      assert is_binary(raw_input)
      assert is_binary(raw_payload)
      assert :erlang.binary_to_term(raw_input) == %{foo: :bar}
      assert :erlang.binary_to_term(raw_payload).payload == {:ok, 42}
    end

    test "round-trips atom values through encode/decode" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      event = %{type: :side_effect, kind: :user, payload: :some_atom, seq: 0}
      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)

      [loaded] = Postgres.load(Continuum.Runtime.Instance.default(), run_id)
      assert loaded.payload == :some_atom
    end

    test "round-trips complex terms (tuples, maps, lists)" do
      run_id = generate_uuid()
      :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

      complex = {:ok, %{id: 1, items: [1, 2, 3], nested: %{a: :b}}}
      event = %{type: :side_effect, kind: :user, payload: complex, seq: 0}
      :ok = Postgres.append!(Continuum.Runtime.Instance.default(), run_id, event, nil)

      [loaded] = Postgres.load(Continuum.Runtime.Instance.default(), run_id)
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

  defmodule SomeWorkflow do
    @moduledoc false

    def __continuum_workflow__ do
      %{
        module: __MODULE__,
        entrypoint: __MODULE__,
        version: 1,
        version_hash: :crypto.hash(:sha256, "concurrency-test")
      }
    end
  end

  test "implicit sequence numbers serialize concurrent appends" do
    run_id = Ecto.UUID.generate()
    :ok = Postgres.start_run(Continuum.Runtime.Instance.default(), run_id, SomeWorkflow, %{})

    1..20
    |> Task.async_stream(
      fn n ->
        Postgres.append!(
          Continuum.Runtime.Instance.default(),
          run_id,
          %{type: :side_effect, kind: :user, payload: n, seq: nil},
          nil
        )
      end,
      max_concurrency: 8,
      timeout: 5_000
    )
    |> Enum.each(fn result -> assert result == {:ok, :ok} end)

    seqs =
      Continuum.Runtime.Instance.default()
      |> Postgres.load(run_id)
      |> Enum.map(& &1.seq)

    assert seqs == Enum.to_list(0..19)
  end
end
