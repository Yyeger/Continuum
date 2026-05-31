defmodule Continuum.OpenTelemetryTest do
  use ExUnit.Case, async: false

  defmodule FakeTracer do
    def start_span(name, attributes, links, test_pid) do
      span = {name, make_ref()}
      send(test_pid, {:span_started, span, name, attributes, links})
      span
    end

    def end_span(span, status, attributes, test_pid) do
      send(test_pid, {:span_ended, span, status, attributes})
      :ok
    end

    def add_event(span, name, attributes, test_pid) do
      send(test_pid, {:span_event, span, name, attributes})
      :ok
    end

    def link(span, attributes, _test_pid), do: [%{span: span, attributes: attributes}]

    def traceparent_links(traceparent, _test_pid) do
      [%{traceparent: traceparent, attributes: %{kind: "original_trace_context"}}]
    end
  end

  defmodule FailingStartTracer do
    def start_span(name, _attributes, _links, test_pid) do
      send(test_pid, {:start_attempted, name})
      raise "boom"
    end

    def end_span(_span, _status, _attributes, _test_pid), do: :ok
    def add_event(_span, _name, _attributes, _test_pid), do: :ok
  end

  defmodule InlineActivity do
    def run(value), do: value + 1
  end

  defmodule ActivityFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(InlineActivity.run(input.value))
    end
  end

  setup do
    handler_id = {__MODULE__, self(), make_ref()}

    {:ok, ^handler_id} =
      Continuum.OpenTelemetry.setup(handler_id: handler_id, tracer: {FakeTracer, self()})

    on_exit(fn -> Continuum.OpenTelemetry.detach(handler_id) end)

    :ok
  end

  test "creates run-attempt and inline activity-attempt spans" do
    Continuum.Test.reset_in_memory!()
    {:ok, run_id} = Continuum.Test.start_synchronous(ActivityFlow, %{value: 41})

    assert {:ok, %{state: :completed, result: 42}} = Continuum.await(run_id, 1_000)

    assert_receive {:span_started, run_span, "continuum.run_attempt", run_attrs, []}
    assert run_attrs["continuum.run_id"] == run_id
    assert run_attrs["continuum.workflow"] =~ "ActivityFlow"

    # FakeTracer exposes explicit links so tests can observe the relationship.
    # Real OTel uses the active in-process span context instead.
    assert_receive {:span_started, activity_span, "continuum.activity_attempt", activity_attrs,
                    [%{span: ^run_span}]}

    assert activity_attrs["continuum.run_id"] == run_id
    assert activity_attrs["continuum.activity_module"] =~ "InlineActivity"
    assert activity_attrs["continuum.activity_function"] == "run"
    assert activity_attrs["continuum.activity_arity"] == 1

    assert_receive {:span_ended, ^activity_span, :unset,
                    %{"continuum.terminal_event" => "activity.completed"}}

    assert_receive {:span_ended, ^run_span, :unset,
                    %{"continuum.terminal_event" => "run.completed"}}
  end

  test "start-span failures do not detach the telemetry handler" do
    handler_id = {__MODULE__, self(), make_ref()}

    {:ok, ^handler_id} =
      Continuum.OpenTelemetry.setup(handler_id: handler_id, tracer: {FailingStartTracer, self()})

    try do
      run_id = Ecto.UUID.generate()

      Continuum.Telemetry.execute([:continuum, :run, :started], %{}, %{
        instance: Continuum,
        run_id: run_id,
        workflow: ActivityFlow,
        lease_token: 123
      })

      Continuum.Telemetry.execute([:continuum, :run, :completed], %{}, %{
        instance: Continuum,
        run_id: run_id
      })

      assert_receive {:start_attempted, "continuum.run_attempt"}

      Continuum.Telemetry.execute([:continuum, :activity, :started], %{}, %{
        run_id: run_id,
        task_id: Ecto.UUID.generate(),
        mfa: {InlineActivity, :run, [1]},
        attempt: 1
      })

      assert_receive {:start_attempted, "continuum.activity_attempt"}
    after
      Continuum.OpenTelemetry.detach(handler_id)
    end
  end

  test "cancelled runs end without error status" do
    run_id = Ecto.UUID.generate()

    Continuum.Telemetry.execute([:continuum, :run, :started], %{}, %{
      instance: Continuum,
      run_id: run_id,
      workflow: ActivityFlow,
      lease_token: 123
    })

    assert_receive {:span_started, run_span, "continuum.run_attempt", _attrs, []}

    Continuum.Telemetry.execute([:continuum, :run, :cancelled], %{}, %{
      instance: Continuum,
      run_id: run_id
    })

    assert_receive {:span_ended, ^run_span, :unset,
                    %{"continuum.terminal_event" => "run.cancelled"}}
  end

  test "links resumed run attempts to persisted trace context" do
    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    run_id = Ecto.UUID.generate()

    Continuum.Telemetry.execute([:continuum, :run, :started], %{}, %{
      instance: Continuum,
      run_id: run_id,
      workflow: ActivityFlow,
      lease_token: 123,
      resumed?: true,
      trace_context: traceparent
    })

    assert_receive {:span_started, run_span, "continuum.run_attempt", attrs, links}
    assert attrs["continuum.run_id"] == run_id
    assert attrs["continuum.resumed"] == true
    assert [%{traceparent: ^traceparent}] = links

    Continuum.Telemetry.execute([:continuum, :run, :suspended], %{}, %{
      instance: Continuum,
      run_id: run_id,
      reason: {:activity_pending, "task"}
    })

    assert_receive {:span_event, ^run_span, "run.suspended", event_attrs}
    assert event_attrs["continuum.run_id"] == run_id
    assert event_attrs["continuum.reason"] == "{:activity_pending, \"task\"}"

    assert_receive {:span_ended, ^run_span, :unset,
                    %{"continuum.terminal_event" => "run.suspended"}}
  end

  test "creates and closes a compensation-attempt span" do
    run_id = Ecto.UUID.generate()
    task_id = Ecto.UUID.generate()

    Continuum.Telemetry.execute([:continuum, :compensation, :started], %{}, %{
      run_id: run_id,
      task_id: task_id,
      target_activity_id: {:cmd, 0},
      attempt: 1
    })

    assert_receive {:span_started, comp_span, "continuum.compensation_attempt", attrs, []}
    assert attrs["continuum.run_id"] == run_id

    Continuum.Telemetry.execute([:continuum, :compensation, :failed], %{duration_ms: 5}, %{
      run_id: run_id,
      task_id: task_id,
      target_activity_id: {:cmd, 0},
      error: :boom
    })

    assert_receive {:span_ended, ^comp_span, :error,
                    %{"continuum.terminal_event" => "compensation.failed"}}
  end

  test "records child and continue_as_new breadcrumbs on the run-attempt span" do
    run_id = Ecto.UUID.generate()

    Continuum.Telemetry.execute([:continuum, :run, :started], %{}, %{
      instance: Continuum,
      run_id: run_id,
      workflow: ActivityFlow,
      lease_token: 1
    })

    assert_receive {:span_started, run_span, "continuum.run_attempt", _attrs, []}

    Continuum.Telemetry.execute([:continuum, :child, :started], %{}, %{
      parent_run_id: run_id,
      child_run_id: "child-1",
      workflow: ActivityFlow
    })

    assert_receive {:span_event, ^run_span, "child.started", child_attrs}
    assert child_attrs["continuum.child_run_id"] == "child-1"

    Continuum.Telemetry.execute([:continuum, :run, :continued_as_new], %{}, %{
      from_run_id: run_id,
      to_run_id: "next-1",
      correlation_id: run_id
    })

    assert_receive {:span_event, ^run_span, "run.continued_as_new", cont_attrs}
    assert cont_attrs["continuum.to_run_id"] == "next-1"
    assert cont_attrs["continuum.correlation_id"] == run_id
  end

  test "closes each retried activity attempt span" do
    run_id = Ecto.UUID.generate()
    task_id = Ecto.UUID.generate()
    mfa = {InlineActivity, :run, [1]}

    Continuum.Telemetry.execute([:continuum, :activity, :started], %{}, %{
      run_id: run_id,
      task_id: task_id,
      mfa: mfa,
      attempt: 1
    })

    Continuum.Telemetry.execute([:continuum, :activity, :retried], %{duration_ms: 12}, %{
      run_id: run_id,
      task_id: task_id,
      mfa: mfa,
      attempt: 1,
      next_attempt: 2,
      error: :boom
    })

    assert_receive {:span_started, first_span, "continuum.activity_attempt", first_attrs, []}
    assert first_attrs["continuum.attempt"] == 1

    assert_receive {:span_event, ^first_span, "activity.retried", retry_attrs}
    assert retry_attrs["continuum.measurement.duration_ms"] == 12
    assert retry_attrs["continuum.next_attempt"] == 2

    assert_receive {:span_ended, ^first_span, :error,
                    %{"continuum.terminal_event" => "activity.retried"}}

    Continuum.Telemetry.execute([:continuum, :activity, :started], %{}, %{
      run_id: run_id,
      task_id: task_id,
      mfa: mfa,
      attempt: 2
    })

    Continuum.Telemetry.execute([:continuum, :activity, :completed], %{duration_ms: 3}, %{
      run_id: run_id,
      task_id: task_id,
      mfa: mfa,
      attempt: 2
    })

    assert_receive {:span_started, second_span, "continuum.activity_attempt", second_attrs, []}
    assert second_attrs["continuum.attempt"] == 2

    assert_receive {:span_ended, ^second_span, :unset,
                    %{"continuum.terminal_event" => "activity.completed"}}
  end
end
