defmodule ContinuumExampleOrders.SmokeTest do
  def run do
    workflow_opts = [
      instance: :continuum_example_orders,
      journal: Continuum.Runtime.Journal.Postgres
    ]

    instance = Continuum.Runtime.Instance.lookup(:continuum_example_orders)

    suffix = System.unique_integer([:positive])

    approved =
      run_order!(
        "order-smoke-approved-#{suffix}",
        :approved,
        workflow_opts,
        instance,
        [
          :activity_scheduled,
          :activity_completed,
          :activity_scheduled,
          :activity_completed,
          :signal_awaited,
          :signal_received,
          :activity_scheduled,
          :activity_completed
        ]
      )

    rejected =
      run_order!(
        "order-smoke-rejected-#{suffix}",
        :rejected,
        workflow_opts,
        instance,
        [
          :activity_scheduled,
          :activity_completed,
          :activity_scheduled,
          :activity_completed,
          :signal_awaited,
          :signal_received,
          :compensation_scheduled,
          :compensation_completed
        ]
      )

    IO.inspect(%{approved: approved, rejected: rejected}, label: "smoke")
  end

  defp run_order!(order_id, decision, workflow_opts, instance, expected_events) do
    input = %{
      "order_id" => order_id,
      "items" => [%{"sku" => "sku_1", "qty" => 1, "price" => 1200}]
    }

    {:ok, run_id} = Continuum.start(ContinuumExampleOrders.OrderFlow, input, workflow_opts)
    IO.puts("started #{decision} order #{run_id}")

    wait_for_event!(instance, run_id, :signal_awaited)
    :ok = Continuum.signal(run_id, :fraud_review, decision, workflow_opts)

    {:ok, %{state: :completed, result: result}} = Continuum.await(run_id, 15_000, workflow_opts)

    events =
      instance
      |> Continuum.Runtime.Journal.Postgres.load(run_id)
      |> Enum.map(& &1.type)

    unless events == expected_events do
      raise "unexpected events for #{order_id}: #{inspect(events)}"
    end

    %{run_id: run_id, result: result, events: events}
  end

  defp wait_for_event!(instance, run_id, event_type, attempts \\ 100)

  defp wait_for_event!(instance, run_id, event_type, attempts) when attempts > 0 do
    history = Continuum.Runtime.Journal.Postgres.load(instance, run_id)

    if Enum.any?(history, &(&1.type == event_type)) do
      :ok
    else
      Process.sleep(100)
      wait_for_event!(instance, run_id, event_type, attempts - 1)
    end
  end

  defp wait_for_event!(_instance, run_id, event_type, 0) do
    raise "run #{run_id} did not journal #{event_type}"
  end
end

ContinuumExampleOrders.SmokeTest.run()
