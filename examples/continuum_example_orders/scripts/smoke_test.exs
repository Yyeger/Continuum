defmodule ContinuumExampleOrders.SmokeTest do
  def run do
    workflow_opts = [
      instance: :continuum_example_orders,
      journal: Continuum.Runtime.Journal.Postgres
    ]

    instance = Continuum.Runtime.Instance.lookup(:continuum_example_orders)

    suffix = System.unique_integer([:positive])
    smoke_id = "smoke-#{suffix}"

    approved =
      run_order!(
        "order-smoke-approved-#{suffix}",
        :approved,
        "retail",
        smoke_id,
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
        "enterprise",
        smoke_id,
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

    subscription = run_subscription!("subscription-smoke-#{suffix}", workflow_opts, instance)
    query = assert_namespace_queries!(workflow_opts, smoke_id, approved, rejected)

    IO.inspect(
      %{approved: approved, rejected: rejected, subscription: subscription, query: query},
      label: "smoke"
    )
  end

  defp run_order!(
         order_id,
         decision,
         namespace,
         smoke_id,
         workflow_opts,
         instance,
         expected_events
       ) do
    input = %{
      "order_id" => order_id,
      "items" => [%{"sku" => "sku_1", "qty" => 1, "price" => 1200}]
    }

    opts =
      Keyword.merge(workflow_opts,
        namespace: namespace,
        attributes: %{
          smoke_id: smoke_id,
          order_id: order_id,
          decision: to_string(decision)
        }
      )

    {:ok, run_id} = Continuum.start(ContinuumExampleOrders.OrderFlow, input, opts)
    IO.puts("started #{namespace}/#{decision} order #{run_id}")

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

    %{run_id: run_id, namespace: namespace, result: result, events: events}
  end

  defp assert_namespace_queries!(workflow_opts, smoke_id, approved, rejected) do
    :ok =
      Continuum.set_attributes(approved.run_id, %{review_status: "approved_by_smoke"},
        instance: :continuum_example_orders
      )

    retail =
      query_runs!(
        workflow_opts,
        namespace: approved.namespace,
        where: [{:eq, [:attributes, "smoke_id"], smoke_id}],
        per_page: 10
      )

    enterprise =
      query_runs!(
        workflow_opts,
        namespace: rejected.namespace,
        where: [{:eq, [:attributes, "smoke_id"], smoke_id}],
        per_page: 10
      )

    all_namespaces =
      query_runs!(
        workflow_opts,
        namespace: nil,
        where: [{:eq, [:attributes, "smoke_id"], smoke_id}],
        order_by: {:asc, :started_at},
        per_page: 10
      )

    assert_run_ids!("retail namespace query", retail.entries, [approved.run_id])
    assert_run_ids!("enterprise namespace query", enterprise.entries, [rejected.run_id])

    assert_run_ids!("cross-namespace query", all_namespaces.entries, [
      approved.run_id,
      rejected.run_id
    ])

    reviewed =
      query_runs!(
        workflow_opts,
        namespace: approved.namespace,
        where: [{:eq, [:attributes, "review_status"], "approved_by_smoke"}],
        per_page: 10
      )

    assert_run_ids!("set_attributes query", reviewed.entries, [approved.run_id])

    %{
      retail_total: retail.total,
      enterprise_total: enterprise.total,
      all_namespaces_total: all_namespaces.total,
      reviewed_total: reviewed.total
    }
  end

  defp query_runs!(workflow_opts, opts) do
    opts = Keyword.merge([instance: Keyword.fetch!(workflow_opts, :instance)], opts)

    case Continuum.query(opts) do
      {:ok, page} -> page
      {:error, reason} -> raise "query failed: #{inspect(reason)}"
    end
  end

  defp assert_run_ids!(label, entries, expected_ids) do
    actual_ids = Enum.map(entries, & &1.run_id)

    unless MapSet.new(actual_ids) == MapSet.new(expected_ids) do
      raise "#{label} returned #{inspect(actual_ids)}, expected #{inspect(expected_ids)}"
    end
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

  defp run_subscription!(subscription_id, workflow_opts, instance) do
    input = %{
      "subscription_id" => subscription_id,
      "cycles_done" => 0,
      "max_cycles" => 2,
      "amount_cents" => 500
    }

    {:ok, run_id} = Continuum.start(ContinuumExampleOrders.SubscriptionFlow, input, workflow_opts)
    IO.puts("started subscription #{run_id}")

    {:ok, %{state: :completed, result: {:continued, next_run_id}}} =
      Continuum.await(run_id, 15_000, workflow_opts)

    {:ok, %{state: :completed, result: {:ok, result}}} =
      Continuum.await(next_run_id, 15_000, workflow_opts)

    root_events =
      instance
      |> Continuum.Runtime.Journal.Postgres.load(run_id)
      |> Enum.map(& &1.type)

    next_events =
      instance
      |> Continuum.Runtime.Journal.Postgres.load(next_run_id)
      |> Enum.map(& &1.type)

    unless :run_continued_as_new in root_events do
      raise "subscription root did not continue_as_new: #{inspect(root_events)}"
    end

    unless result.cycles_done == 2 do
      raise "subscription completed with unexpected result: #{inspect(result)}"
    end

    %{
      root_run_id: run_id,
      next_run_id: next_run_id,
      result: result,
      root_events: root_events,
      next_events: next_events
    }
  end
end

ContinuumExampleOrders.SmokeTest.run()
