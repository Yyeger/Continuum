defmodule ContinuumExampleOrdersWeb.OrderController do
  use Phoenix.Controller, formats: [:json]

  @workflow_opts [
    instance: :continuum_example_orders,
    journal: Continuum.Runtime.Journal.Postgres
  ]

  @fraud_review_decisions %{
    "approved" => :approved,
    "rejected" => :rejected
  }

  def create(conn, params) do
    order_id = Map.get(params, "order_id") || Ecto.UUID.generate()
    input = Map.put(params, "order_id", order_id)

    case Continuum.start(ContinuumExampleOrders.OrderFlow, input, @workflow_opts) do
      {:ok, run_id} ->
        json(conn, %{order_id: order_id, run_id: run_id})

      {:error, reason} ->
        conn
        |> put_status(:unprocessable_entity)
        |> json(%{error: inspect(reason)})
    end
  end

  def fraud_review(conn, %{"run_id" => run_id, "decision" => decision}) do
    case Map.fetch(@fraud_review_decisions, decision) do
      {:ok, signal} ->
        signal_fraud_review(conn, run_id, signal)

      :error ->
        invalid_fraud_review_decision(conn)
    end
  end

  def fraud_review(conn, _params), do: invalid_fraud_review_decision(conn)

  defp signal_fraud_review(conn, run_id, signal) do
    case Continuum.signal(run_id, :fraud_review, signal, @workflow_opts) do
      :ok ->
        json(conn, %{ok: true})

      {:error, reason} ->
        conn
        |> put_status(:not_found)
        |> json(%{error: inspect(reason)})
    end
  end

  defp invalid_fraud_review_decision(conn) do
    conn
    |> put_status(:bad_request)
    |> json(%{error: "decision must be approved or rejected"})
  end
end
