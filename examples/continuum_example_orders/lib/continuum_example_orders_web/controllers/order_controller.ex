defmodule ContinuumExampleOrdersWeb.OrderController do
  use Phoenix.Controller, formats: [:json]

  def create(conn, params) do
    order_id = Map.get(params, "order_id") || Ecto.UUID.generate()
    input = Map.put(params, "order_id", order_id)

    case Continuum.start(ContinuumExampleOrders.OrderFlow, input) do
      {:ok, run_id} ->
        json(conn, %{order_id: order_id, run_id: run_id})

      {:error, reason} ->
        conn
        |> put_status(:unprocessable_entity)
        |> json(%{error: inspect(reason)})
    end
  end

  def fraud_review(conn, %{"run_id" => run_id, "decision" => decision}) do
    signal = String.to_existing_atom(decision)

    case Continuum.signal(run_id, :fraud_review, signal) do
      :ok ->
        json(conn, %{ok: true})

      {:error, reason} ->
        conn
        |> put_status(:not_found)
        |> json(%{error: inspect(reason)})
    end
  rescue
    ArgumentError ->
      conn
      |> put_status(:bad_request)
      |> json(%{error: "decision must be approved or rejected"})
  end
end
