defmodule ContinuumExampleOrders.Activities.ValidateOrder do
  use Continuum.Activity,
    retry: [max_attempts: 3, backoff: :exponential, base_ms: 250],
    timeout: {:seconds, 5}

  @impl true
  def run(%{"items" => items}) when is_list(items) and items != [] do
    total_cents =
      Enum.reduce(items, 0, fn item, acc ->
        acc + Map.fetch!(item, "qty") * Map.fetch!(item, "price")
      end)

    {:ok, %{total_cents: total_cents, item_count: length(items)}}
  end

  def run(_input), do: {:error, :empty_order}
end
