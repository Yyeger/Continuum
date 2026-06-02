defmodule Continuum.Query do
  @moduledoc """
  Structured read API for durable run rows.

  Queries are intentionally closed over a small set of fields and operators.
  This keeps the public API independent from arbitrary SQL fragments while still
  supporting operator dashboards and search attributes.
  """

  import Ecto.Query

  alias Continuum.Runtime.Instance
  alias Continuum.Schema.Run

  @default_per_page 25
  @max_per_page 100
  @query_fields [:id, :state, :workflow, :started_at, :completed_at]
  @order_fields [:id, :state, :workflow, :started_at, :completed_at]

  @type field :: :id | :run_id | :state | :workflow | :started_at | :completed_at
  @type attribute_path :: list(atom() | binary())
  @type condition ::
          {:eq | :neq | :lt | :lte | :gt | :gte, field(), term()}
          | {:eq | :neq, attribute_path(), term()}
          | {:in, field(), [term()]}

  @doc """
  Lists runs matching a structured query.

  Options:

    * `:instance` - Continuum instance name or struct. Defaults to `Continuum`.
    * `:where` - list of condition tuples.
    * `:search` - run id or workflow substring convenience filter.
    * `:workflow` - workflow substring convenience filter.
    * `:state` - run state convenience filter.
    * `:order_by` - `{direction, field}`. Defaults to `{:desc, :started_at}`.
    * `:page` and `:per_page` - 1-based pagination; `:per_page` caps at 100.
  """
  @spec list(keyword()) :: {:ok, map()} | {:error, term()}
  def list(opts \\ []) do
    with {:ok, instance} <- repo_instance(opts),
         {:ok, query} <- build_query(opts) do
      page = opts |> Keyword.get(:page, 1) |> positive_integer(1)

      per_page =
        opts |> Keyword.get(:per_page, @default_per_page) |> positive_integer(@default_per_page)

      per_page = min(per_page, @max_per_page)
      offset = (page - 1) * per_page

      total = instance.repo.one(from(r in query, select: count(r.id)))

      entries =
        instance.repo.all(
          from(r in query,
            order_by: ^order_by(Keyword.get(opts, :order_by, {:desc, :started_at})),
            limit: ^per_page,
            offset: ^offset
          )
        )
        |> Enum.map(&decode_run/1)

      {:ok,
       %{
         entries: entries,
         page: page,
         per_page: per_page,
         total: total,
         total_pages: max(ceil_div(total, per_page), 1)
       }}
    end
  end

  @doc """
  Loads one run by id.
  """
  @spec get_run(binary(), keyword()) :: {:ok, map()} | {:error, :not_found | term()}
  def get_run(run_id, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      case instance.repo.one(from(r in Run, where: r.id == ^run_id)) do
        nil -> {:error, :not_found}
        run -> {:ok, decode_run(run)}
      end
    end
  end

  @doc """
  Merges search attributes into a run row.

  Attributes must be JSON-encodable map data. This updates metadata only; it
  does not append a journal event.
  """
  @spec set_attributes(binary(), map(), keyword()) :: :ok | {:error, term()}
  def set_attributes(run_id, attributes, opts \\ [])

  def set_attributes(run_id, attributes, opts) when is_map(attributes) do
    with {:ok, instance} <- repo_instance(opts),
         {:ok, attributes} <- normalize_attributes(attributes) do
      case instance.repo.one(from(r in Run, where: r.id == ^run_id)) do
        nil ->
          {:error, :not_found}

        %Run{} = run ->
          merged = Map.merge(run.attributes || %{}, attributes)

          {1, _} =
            instance.repo.update_all(from(r in Run, where: r.id == ^run_id),
              set: [attributes: merged]
            )

          :ok
      end
    end
  end

  def set_attributes(_run_id, attributes, _opts), do: {:error, {:invalid_attributes, attributes}}

  @doc false
  def decode_run(%Run{} = run) do
    error = decode_term(run.error)

    %{
      id: run.id,
      run_id: run.id,
      workflow: run.workflow,
      state: display_state(run.state, error),
      input: decode_term(run.input),
      attributes: run.attributes || %{},
      result: decode_term(run.result),
      error: error,
      trace_context: run.trace_context,
      started_at: run.started_at,
      completed_at: run.completed_at,
      lease_owner: run.lease_owner,
      lease_token: run.lease_token,
      lease_expires_at: run.lease_expires_at,
      next_wakeup_at: run.next_wakeup_at,
      retention_until: run.retention_until,
      parent_run_id: run.parent_run_id,
      correlation_id: run.correlation_id,
      continued_from_run_id: run.continued_from_run_id
    }
  end

  defp build_query(opts) do
    query = Run
    query = apply_state(query, Keyword.get(opts, :state))
    query = apply_workflow(query, Keyword.get(opts, :workflow))
    query = apply_search(query, Keyword.get(opts, :search))

    Enum.reduce_while(Keyword.get(opts, :where, []), {:ok, query}, fn condition, {:ok, acc} ->
      case apply_condition(acc, condition) do
        {:ok, query} -> {:cont, {:ok, query}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp apply_condition(query, {op, [:attributes, key], value}) when op in [:eq, :neq] do
    key = to_string(key)
    value = attribute_value(value)

    query =
      case op do
        :eq -> from(r in query, where: fragment("? ->> ? = ?", r.attributes, ^key, ^value))
        :neq -> from(r in query, where: fragment("? ->> ? != ?", r.attributes, ^key, ^value))
      end

    {:ok, query}
  end

  defp apply_condition(query, {:in, field, values}) when is_list(values) do
    with {:ok, field} <- query_field(field) do
      {:ok, from(r in query, where: field(r, ^field) in ^values)}
    end
  end

  defp apply_condition(query, {:eq, :state, value}), do: {:ok, apply_state(query, value)}

  defp apply_condition(query, {:eq, :workflow, value}),
    do: {:ok, from(r in query, where: r.workflow == ^to_string(value))}

  defp apply_condition(query, {op, field, value}) when op in [:eq, :neq, :lt, :lte, :gt, :gte] do
    with {:ok, field} <- query_field(field) do
      {:ok, compare_field(query, op, field, value)}
    end
  end

  defp apply_condition(_query, condition), do: {:error, {:invalid_condition, condition}}

  defp compare_field(query, :eq, field, value),
    do: from(r in query, where: field(r, ^field) == ^value)

  defp compare_field(query, :neq, field, value),
    do: from(r in query, where: field(r, ^field) != ^value)

  defp compare_field(query, :lt, field, value),
    do: from(r in query, where: field(r, ^field) < ^value)

  defp compare_field(query, :lte, field, value),
    do: from(r in query, where: field(r, ^field) <= ^value)

  defp compare_field(query, :gt, field, value),
    do: from(r in query, where: field(r, ^field) > ^value)

  defp compare_field(query, :gte, field, value),
    do: from(r in query, where: field(r, ^field) >= ^value)

  defp apply_state(query, nil), do: query
  defp apply_state(query, ""), do: query

  defp apply_state(query, state) do
    state = state |> to_string() |> String.downcase()

    case state do
      "cancelled" ->
        cancelled = encode_term(:cancelled)
        from(r in query, where: r.state == "failed" and r.error == ^cancelled)

      "failed" ->
        cancelled = encode_term(:cancelled)

        from(r in query,
          where: r.state == "failed" and (is_nil(r.error) or r.error != ^cancelled)
        )

      _ ->
        from(r in query, where: r.state == ^state)
    end
  end

  defp apply_workflow(query, nil), do: query
  defp apply_workflow(query, ""), do: query

  defp apply_workflow(query, workflow) do
    pattern = "%#{workflow}%"
    from(r in query, where: ilike(r.workflow, ^pattern))
  end

  defp apply_search(query, nil), do: query
  defp apply_search(query, ""), do: query

  defp apply_search(query, search) do
    pattern = "%#{search}%"

    from(r in query,
      where: fragment("?::text ILIKE ?", r.id, ^pattern) or ilike(r.workflow, ^pattern)
    )
  end

  defp query_field(:run_id), do: {:ok, :id}
  defp query_field(field) when field in @query_fields, do: {:ok, field}
  defp query_field(field), do: {:error, {:invalid_field, field}}

  defp order_by({direction, :run_id}), do: order_by({direction, :id})

  defp order_by({direction, field}) when direction in [:asc, :desc] and field in @order_fields,
    do: [{direction, field}]

  defp order_by(_other), do: [desc: :started_at, desc: :id]

  defp repo_instance(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case instance.repo do
      nil -> {:error, :repo_not_configured}
      _repo -> {:ok, instance}
    end
  end

  defp normalize_attributes(attributes) do
    with {:ok, json} <- encode_json(attributes),
         {:ok, decoded} <- Jason.decode(json) do
      {:ok, decoded}
    end
  end

  defp encode_json(value) do
    case Jason.encode(value) do
      {:ok, json} -> {:ok, json}
      {:error, reason} -> {:error, {:invalid_json, reason}}
    end
  end

  defp attribute_value(value) when is_binary(value), do: value
  defp attribute_value(value) when is_atom(value), do: Atom.to_string(value)
  defp attribute_value(value), do: to_string(value)

  defp decode_term(nil), do: nil

  defp decode_term(binary) when is_binary(binary) do
    :erlang.binary_to_term(binary)
  rescue
    error -> {:decode_error, error}
  end

  defp decode_term(other), do: other

  defp display_state("failed", :cancelled), do: :cancelled
  defp display_state(state, _error), do: String.to_atom(state)

  defp encode_term(term), do: :erlang.term_to_binary(term)

  defp positive_integer(value, _fallback) when is_integer(value) and value > 0, do: value

  defp positive_integer(value, fallback) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> fallback
    end
  end

  defp positive_integer(_value, fallback), do: fallback

  defp ceil_div(0, _denominator), do: 0
  defp ceil_div(numerator, denominator), do: div(numerator + denominator - 1, denominator)
end
