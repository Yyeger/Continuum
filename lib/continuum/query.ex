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
    * `:namespace` - run namespace. Defaults to `"default"`.
    * `:where` - list of condition tuples.
    * `:search` - run id or workflow substring convenience filter.
    * `:workflow` - workflow substring convenience filter.
    * `:state` - run state convenience filter.
    * `:order_by` - `{direction, field}`. Defaults to `{:desc, :started_at}`;
      run id is always appended as a stable tie-breaker.
    * `:cursor` and `:per_page` - opaque keyset cursor from the previous result;
      `:per_page` caps at 100.
  """
  @spec list(keyword()) :: {:ok, map()} | {:error, term()}
  def list(opts \\ []) do
    with {:ok, instance} <- repo_instance(opts),
         {:ok, query} <- build_query(opts),
         {direction, order_field} <- normalize_order(Keyword.get(opts, :order_by)),
         {:ok, cursor} <- decode_cursor(Keyword.get(opts, :cursor), direction, order_field) do
      per_page =
        opts |> Keyword.get(:per_page, @default_per_page) |> positive_integer(@default_per_page)

      per_page = min(per_page, @max_per_page)
      query = apply_cursor(query, cursor, direction, order_field)

      rows =
        instance.repo.all(
          from(r in query,
            order_by: ^stable_order(direction, order_field),
            limit: ^(per_page + 1)
          )
        )

      page_rows = Enum.take(rows, per_page)

      next_cursor =
        if length(rows) > per_page do
          encode_cursor(List.last(page_rows), direction, order_field)
        end

      {:ok,
       %{
         entries: Enum.map(page_rows, &decode_run/1),
         per_page: per_page,
         next_cursor: next_cursor
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
      # Merge in SQL: jsonb concatenation is atomic per statement, so two
      # concurrent callers cannot interleave a read-merge-write and silently
      # drop each other's keys.
      sql = """
      UPDATE continuum_runs
      SET attributes = COALESCE(attributes, '{}'::jsonb) || $2::jsonb
      WHERE id = $1::text::uuid
      """

      case instance.repo.query(sql, [run_id, attributes]) do
        {:ok, %{num_rows: 1}} -> :ok
        {:ok, %{num_rows: 0}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  def set_attributes(_run_id, attributes, _opts), do: {:error, {:invalid_attributes, attributes}}

  @doc false
  def decode_run(%Run{} = run) do
    {error, legacy_stacktrace} = run.error |> decode_term() |> Continuum.RunFailure.split()

    %{
      id: run.id,
      run_id: run.id,
      workflow: run.workflow,
      state: display_state(run.state, error),
      input: decode_term(run.input),
      attributes: run.attributes || %{},
      namespace: run.namespace || "default",
      idempotency_key: run.idempotency_key,
      result: decode_term(run.result),
      error: error,
      error_stacktrace: decode_term(run.error_stacktrace) || legacy_stacktrace,
      trace_context: run.trace_context,
      started_at: run.started_at,
      completed_at: run.completed_at,
      lease_owner: run.lease_owner,
      lease_token: run.lease_token,
      lease_acquired_at: run.lease_acquired_at,
      lease_heartbeat_at: run.lease_heartbeat_at,
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
    query = apply_namespace(query, Keyword.get(opts, :namespace, "default"))
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

  defp apply_namespace(query, nil), do: query

  defp apply_namespace(query, namespace) do
    namespace = to_string(namespace)
    from(r in query, where: r.namespace == ^namespace)
  end

  defp apply_condition(query, {op, [:attributes, key], value}) when op in [:eq, :neq] do
    key = to_string(key)

    with {:ok, containment} <- normalize_attributes(%{key => value}) do
      query =
        case op do
          :eq ->
            from(r in query, where: fragment("? @> ?", r.attributes, type(^containment, :map)))

          :neq ->
            from(r in query,
              where:
                fragment("(? ->> ?) IS NOT NULL", r.attributes, ^key) and
                  not fragment("? @> ?", r.attributes, type(^containment, :map))
            )
        end

      {:ok, query}
    end
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
        # Match both the canonical state and legacy pre-0.5.2 rows that
        # stored a cancel as failed + :cancelled.
        cancelled = encode_term(:cancelled)

        from(r in query,
          where: r.state == "cancelled" or (r.state == "failed" and r.error == ^cancelled)
        )

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

  defp normalize_order({direction, :run_id}), do: normalize_order({direction, :id})

  defp normalize_order({direction, field})
       when direction in [:asc, :desc] and field in @order_fields,
       do: {direction, field}

  defp normalize_order(_other), do: {:desc, :started_at}

  defp stable_order(:asc, :id), do: [asc: :id]
  defp stable_order(:desc, :id), do: [desc: :id]
  defp stable_order(:asc, field), do: [{:asc_nulls_last, field}, {:asc, :id}]
  defp stable_order(:desc, field), do: [{:desc_nulls_last, field}, {:desc, :id}]

  defp apply_cursor(query, nil, _direction, _field), do: query

  defp apply_cursor(query, {_value, id}, :asc, :id),
    do: from(r in query, where: r.id > ^id)

  defp apply_cursor(query, {_value, id}, :desc, :id),
    do: from(r in query, where: r.id < ^id)

  defp apply_cursor(query, {nil, id}, :asc, field),
    do: from(r in query, where: is_nil(field(r, ^field)) and r.id > ^id)

  defp apply_cursor(query, {nil, id}, :desc, field),
    do: from(r in query, where: is_nil(field(r, ^field)) and r.id < ^id)

  defp apply_cursor(query, {value, id}, :asc, field) do
    from(r in query,
      where:
        field(r, ^field) > ^value or is_nil(field(r, ^field)) or
          (field(r, ^field) == ^value and r.id > ^id)
    )
  end

  defp apply_cursor(query, {value, id}, :desc, field) do
    from(r in query,
      where:
        field(r, ^field) < ^value or is_nil(field(r, ^field)) or
          (field(r, ^field) == ^value and r.id < ^id)
    )
  end

  defp encode_cursor(run, direction, field) do
    {direction, field, Map.fetch!(run, field), run.id}
    |> :erlang.term_to_binary()
    |> Base.url_encode64(padding: false)
  end

  defp decode_cursor(nil, _direction, _field), do: {:ok, nil}
  defp decode_cursor("", _direction, _field), do: {:ok, nil}

  defp decode_cursor(cursor, direction, field)
       when is_binary(cursor) and byte_size(cursor) <= 1_024 do
    with {:ok, binary} <- Base.url_decode64(cursor, padding: false),
         {^direction, ^field, value, id} when is_binary(id) <-
           :erlang.binary_to_term(binary, [:safe]) do
      {:ok, {value, id}}
    else
      _ -> {:error, :invalid_cursor}
    end
  rescue
    _ -> {:error, :invalid_cursor}
  end

  defp decode_cursor(_cursor, _direction, _field), do: {:error, :invalid_cursor}

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
end
