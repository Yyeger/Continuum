defmodule Continuum.Query do
  @moduledoc """
  Structured read API for durable run rows.

  Queries are intentionally closed over a small set of fields and operators.
  This keeps the public API independent from arbitrary SQL fragments while still
  supporting operator dashboards and search attributes.
  """

  import Ecto.Query

  alias Continuum.DurableTerm
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
  @spec list(keyword()) :: {:ok, Continuum.Page.t(Continuum.Run.t())} | {:error, term()}
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
       %Continuum.Page{
         entries: Enum.map(page_rows, &decode_run(&1, opts)),
         per_page: per_page,
         next_cursor: next_cursor
       }}
    end
  end

  @doc """
  Loads one run by id.
  """
  @spec get_run(binary(), keyword()) ::
          {:ok, Continuum.Run.t()} | {:error, :not_found | term()}
  def get_run(run_id, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      query = from(r in Run, where: r.id == ^run_id)

      with {:ok, query} <- apply_namespace_precondition(query, opts),
           run when not is_nil(run) <- instance.repo.one(query) do
        {:ok, decode_run(run, opts)}
      else
        nil -> {:error, :not_found}
        {:error, _reason} = error -> error
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
         {:ok, attributes} <- normalize_attributes(attributes),
         {:ok, namespace} <- namespace_precondition(opts) do
      # Merge in SQL: jsonb concatenation is atomic per statement, so two
      # concurrent callers cannot interleave a read-merge-write and silently
      # drop each other's keys.
      {sql, params} = set_attributes_query(run_id, attributes, namespace)

      case instance.repo.query(sql, params) do
        {:ok, %{num_rows: 1}} -> :ok
        {:ok, %{num_rows: 0}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  def set_attributes(_run_id, attributes, _opts), do: {:error, {:invalid_attributes, attributes}}

  defp apply_namespace_precondition(query, opts) do
    case namespace_precondition(opts) do
      {:ok, nil} -> {:ok, query}
      {:ok, namespace} -> {:ok, from(r in query, where: r.namespace == ^namespace)}
      {:error, _reason} = error -> error
    end
  end

  defp namespace_precondition(opts) do
    case Keyword.fetch(opts, :namespace) do
      :error -> {:ok, nil}
      {:ok, namespace} -> Continuum.Runtime.NamespacePrecondition.normalize(namespace)
    end
  end

  defp set_attributes_query(run_id, attributes, nil) do
    {"""
     UPDATE continuum_runs
     SET attributes = COALESCE(attributes, '{}'::jsonb) || $2::jsonb
     WHERE id = $1::text::uuid
     """, [run_id, attributes]}
  end

  defp set_attributes_query(run_id, attributes, namespace) do
    {"""
     UPDATE continuum_runs
     SET attributes = COALESCE(attributes, '{}'::jsonb) || $2::jsonb
     WHERE id = $1::text::uuid AND namespace = $3
     """, [run_id, attributes, namespace]}
  end

  @doc false
  def decode_run(%Run{} = run), do: decode_run(run, [])

  @doc false
  def decode_run(%Run{} = run, opts) do
    include_payloads? = Keyword.get(opts, :include_payloads, true)

    {input, result, error, error_stacktrace} =
      if include_payloads? do
        decoded_error = decode_payload_raw(run.error, opts)

        {error, legacy_stacktrace} =
          if omitted_payload?(decoded_error) do
            {decoded_error, nil}
          else
            Continuum.RunFailure.split(decoded_error)
          end

        stacktrace = decode_payload_raw(run.error_stacktrace, opts) || legacy_stacktrace

        {
          decode_payload(run.input, opts),
          decode_payload(run.result, opts),
          redact_payload(error, opts),
          redact_payload(stacktrace, opts)
        }
      else
        {nil, nil, nil, nil}
      end

    %Continuum.Run{
      id: run.id,
      run_id: run.id,
      workflow: run.workflow,
      state: display_state(run.state, error),
      input: input,
      attributes: run.attributes || %{},
      namespace: run.namespace || "default",
      idempotency_key: run.idempotency_key,
      result: result,
      error: error,
      error_stacktrace: error_stacktrace,
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

  defp apply_condition(query, {op, [:attributes | path], value})
       when op in [:eq, :neq] and path != [] do
    path = Enum.map(path, &to_string/1)
    containment = Enum.reduce(Enum.reverse(path), value, fn key, nested -> %{key => nested} end)

    with {:ok, containment} <- normalize_attributes(containment) do
      query =
        case op do
          :eq ->
            from(r in query, where: fragment("? @> ?", r.attributes, type(^containment, :map)))

          :neq ->
            from(r in query,
              where:
                fragment("(? #> ?::text[]) IS NOT NULL", r.attributes, ^path) and
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

    # `cancelled` has been a real terminal state since v0.5.2, and the v0.7.2
    # migration promotes legacy `failed` + encoded `:cancelled` rows into it, so
    # the state column is authoritative on its own. Do not reintroduce a
    # comparison against an encoded error payload here: it is only correct while
    # payloads are encoded with `:erlang.term_to_binary/1`, and it fails
    # silently — legacy cancels reclassify as failures with no error.
    from(r in query, where: r.state == ^state)
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
    with {:ok, json} <- encode_json(attributes) do
      Jason.decode(json)
    end
  end

  defp decode_payload(nil, _opts), do: nil

  defp decode_payload(value, opts) do
    value |> decode_payload_raw(opts) |> redact_payload(opts)
  end

  defp decode_payload_raw(nil, _opts), do: nil

  defp decode_payload_raw(binary, opts) when is_binary(binary) do
    case payload_limit!(opts) do
      limit when is_integer(limit) and byte_size(binary) > limit ->
        %{omitted: :payload_too_large, encoded_bytes: byte_size(binary)}

      _limit ->
        decode_term(binary)
    end
  end

  defp decode_payload_raw(value, _opts), do: value

  defp payload_limit!(opts) do
    case Keyword.get(opts, :max_payload_bytes, :infinity) do
      :infinity ->
        :infinity

      limit when is_integer(limit) and limit > 0 ->
        limit

      limit ->
        raise ArgumentError,
              "expected :max_payload_bytes to be positive or :infinity, got: #{inspect(limit)}"
    end
  end

  defp redact_payload(nil, _opts), do: nil
  defp redact_payload(%{omitted: :payload_too_large} = payload, _opts), do: payload

  defp redact_payload(payload, opts) do
    case Keyword.get(opts, :redactor) do
      nil -> payload
      redactor when is_function(redactor, 1) -> redactor.(payload)
      module when is_atom(module) -> module.redact(payload)
      redactor -> raise ArgumentError, "invalid query redactor: #{inspect(redactor)}"
    end
  end

  defp omitted_payload?(%{omitted: :payload_too_large}), do: true
  defp omitted_payload?(_payload), do: false

  defp encode_json(value) do
    case Jason.encode(value) do
      {:ok, json} -> {:ok, json}
      {:error, reason} -> {:error, {:invalid_json, reason}}
    end
  end

  defp decode_term(binary) do
    DurableTerm.decode!(binary)
  rescue
    error -> {:decode_error, error}
  end

  # Kept as a courtesy for databases that have not yet run the v0.7.2 legacy
  # cancel promotion. This decodes the payload in Elixir rather than comparing
  # encoded bytes in SQL, so unlike the filter in `apply_state/2` it stays
  # correct under any payload encoding.
  defp display_state("failed", :cancelled), do: :cancelled

  defp display_state(state, _error),
    do: DurableTerm.atom_from_binary!(state, :run_state)

  defp positive_integer(value, _fallback) when is_integer(value) and value > 0, do: value

  defp positive_integer(value, fallback) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> fallback
    end
  end

  defp positive_integer(_value, fallback), do: fallback
end
