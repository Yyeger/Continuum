defmodule Continuum.OpenTelemetry do
  @moduledoc """
  Optional OpenTelemetry bridge for Continuum telemetry events.

  Continuum does not depend on OpenTelemetry. Applications that already use
  OpenTelemetry can add the `:opentelemetry` and `:opentelemetry_api` packages,
  configure their exporter, and call `setup/1` from their application startup.

      {:ok, _handler_id} = Continuum.OpenTelemetry.setup()

  The bridge turns existing `[:continuum, ...]` telemetry events into short
  spans:

    * a `continuum.run_attempt` span for each engine attempt, closed when the
      run suspends, completes, fails, is cancelled, or loses its lease
    * a `continuum.activity_attempt` span for each activity attempt, closed
      when the activity completes, fails, or is retried

  Run-attempt spans always include `continuum.run_id`. When the run metadata
  contains a persisted W3C `traceparent`, the span also gets a link to that
  original trace context so resumed attempts can be correlated across process
  and VM restarts.

  In durable Postgres mode, activity attempts run in worker processes after the
  engine attempt has suspended. Those activity spans are usually sibling roots
  correlated by `continuum.run_id`, not children of the run-attempt span.
  """

  @default_handler_id "#{__MODULE__}.handler"
  @run_span_key {__MODULE__, :run_span}
  @activity_span_key {__MODULE__, :activity_span}

  @run_start [:continuum, :run, :started]
  @run_end [
    [:continuum, :run, :suspended],
    [:continuum, :run, :completed],
    [:continuum, :run, :failed],
    [:continuum, :run, :cancelled],
    [:continuum, :run, :lease_lost]
  ]
  @activity_start [:continuum, :activity, :started]
  @activity_end [
    [:continuum, :activity, :completed],
    [:continuum, :activity, :failed],
    [:continuum, :activity, :retried]
  ]
  @breadcrumbs [
    [:continuum, :activity, :scheduled],
    [:continuum, :activity, :idempotency_hit],
    [:continuum, :timer, :scheduled],
    [:continuum, :timer, :fired],
    [:continuum, :signal, :awaited],
    [:continuum, :signal, :delivered],
    [:continuum, :signal, :received]
  ]

  @events [@run_start, @activity_start] ++ @run_end ++ @activity_end ++ @breadcrumbs

  @typedoc "Handler id returned by `setup/1` and accepted by `detach/1`."
  @type handler_id :: term()

  @doc """
  Attaches the OpenTelemetry bridge to Continuum telemetry events.

  By default this expects `:opentelemetry` and `:opentelemetry_api` to be loaded
  by the host application. If they are not available, returns
  `{:error, :opentelemetry_not_loaded}` and does not attach handlers.

  Options:

    * `:handler_id` - telemetry handler id. Defaults to a stable Continuum id.
    * `:tracer` - internal testing hook. Production callers should leave this
      unset so the real OpenTelemetry API is used.
  """
  @spec setup(keyword()) :: {:ok, handler_id()} | {:error, term()}
  def setup(opts \\ []) do
    handler_id = Keyword.get(opts, :handler_id, @default_handler_id)
    tracer = Keyword.get(opts, :tracer, :opentelemetry)

    with :ok <- ensure_tracer(tracer) do
      state = %{tracer: tracer}

      case :telemetry.attach_many(handler_id, @events, &__MODULE__.handle_event/4, state) do
        :ok -> {:ok, handler_id}
        {:error, :already_exists} = error -> error
      end
    end
  end

  @doc """
  Detaches a bridge installed by `setup/1`.
  """
  @spec detach(handler_id()) :: :ok | {:error, :not_found}
  def detach(handler_id \\ @default_handler_id), do: :telemetry.detach(handler_id)

  @doc false
  def handle_event(@run_start, _measurements, metadata, state) do
    span =
      start_span(state, "continuum.run_attempt", run_attributes(metadata),
        links: trace_links(state.tracer, metadata)
      )

    Process.put(@run_span_key, span)
    :ok
  end

  def handle_event(event, measurements, metadata, state) when event in @run_end do
    span = Process.get(@run_span_key)

    if span_started?(span) do
      add_event(state, span, event_name(event), event_attributes(measurements, metadata))
      end_span(state, span, span_status(event), terminal_attributes(event, metadata))
      Process.delete(@run_span_key)
    end

    :ok
  end

  def handle_event(@activity_start, _measurements, metadata, state) do
    span =
      start_span(state, "continuum.activity_attempt", activity_attributes(metadata),
        links: active_run_link(state.tracer)
      )

    Process.put(activity_span_key(metadata), span)
    :ok
  end

  def handle_event(event, measurements, metadata, state) when event in @activity_end do
    span = Process.get(activity_span_key(metadata))

    if span_started?(span) do
      add_event(state, span, event_name(event), event_attributes(measurements, metadata))
      end_span(state, span, span_status(event), terminal_attributes(event, metadata))
      Process.delete(activity_span_key(metadata))
    end

    :ok
  end

  def handle_event(event, measurements, metadata, state) when event in @breadcrumbs do
    span = Process.get(@run_span_key)

    if span_started?(span) do
      add_event(state, span, event_name(event), event_attributes(measurements, metadata))
    end

    :ok
  end

  def handle_event(_event, _measurements, _metadata, _state), do: :ok

  defp ensure_tracer(:opentelemetry) do
    if Code.ensure_loaded?(:opentelemetry) and Code.ensure_loaded?(:otel_tracer) and
         Code.ensure_loaded?(:otel_span) do
      :ok
    else
      {:error, :opentelemetry_not_loaded}
    end
  end

  defp ensure_tracer({module, _config}), do: ensure_tracer(module)

  defp ensure_tracer(module) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :start_span, 4) and
         function_exported?(module, :end_span, 4) do
      :ok
    else
      {:error, {:invalid_tracer, module}}
    end
  end

  defp start_span(%{tracer: :opentelemetry}, name, attributes, opts) do
    tracer = apply(:opentelemetry, :get_tracer, [:continuum])

    span =
      apply(:otel_tracer, :start_span, [
        tracer,
        name,
        %{
          attributes: attributes,
          links: Keyword.get(opts, :links, [])
        }
      ])

    previous = apply(:otel_tracer, :set_current_span, [span])
    %{span: span, previous: previous}
  rescue
    _ -> :no_span
  end

  defp start_span(%{tracer: tracer}, name, attributes, opts) do
    call_custom_tracer(tracer, :start_span, [
      name,
      attributes,
      Keyword.get(opts, :links, []),
      custom_config(tracer)
    ])
  rescue
    _ -> :no_span
  end

  defp end_span(_state, :no_span, _status, _attributes), do: :ok

  defp end_span(%{tracer: :opentelemetry}, %{span: span, previous: previous}, status, attributes) do
    apply(:otel_span, :set_attributes, [span, attributes])
    if status != :unset, do: apply(:otel_span, :set_status, [span, status])
    apply(:otel_span, :end_span, [span])
    apply(:otel_tracer, :set_current_span, [previous])
    :ok
  rescue
    _ -> :ok
  end

  defp end_span(%{tracer: tracer}, span, status, attributes) do
    call_custom_tracer(tracer, :end_span, [span, status, attributes, custom_config(tracer)])
    :ok
  end

  defp add_event(_state, :no_span, _name, _attributes), do: :ok

  defp add_event(%{tracer: :opentelemetry}, %{span: span}, name, attributes) do
    apply(:otel_span, :add_event, [span, name, attributes])
    :ok
  rescue
    _ -> :ok
  end

  defp add_event(%{tracer: tracer}, span, name, attributes) do
    call_custom_tracer(tracer, :add_event, [span, name, attributes, custom_config(tracer)])
    :ok
  end

  defp active_run_link(:opentelemetry), do: []

  defp active_run_link(tracer) do
    case Process.get(@run_span_key) do
      nil ->
        []

      span ->
        call_custom_tracer(tracer, :link, [span, %{kind: "run_attempt"}, custom_config(tracer)])
    end
  end

  defp trace_links(_tracer, %{trace_context: nil}), do: []
  defp trace_links(_tracer, %{trace_context: ""}), do: []

  defp trace_links(:opentelemetry, %{trace_context: trace_context})
       when is_binary(trace_context) do
    case parse_traceparent(trace_context) do
      {:ok, trace_id, span_id, flags} ->
        remote_span = apply(:otel_tracer, :from_remote_span, [trace_id, span_id, flags])

        [
          apply(:"Elixir.OpenTelemetry", :link, [
            remote_span,
            %{"continuum.link.type" => "original_trace_context"}
          ])
        ]

      :error ->
        []
    end
  rescue
    _ -> []
  end

  defp trace_links(tracer, %{trace_context: trace_context}) when is_binary(trace_context) do
    call_custom_tracer(tracer, :traceparent_links, [trace_context, custom_config(tracer)])
  end

  defp trace_links(_tracer, _metadata), do: []

  defp parse_traceparent(traceparent) do
    with [_, trace_id_hex, span_id_hex, flags_hex] <-
           Regex.run(
             ~r/\A[[:xdigit:]]{2}-([[:xdigit:]]{32})-([[:xdigit:]]{16})-([[:xdigit:]]{2})\z/,
             traceparent
           ),
         {trace_id, ""} <- Integer.parse(trace_id_hex, 16),
         {span_id, ""} <- Integer.parse(span_id_hex, 16),
         {flags, ""} <- Integer.parse(flags_hex, 16),
         true <- trace_id > 0 and span_id > 0 do
      {:ok, trace_id, span_id, flags}
    else
      _ -> :error
    end
  end

  defp call_custom_tracer({module, config}, function, args) do
    apply(module, function, replace_config(args, config))
  rescue
    UndefinedFunctionError -> fallback_custom(function)
  end

  defp call_custom_tracer(module, function, args) when is_atom(module) do
    apply(module, function, args)
  rescue
    UndefinedFunctionError -> fallback_custom(function)
  end

  defp replace_config(args, config), do: List.replace_at(args, length(args) - 1, config)

  defp custom_config({_module, config}), do: config
  defp custom_config(_module), do: nil

  defp fallback_custom(:link), do: []
  defp fallback_custom(:traceparent_links), do: []
  defp fallback_custom(:add_event), do: :ok
  defp fallback_custom(_function), do: nil

  defp span_started?(:no_span), do: false
  defp span_started?(nil), do: false
  defp span_started?(_span), do: true

  defp activity_span_key(metadata) do
    {@activity_span_key,
     Map.get(metadata, :task_id) || {Map.get(metadata, :run_id), Map.get(metadata, :mfa)}}
  end

  defp run_attributes(metadata) do
    metadata
    |> take_attributes(%{
      instance: "continuum.instance",
      run_id: "continuum.run_id",
      workflow: "continuum.workflow",
      lease_token: "continuum.lease_token",
      resumed?: "continuum.resumed"
    })
    |> maybe_put("continuum.trace_context", Map.get(metadata, :trace_context))
  end

  defp activity_attributes(metadata) do
    metadata
    |> take_attributes(%{
      instance: "continuum.instance",
      run_id: "continuum.run_id",
      task_id: "continuum.task_id",
      attempt: "continuum.attempt"
    })
    |> Map.merge(mfa_attributes(Map.get(metadata, :mfa)))
  end

  defp terminal_attributes(event, metadata) do
    metadata
    |> take_attributes(%{
      reason: "continuum.reason",
      error: "continuum.error",
      next_attempt: "continuum.next_attempt",
      retry_at: "continuum.retry_at"
    })
    |> Map.put("continuum.terminal_event", event_name(event))
  end

  defp event_attributes(measurements, metadata) do
    measurements
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      maybe_put(acc, "continuum.measurement.#{key}", value)
    end)
    |> Map.merge(
      take_attributes(metadata, %{
        run_id: "continuum.run_id",
        task_id: "continuum.task_id",
        timer_id: "continuum.timer_id",
        signal_name: "continuum.signal_name",
        seq: "continuum.seq",
        attempt: "continuum.attempt",
        next_attempt: "continuum.next_attempt",
        fires_at: "continuum.fires_at",
        retry_at: "continuum.retry_at",
        error: "continuum.error",
        reason: "continuum.reason"
      })
    )
  end

  defp take_attributes(metadata, mapping) do
    Enum.reduce(mapping, %{}, fn {source, target}, attrs ->
      maybe_put(attrs, target, Map.get(metadata, source))
    end)
  end

  defp mfa_attributes({module, function, args}) do
    %{
      "continuum.activity_module" => inspect(module),
      "continuum.activity_function" => to_string(function),
      "continuum.activity_arity" => length(args || [])
    }
  end

  defp mfa_attributes(_mfa), do: %{}

  defp maybe_put(attrs, _key, nil), do: attrs
  defp maybe_put(attrs, key, value), do: Map.put(attrs, key, attribute_value(value))

  defp attribute_value(value) when is_binary(value) or is_boolean(value) or is_integer(value),
    do: value

  defp attribute_value(value) when is_float(value), do: value
  defp attribute_value(value) when is_atom(value), do: inspect(value)

  defp attribute_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp attribute_value(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)

  defp attribute_value(value), do: inspect(value, limit: 20, printable_limit: 200)

  defp event_name(event) do
    event
    |> Enum.drop(1)
    |> Enum.map_join(".", &to_string/1)
  end

  defp span_status([:continuum, :run, :failed]), do: :error
  defp span_status([:continuum, :run, :lease_lost]), do: :error
  defp span_status([:continuum, :activity, :failed]), do: :error
  defp span_status([:continuum, :activity, :retried]), do: :error
  defp span_status(_event), do: :unset

  @doc false
  def events, do: @events
end
