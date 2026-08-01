defmodule Continuum.Config do
  @moduledoc """
  Central schema and validation for Continuum runtime configuration.

  The schema backs `Continuum.children/1`, direct instance construction, and
  the generated configuration reference.
  """

  @top_schema [
    {:name, "atom", "Continuum", "Runtime instance name."},
    {:repo, "module | nil", "configured repo", "Ecto repo used by durable components."},
    {:journal, "module | nil", "derived", "Journal adapter override."},
    {:workflow_modules, "[module] | nil", "discovered", "Workflow modules registered at boot."},
    {:activity_executor, ":builtin | {:oban, keyword}", ":builtin",
     "Activity execution backend."},
    {:activity_max_concurrency, "positive_integer", "10", "Instance-wide built-in worker limit."},
    {:activity_queues, "map | keyword", "%{}", "Per-queue positive concurrency limits."},
    {:heartbeater, "boolean | keyword", "[]", "Lease heartbeater child options."},
    {:run_supervisor, "boolean | keyword", "[]", "Run supervisor child options."},
    {:activity_supervisor, "boolean | keyword", "[]", "Activity supervisor child options."},
    {:recovery, "boolean | keyword", "[]", "Startup recovery child options."},
    {:dispatcher, "boolean | keyword", "[]", "Run dispatcher child options."},
    {:activity_dispatcher, "boolean | keyword", "[]", "Activity dispatcher child options."},
    {:timer_wheel, "boolean | keyword", "[]", "Timer wheel child options."},
    {:schedule_runner, "boolean | keyword", "[]", "One-shot schedule runner child options."},
    {:signal_router, "boolean | keyword", "[]", "Signal router child options."},
    {:snapshotter, "boolean | keyword", "[]", "Snapshotter child options."},
    {:partition_maintainer, "boolean | keyword", "false",
     "Optional partition DDL child options."},
    {:version_registry, "boolean | keyword", "[]", "Workflow version registrar child options."}
  ]

  @component_schemas %{
    heartbeater: [
      name: :atom,
      interval_ms: :positive,
      ttl_seconds: :positive,
      drain_timeout_ms: :non_negative
    ],
    run_supervisor: [],
    activity_supervisor: [],
    recovery: [enabled?: :boolean, name: :atom],
    dispatcher: [
      enabled?: :boolean,
      interval_ms: :positive,
      batch_size: :positive,
      ttl_seconds: :positive
    ],
    activity_dispatcher: [
      enabled?: :boolean,
      interval_ms: :positive,
      batch_size: :positive,
      ttl_seconds: :positive,
      backpressure_jitter_ms: :non_negative
    ],
    timer_wheel: [
      enabled?: :boolean,
      listen?: :boolean,
      refresh_ms: :positive,
      window_ms: :positive,
      batch_size: :positive
    ],
    schedule_runner: [interval_ms: :positive, batch_size: :positive],
    signal_router: [
      listen?: :boolean,
      catch_up_interval_ms: :positive,
      catch_up_batch_size: :positive
    ],
    snapshotter: [
      snapshot_threshold: :snapshot_threshold,
      snapshot_max_size_bytes: :positive,
      journal: :module
    ],
    partition_maintainer: [
      months: :positive,
      start_month: :date_or_nil,
      interval_ms: :positive,
      initial_delay_ms: :non_negative
    ],
    version_registry: [
      workflow_modules: :modules_or_nil,
      registration_fun: :function_2,
      retry_base_ms: :positive,
      retry_max_ms: :positive
    ]
  }

  @child_keys Map.keys(@component_schemas)
  @instance_keys [
    :name,
    :repo,
    :journal,
    :workflow_modules,
    :activity_executor,
    :activity_max_concurrency,
    :activity_queues,
    :drain_timeout_ms
  ]

  @doc "Return the public runtime option schema used by the docs generator."
  @spec schema() :: [tuple()]
  def schema, do: @top_schema

  @doc false
  def validate_children!(opts) do
    validate_keyword!(opts, :children)
    reject_duplicates!(opts, :children)
    validate_known_keys!(opts, Enum.map(@top_schema, &elem(&1, 0)), :children)

    Enum.each(opts, fn
      {key, value} when key in @child_keys -> validate_child!(key, value)
      {key, value} -> validate_top!(key, value)
    end)

    opts
  end

  @doc false
  def validate_instance!(opts) do
    validate_keyword!(opts, :instance)
    reject_duplicates!(opts, :instance)
    validate_known_keys!(opts, @instance_keys, :instance)
    Enum.each(opts, fn {key, value} -> validate_top!(key, value) end)
    opts
  end

  @doc false
  def validate_component!(component, opts) when is_atom(component) do
    validate_keyword!(opts, component)
    schema = Map.fetch!(@component_schemas, component)
    allowed = [:instance | Keyword.keys(schema)]
    reject_duplicates!(opts, component)
    validate_known_keys!(opts, allowed, component)

    Enum.each(opts, fn
      {:instance, _instance} -> :ok
      {key, value} -> validate_type!(component, key, value, Keyword.fetch!(schema, key))
    end)

    opts
  end

  @doc "Render the committed runtime configuration reference."
  @spec reference_markdown() :: binary()
  def reference_markdown do
    rows =
      Enum.map_join(@top_schema, "\n", fn {key, type, default, description} ->
        "| `#{key}` | `#{markdown_table_cell(type)}` | `#{markdown_table_cell(default)}` | #{description} |"
      end)

    components =
      @component_schemas
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map_join("\n", fn {component, fields} ->
        options = fields |> Keyword.keys() |> Enum.map_join(", ", &"`#{&1}`")
        "- `#{component}`: #{if(options == "", do: "no child-specific options", else: options)}"
      end)

    """
    # Runtime Configuration Reference

    This file is generated from `Continuum.Config.schema/0`. Configure a runtime
    through `Continuum.children/1`; unknown, duplicate, or invalid options raise
    before child specs are returned.

    | Option | Type | Default | Description |
    |---|---|---|---|
    #{rows}

    ## Child option keys

    Each child accepts `false` to omit it, `true` for defaults, or a keyword list:

    #{components}

    Regenerate with `mix continuum.config.docs`; CI can verify the committed file
    with `mix continuum.config.docs --check`.
    """
  end

  defp markdown_table_cell(value), do: String.replace(value, "|", "\\|")

  defp validate_child!(_key, value) when is_boolean(value), do: :ok

  defp validate_child!(key, value) when is_list(value) do
    validate_component!(key, value)
  end

  defp validate_child!(key, value), do: invalid!(key, value, "boolean or keyword list")

  defp validate_top!(:name, value) when is_atom(value), do: :ok
  defp validate_top!(:repo, value) when is_atom(value) or is_nil(value), do: :ok
  defp validate_top!(:journal, value) when is_atom(value) or is_nil(value), do: :ok

  defp validate_top!(:workflow_modules, value),
    do: validate_type!(:children, :workflow_modules, value, :modules_or_nil)

  defp validate_top!(:activity_executor, :builtin), do: :ok

  defp validate_top!(:activity_executor, {:oban, opts}) when is_list(opts),
    do: validate_keyword!(opts, :activity_executor)

  defp validate_top!(:activity_executor, value),
    do: raise(ArgumentError, "invalid Continuum activity executor: #{inspect(value)}")

  defp validate_top!(:activity_max_concurrency, value) when is_integer(value) and value > 0,
    do: :ok

  defp validate_top!(:activity_max_concurrency, value),
    do:
      raise(
        ArgumentError,
        "activity_max_concurrency must be a positive integer, got: #{inspect(value)}"
      )

  defp validate_top!(:activity_queues, value), do: validate_activity_queues!(value)

  defp validate_top!(:drain_timeout_ms, value),
    do: validate_type!(:instance, :drain_timeout_ms, value, :non_negative)

  defp validate_top!(key, value), do: invalid!(key, value, "a valid configured value")

  defp validate_activity_queues!(queues) when is_list(queues) do
    validate_keyword!(queues, :activity_queues)
    validate_activity_queues!(Map.new(queues))
  end

  defp validate_activity_queues!(queues) when is_map(queues) do
    Enum.each(queues, fn {queue, limit} ->
      unless (is_atom(queue) or (is_binary(queue) and byte_size(queue) > 0)) and
               is_integer(limit) and limit > 0 do
        invalid!(:activity_queues, queues, "queue names mapped to positive integers")
      end
    end)
  end

  defp validate_activity_queues!(value),
    do: invalid!(:activity_queues, value, "map or keyword list")

  defp validate_type!(_component, _key, value, :positive) when is_integer(value) and value > 0,
    do: :ok

  defp validate_type!(_component, _key, value, :non_negative)
       when is_integer(value) and value >= 0, do: :ok

  defp validate_type!(_component, _key, value, :boolean) when is_boolean(value), do: :ok
  defp validate_type!(_component, _key, value, :atom) when is_atom(value), do: :ok
  defp validate_type!(_component, _key, value, :module) when is_atom(value), do: :ok
  defp validate_type!(_component, _key, nil, :modules_or_nil), do: :ok

  defp validate_type!(_component, _key, value, :modules_or_nil) when is_list(value) do
    if Enum.all?(value, &is_atom/1),
      do: :ok,
      else: invalid!(:workflow_modules, value, "list of modules")
  end

  defp validate_type!(_component, _key, value, :function_2) when is_function(value, 2), do: :ok
  defp validate_type!(_component, _key, :infinity, :snapshot_threshold), do: :ok

  defp validate_type!(_component, _key, value, :snapshot_threshold)
       when is_integer(value) and value > 0, do: :ok

  defp validate_type!(_component, _key, nil, :date_or_nil), do: :ok
  defp validate_type!(_component, _key, %Date{}, :date_or_nil), do: :ok

  defp validate_type!(component, key, value, type),
    do: invalid!("#{component}.#{key}", value, inspect(type))

  defp validate_keyword!(value, context) do
    unless Keyword.keyword?(value), do: invalid!(context, value, "keyword list")
  end

  defp validate_known_keys!(opts, allowed, context) do
    unknown = Keyword.keys(opts) -- allowed

    if unknown != [],
      do: raise(ArgumentError, "unknown Continuum #{context} options: #{inspect(unknown)}")
  end

  defp reject_duplicates!(opts, context) do
    duplicates =
      opts
      |> Keyword.keys()
      |> Enum.frequencies()
      |> Enum.filter(fn {_key, count} -> count > 1 end)
      |> Keyword.keys()

    if duplicates != [],
      do: raise(ArgumentError, "duplicate Continuum #{context} options: #{inspect(duplicates)}")
  end

  defp invalid!(key, value, expected) do
    raise ArgumentError,
          "invalid Continuum option #{inspect(key)}: expected #{expected}, got: #{inspect(value)}"
  end
end
