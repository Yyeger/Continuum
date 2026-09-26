defmodule Continuum.RecurringSchedules do
  @moduledoc """
  UTC interval schedule definitions, separate from their one-shot occurrences.

  Definitions pin a workflow version and input. The existing schedule runner
  atomically advances each definition's cursor while inserting uniquely named
  occurrences. Each occurrence then uses the ordinary one-shot retry path and
  stable run ID. No additional runtime process is required.
  """

  import Ecto.Query
  alias Continuum.{DurableTerm, Page, Runtime.Instance}
  alias Continuum.Schema.{RecurringSchedule, Run, Schedule}

  @doc """
  Creates an interval definition and returns its ID.

  `every_ms` must be at least 1,000. `:starts_at` is an optional UTC DateTime;
  its default is one interval from now. Calendar/cron and non-UTC zones are
  not supported: intervals measure elapsed milliseconds and have no DST rules.

  Required options:

    * `overlap: :allow | :skip` — allow simultaneous occurrences, or record
      an overlap skip while an earlier occurrence is queued/starting or has
      any active run in its continuation chain.
    * `missed: :catch_up | :skip` — generate overdue times oldest-first, or
      coalesce overdue times into the most recent due time and advance beyond
      now. Coalescing does not create rows for the older omitted times.

  `:max_catch_up` (1–100, default 10) bounds occurrences per definition per
  poll; the runner's batch size also bounds the total across definitions.
  Overlap skips count against this budget. Pausing retains the cursor; resume
  applies the same missed policy. Existing occurrences continue while paused.

  Also accepts `:instance`, `:id`, `:namespace`, `:attributes`, and
  `:trace_context`. A duplicate definition ID returns a changeset error;
  use the existing definition ID for inspection or pause/resume.
  """
  @spec create(module(), term(), pos_integer(), keyword()) :: {:ok, binary()} | {:error, term()}
  def create(workflow, input, every_ms, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts),
         {:ok, settings} <- settings(every_ms, opts),
         :ok <- DurableTerm.validate(input, :schedule_input),
         {:ok, metadata} <- Continuum.VersionRegistry.ensure_registered(workflow, instance),
         {:ok, attributes} <- attributes(Keyword.get(opts, :attributes, %{})) do
      changeset =
        %RecurringSchedule{}
        |> Ecto.Changeset.change(
          Map.merge(settings, %{
            id: Keyword.get_lazy(opts, :id, &Ecto.UUID.generate/0),
            workflow: metadata.workflow_string,
            version_hash: metadata.version_hash,
            input: :erlang.term_to_binary(input),
            attributes: attributes,
            trace_context: Keyword.get(opts, :trace_context),
            inserted_at: DateTime.utc_now(),
            state: "active"
          })
        )
        |> Ecto.Changeset.unique_constraint(:id, name: :continuum_recurring_schedules_pkey)

      case instance.repo.insert(changeset) do
        {:ok, row} -> {:ok, row.id}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @doc "Loads a definition, including its next occurrence cursor and recorded policies."
  def get(id, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      case instance.repo.get(RecurringSchedule, id) do
        nil -> {:error, :not_found}
        row -> {:ok, decode(row)}
      end
    end
  end

  @doc "Lists definitions with `:limit` (1–100), `:cursor`, and optional `:namespace`."
  def list(opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      query = from(s in RecurringSchedule)

      query =
        if opts[:namespace], do: where(query, [s], s.namespace == ^opts[:namespace]), else: query

      page(instance.repo, query, opts, &decode/1)
    end
  end

  @doc false
  def set_state(id, state, opts) when state in [:active, :paused] do
    with {:ok, instance} <- repo_instance(opts) do
      case instance.repo.update_all(from(s in RecurringSchedule, where: s.id == ^id),
             set: [state: Atom.to_string(state)]
           ) do
        {1, _} -> :ok
        {0, _} -> {:error, :not_found}
      end
    end
  end

  @doc "Lists occurrences with `:limit` (1–100) and `:cursor`; each row includes its stable run ID."
  def occurrences(id, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      page(
        instance.repo,
        from(s in Schedule, where: s.recurring_schedule_id == ^id),
        opts,
        fn row -> row |> Map.from_struct() |> Map.drop([:__meta__, :input, :trace_context]) end
      )
    end
  end

  @doc false
  def materialize(%Instance{repo: nil}, _budget), do: {:error, :repo_not_configured}

  def materialize(instance, budget) when is_integer(budget) and budget > 0 do
    budget = min(budget, 1_000)

    instance.repo.transaction(fn ->
      [[now]] = instance.repo.query!("SELECT clock_timestamp()").rows

      definitions =
        instance.repo.all(
          from(s in RecurringSchedule,
            where: s.state == "active" and s.next_occurrence_at <= ^now,
            order_by: [asc: s.next_occurrence_at, asc: s.id],
            limit: ^budget,
            lock: "FOR UPDATE SKIP LOCKED"
          )
        )

      Enum.reduce_while(definitions, 0, fn definition, used ->
        count = materialize_definition(instance.repo, definition, now, budget - used)
        total = used + count
        if total >= budget, do: {:halt, total}, else: {:cont, total}
      end)
    end)
  end

  def materialize(_instance, budget), do: {:error, {:invalid_batch_size, budget}}

  defp materialize_definition(repo, definition, now, budget) do
    due_count =
      div(DateTime.diff(now, definition.next_occurrence_at, :millisecond), definition.every_ms) +
        1

    {first, count} =
      if definition.missed_policy == "skip" do
        {DateTime.add(
           definition.next_occurrence_at,
           (due_count - 1) * definition.every_ms,
           :millisecond
         ), 1}
      else
        {definition.next_occurrence_at, min(due_count, min(budget, definition.max_catch_up))}
      end

    Enum.each(0..(count - 1), fn index ->
      at = DateTime.add(first, index * definition.every_ms, :millisecond)
      skipped? = definition.overlap_policy == "skip" and outstanding?(repo, definition.id)

      occurrence = %{
        id: Ecto.UUID.generate(),
        run_id: Ecto.UUID.generate(),
        recurring_schedule_id: definition.id,
        occurrence_at: at,
        workflow: definition.workflow,
        version_hash: definition.version_hash,
        input: definition.input,
        namespace: definition.namespace,
        attributes: definition.attributes,
        trace_context: definition.trace_context,
        scheduled_at: at,
        inserted_at: now,
        attempt: 0,
        state: if(skipped?, do: "skipped", else: "scheduled"),
        last_error: if(skipped?, do: "overlap policy skipped this occurrence", else: nil)
      }

      repo.insert_all(Schedule, [occurrence],
        on_conflict: :nothing,
        conflict_target: [:recurring_schedule_id, :occurrence_at]
      )
    end)

    repo.update_all(from(s in RecurringSchedule, where: s.id == ^definition.id),
      set: [next_occurrence_at: DateTime.add(first, count * definition.every_ms, :millisecond)]
    )

    count
  end

  defp outstanding?(repo, id) do
    repo.exists?(
      from(s in Schedule,
        as: :occurrence,
        where: s.recurring_schedule_id == ^id,
        where:
          s.state in ["scheduled", "starting"] or
            exists(
              from(r in Run,
                where: r.correlation_id == parent_as(:occurrence).run_id,
                where: r.state in ["running", "suspended"]
              )
            )
      )
    )
  end

  defp settings(every_ms, opts) do
    overlap = Keyword.get(opts, :overlap)
    missed = Keyword.get(opts, :missed)
    max_catch_up = Keyword.get(opts, :max_catch_up, 10)
    namespace = Keyword.get(opts, :namespace, "default")

    cond do
      not is_integer(every_ms) or every_ms < 1_000 ->
        {:error, :invalid_interval}

      overlap not in [:allow, :skip] ->
        {:error, :invalid_overlap_policy}

      missed not in [:skip, :catch_up] ->
        {:error, :invalid_missed_policy}

      not is_integer(max_catch_up) or max_catch_up not in 1..100 ->
        {:error, :invalid_max_catch_up}

      not is_binary(namespace) or namespace == "" ->
        {:error, :invalid_namespace}

      true ->
        settings_with_start(every_ms, overlap, missed, max_catch_up, namespace, opts)
    end
  end

  defp settings_with_start(every_ms, overlap, missed, max_catch_up, namespace, opts) do
    starts_at =
      Keyword.get_lazy(opts, :starts_at, fn ->
        DateTime.add(DateTime.utc_now(), every_ms, :millisecond)
      end)

    case starts_at do
      %DateTime{time_zone: "Etc/UTC", utc_offset: 0, std_offset: 0} ->
        {:ok,
         %{
           every_ms: every_ms,
           overlap_policy: Atom.to_string(overlap),
           missed_policy: Atom.to_string(missed),
           max_catch_up: max_catch_up,
           next_occurrence_at: DateTime.truncate(starts_at, :microsecond),
           namespace: namespace
         }}

      _ ->
        {:error, :utc_start_required}
    end
  end

  defp attributes(value) when is_map(value) do
    with {:ok, encoded} <- Jason.encode(value), do: Jason.decode(encoded)
  end

  defp attributes(_), do: {:error, :invalid_attributes}

  defp decode(row) do
    row
    |> Map.from_struct()
    |> Map.delete(:__meta__)
    |> Map.update!(:input, &DurableTerm.decode!/1)
  end

  defp page(repo, query, opts, decode) do
    limit = Keyword.get(opts, :limit, 50)

    if not is_integer(limit) or limit not in 1..100,
      do: raise(ArgumentError, "limit must be 1–100")

    query = if opts[:cursor], do: where(query, [s], s.id > ^opts[:cursor]), else: query
    rows = repo.all(from(s in query, order_by: [asc: s.id], limit: ^(limit + 1)))
    entries = Enum.take(rows, limit)
    cursor = if length(rows) > limit, do: List.last(entries).id, else: nil
    {:ok, %Page{entries: Enum.map(entries, decode), per_page: limit, next_cursor: cursor}}
  end

  defp repo_instance(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    if instance.repo, do: {:ok, instance}, else: {:error, :repo_not_configured}
  end
end
