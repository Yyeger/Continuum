defmodule Continuum.RecurringSchedules.Backfill do
  @moduledoc false

  import Ecto.Query
  alias Continuum.RecurringSchedules
  alias Continuum.Runtime.{Instance, NamespacePrecondition, SQL}
  alias Continuum.Schema.{RecurringSchedule, Schedule}

  def run(id, from_at, until_at, opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    execute = Keyword.get(opts, :execute, false)

    with :ok <- validate_range(from_at, until_at),
         {:ok, query} <- NamespacePrecondition.scope(RecurringSchedule, opts) do
      cond do
        is_nil(instance.repo) ->
          {:error, :repo_not_configured}

        not is_boolean(execute) ->
          {:error, :invalid_execute}

        true ->
          backfill(instance.repo, where(query, [s], s.id == ^id), from_at, until_at, execute)
      end
    end
  end

  defp validate_range(
         %DateTime{time_zone: "Etc/UTC"} = first,
         %DateTime{time_zone: "Etc/UTC"} = last
       ) do
    if DateTime.compare(first, last) == :gt, do: {:error, :invalid_range}, else: :ok
  end

  defp validate_range(_first, _last), do: {:error, :utc_range_required}

  defp backfill(repo, query, from_at, until_at, execute) do
    SQL.transaction(repo, fn ->
      definition = repo.one(SQL.for_update(query, repo)) || repo.rollback(:not_found)
      now = SQL.now(repo)

      if DateTime.compare(until_at, now) == :gt or
           DateTime.compare(until_at, definition.next_occurrence_at) != :lt,
         do: repo.rollback(:range_not_historical)

      step = definition.every_ms * 1_000
      # Anchor to the stored microsecond grid, even when range endpoints fall
      # between slots. Check the budget before building a list or writing rows.
      offset = rem(DateTime.diff(definition.next_occurrence_at, from_at, :microsecond), step)
      span = DateTime.diff(until_at, from_at, :microsecond) - offset
      count = if span < 0, do: 0, else: div(span, step) + 1
      if count > 100, do: repo.rollback(:too_many_occurrences)

      slots =
        for i <- 0..(count - 1)//1, do: DateTime.add(from_at, offset + i * step, :microsecond)

      entries = fill_slots(repo, definition, slots, now, execute)
      %{mode: if(execute, do: :executed, else: :preview), entries: entries}
    end)
  end

  defp fill_slots(_repo, _definition, [], _now, _execute), do: []

  defp fill_slots(repo, definition, slots, now, execute) do
    existing =
      repo.all(
        from(s in Schedule,
          where: s.recurring_schedule_id == ^definition.id and s.occurrence_at in ^slots,
          select: s.occurrence_at
        )
      )
      |> MapSet.new(&DateTime.to_unix(&1, :microsecond))

    skip_overlap = definition.overlap_policy == "skip"
    busy = skip_overlap and RecurringSchedules.outstanding?(repo, definition.id)

    {entries, _busy} =
      Enum.map_reduce(slots, busy, fn at, busy ->
        status =
          cond do
            MapSet.member?(existing, DateTime.to_unix(at, :microsecond)) -> :existing
            busy -> :skipped
            true -> :scheduled
          end

        if execute and status != :existing,
          do: RecurringSchedules.insert_occurrence(repo, definition, at, now, status == :skipped)

        {%{occurrence_at: at, status: status}, busy or (skip_overlap and status == :scheduled)}
      end)

    entries
  end
end
