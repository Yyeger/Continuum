defmodule Continuum.Oban do
  @moduledoc """
  Optional Oban activity executor integration.

  This module is inert unless an instance is configured with
  `activity_executor: {:oban, opts}` and the host application has started an
  Oban supervision tree. Continuum keeps `continuum_activity_tasks` as the
  source of truth; Oban jobs carry only stable task identifiers and the worker
  claims the task row when the job performs.
  """

  alias Continuum.Runtime.Instance

  @default_queue :continuum_activities
  @default_unique_period 60
  @default_ttl_seconds 30

  @doc false
  def enqueue(%Instance{activity_executor: {:oban, opts}} = instance, %{id: id, attempt: attempt}) do
    with {:module, oban} <- Code.ensure_loaded(Oban),
         {:module, worker} <- Code.ensure_loaded(Continuum.Oban.Worker) do
      args = %{
        instance: encode_instance(instance.name),
        task_id: id,
        attempt: attempt,
        ttl_seconds: Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)
      }

      changeset = apply(worker, :new, [args, job_opts(opts)])
      apply(oban, :insert, [oban_name(opts), changeset, []])
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  def enqueue(%Instance{} = instance, _task) do
    {:error, {:invalid_activity_executor, instance.activity_executor}}
  end

  @doc false
  def decode_instance(encoded) when is_binary(encoded) do
    encoded
    |> Base.decode64!()
    |> :erlang.binary_to_term()
  end

  defp job_opts(opts) do
    [
      queue: Keyword.get(opts, :queue, @default_queue),
      max_attempts: 1,
      unique: unique_opts(opts)
    ]
  end

  defp unique_opts(opts) do
    [
      period: Keyword.get(opts, :unique_period, @default_unique_period),
      fields: [:args, :worker],
      keys: [:instance, :task_id, :attempt],
      states: [:available, :scheduled, :executing, :retryable]
    ]
  end

  defp oban_name(opts), do: Keyword.get(opts, :name, Oban)

  defp encode_instance(name) do
    name
    |> :erlang.term_to_binary()
    |> Base.encode64()
  end
end
