defmodule Continuum.Schedules do
  @moduledoc """
  Durable one-shot workflow scheduling.

  Schedules preallocate a stable run ID. A runner may retry across any crash
  window, but the same scheduled occurrence can create at most one run.
  """

  import Ecto.Query

  alias Continuum.{DurableTerm, Runtime.Instance}
  alias Continuum.Schema.Schedule

  @doc "Creates a durable one-shot schedule."
  @spec schedule_at(module(), term(), DateTime.t(), keyword()) ::
          {:ok, binary()} | {:error, term()}
  def schedule_at(workflow, input, scheduled_at, opts \\ [])

  def schedule_at(workflow, input, %DateTime{} = scheduled_at, opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    with {:ok, repo} <- fetch_repo(instance),
         :ok <- DurableTerm.validate(input, :schedule_input),
         {:ok, metadata} <- Continuum.VersionRegistry.ensure_registered(workflow, instance),
         {:ok, attributes} <- normalize_attributes(Keyword.get(opts, :attributes, %{})) do
      schedule_id = Keyword.get(opts, :id, Ecto.UUID.generate())
      run_id = Keyword.get(opts, :run_id, Ecto.UUID.generate())
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      changeset =
        %Schedule{}
        |> Ecto.Changeset.change(%{
          id: schedule_id,
          run_id: run_id,
          workflow: metadata.workflow_string,
          version_hash: metadata.version_hash,
          input: :erlang.term_to_binary(input),
          namespace: normalize_namespace(Keyword.get(opts, :namespace, "default")),
          attributes: attributes,
          trace_context: Keyword.get(opts, :trace_context),
          scheduled_at: DateTime.truncate(scheduled_at, :microsecond),
          state: "scheduled",
          attempt: 0,
          inserted_at: now
        })
        |> Ecto.Changeset.unique_constraint(:id, name: :continuum_schedules_pkey)

      case repo.insert(changeset) do
        {:ok, _schedule} -> {:ok, schedule_id}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  def schedule_at(_workflow, _input, scheduled_at, _opts),
    do: {:error, {:invalid_scheduled_at, scheduled_at}}

  @doc "Cancels a schedule that has not begun starting its run."
  @spec cancel(binary(), keyword()) :: :ok | {:error, :not_found | :already_started | term()}
  def cancel(schedule_id, opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    if instance.repo do
      {count, _} =
        instance.repo.update_all(
          from(s in Schedule, where: s.id == ^schedule_id and s.state == "scheduled"),
          set: [state: "cancelled"]
        )

      cond do
        count == 1 ->
          :ok

        instance.repo.exists?(from(s in Schedule, where: s.id == ^schedule_id)) ->
          {:error, :already_started}

        true ->
          {:error, :not_found}
      end
    else
      {:error, :repo_not_configured}
    end
  end

  @doc "Loads a schedule for operational inspection."
  @spec get(binary(), keyword()) :: {:ok, map()} | {:error, :not_found | term()}
  def get(schedule_id, opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case instance.repo && instance.repo.get(Schedule, schedule_id) do
      nil -> {:error, :not_found}
      schedule -> {:ok, decode(schedule)}
    end
  end

  defp decode(schedule) do
    %{
      id: schedule.id,
      run_id: schedule.run_id,
      workflow: schedule.workflow,
      version_hash: schedule.version_hash,
      input: DurableTerm.decode!(schedule.input),
      namespace: schedule.namespace,
      attributes: schedule.attributes,
      scheduled_at: schedule.scheduled_at,
      state: String.to_atom(schedule.state),
      attempt: schedule.attempt,
      started_at: schedule.started_at,
      last_error: schedule.last_error
    }
  end

  defp normalize_namespace(namespace) when is_binary(namespace) and byte_size(namespace) > 0,
    do: namespace

  defp normalize_namespace(value),
    do:
      raise(
        ArgumentError,
        "schedule namespace must be a non-empty binary, got: #{inspect(value)}"
      )

  defp normalize_attributes(attributes) when is_map(attributes) do
    with {:ok, json} <- Jason.encode(attributes), {:ok, decoded} <- Jason.decode(json) do
      {:ok, decoded}
    else
      {:error, reason} -> {:error, {:invalid_attributes, reason}}
    end
  end

  defp normalize_attributes(value), do: {:error, {:invalid_attributes, value}}

  defp fetch_repo(%Instance{repo: nil}), do: {:error, :repo_not_configured}
  defp fetch_repo(%Instance{repo: repo}), do: {:ok, repo}
end
