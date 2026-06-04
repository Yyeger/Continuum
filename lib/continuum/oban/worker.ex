if Code.ensure_loaded?(Oban.Worker) do
  defmodule Continuum.Oban.Worker do
    @moduledoc false

    use Oban.Worker,
      queue: :continuum_activities,
      max_attempts: 1

    require Logger

    alias Continuum.Runtime.{ActivityWorker, ActivityWorker.Dispatcher, Instance}

    @impl Oban.Worker
    def perform(%Oban.Job{id: job_id, args: args}) do
      instance_name = args |> fetch_arg!("instance") |> Continuum.Oban.decode_instance()
      task_id = fetch_arg!(args, "task_id")
      attempt = fetch_arg!(args, "attempt")
      ttl_seconds = fetch_arg(args, "ttl_seconds", 30)

      instance = Instance.lookup(instance_name)
      owner = owner(instance, job_id)

      case Dispatcher.claim_one(instance, task_id, attempt, owner, ttl_seconds) do
        {:ok, task} ->
          task
          |> Map.put(:oban_job_id, job_id)
          |> ActivityWorker.execute()

          :ok

        status when status in [:not_available, :stale] ->
          :ok

        {:error, reason} ->
          Logger.error("Continuum Oban activity claim failed: #{inspect(reason)}")
          {:cancel, reason}
      end
    end

    defp fetch_arg!(args, key) do
      case fetch_arg(args, key, :error) do
        :error -> raise KeyError, key: key, term: args
        value -> value
      end
    end

    defp fetch_arg(args, key, default) when is_binary(key) do
      Map.get(args, key, Map.get(args, String.to_atom(key), default))
    end

    defp owner(instance, job_id) do
      "#{node()}/#{instance.name}/oban-#{job_id}:activity"
    end
  end
else
  defmodule Continuum.Oban.Worker do
    @moduledoc false
  end
end
