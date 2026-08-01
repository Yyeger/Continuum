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
  @max_instance_bytes 1_024

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
  def decode_instance(%{"kind" => "atom", "name" => name}), do: existing_instance_atom!(name)
  def decode_instance(%{kind: "atom", name: name}), do: existing_instance_atom!(name)

  def decode_instance(%{"kind" => "string", "name" => name}), do: instance_string!(name)
  def decode_instance(%{kind: "string", name: name}), do: instance_string!(name)

  # Decode jobs enqueued before v0.6.4 without allowing external terms to
  # create atoms, functions, references, or other unsafe VM objects.
  def decode_instance(encoded)
      when is_binary(encoded) and byte_size(encoded) <= @max_instance_bytes * 2 do
    with {:ok, binary} when byte_size(binary) <= @max_instance_bytes <- Base.decode64(encoded),
         name when is_atom(name) or is_binary(name) <- :erlang.binary_to_term(binary, [:safe]) do
      name
    else
      _ -> raise ArgumentError, "invalid legacy Continuum instance identifier"
    end
  rescue
    _ -> raise ArgumentError, "invalid legacy Continuum instance identifier"
  end

  def decode_instance(_encoded), do: raise(ArgumentError, "invalid Continuum instance identifier")

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

  defp encode_instance(name) when is_atom(name),
    do: %{"kind" => "atom", "name" => Atom.to_string(name)}

  defp encode_instance(name) when is_binary(name),
    do: %{"kind" => "string", "name" => instance_string!(name)}

  defp encode_instance(name) do
    raise ArgumentError,
          "expected the Continuum instance name to be an atom or string, got: #{inspect(name)}"
  end

  defp existing_instance_atom!(name)
       when is_binary(name) and byte_size(name) <= @max_instance_bytes do
    String.to_existing_atom(name)
  rescue
    ArgumentError -> raise ArgumentError, "unknown Continuum instance atom: #{inspect(name)}"
  end

  defp existing_instance_atom!(_name),
    do: raise(ArgumentError, "invalid Continuum instance atom identifier")

  defp instance_string!(name)
       when is_binary(name) and byte_size(name) > 0 and byte_size(name) <= @max_instance_bytes,
       do: name

  defp instance_string!(_name),
    do: raise(ArgumentError, "invalid Continuum instance string identifier")
end
