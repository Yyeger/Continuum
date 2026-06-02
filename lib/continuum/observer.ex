defmodule Continuum.Observer do
  @moduledoc """
  Data and action helpers for the optional Continuum Observer.

  The Observer is mounted from a host Phoenix router with
  `Continuum.Observer.Router.continuum_observer/2`. Continuum does not start an
  Observer supervisor and does not provide authentication; mount it only inside
  an authenticated admin scope.

  Query helpers in this module are Phoenix-independent and operate on the
  configured Continuum instance repo. Event payloads are decoded with
  `:erlang.binary_to_term/1` because Continuum stores its own trusted journal
  data as `bytea`; the Observer is not a boundary for untrusted database writes.
  """

  import Ecto.Query

  alias Continuum.Runtime.Instance
  alias Continuum.Schema.{Event, Run}

  @runs_topic "continuum:runs"
  @type run_state :: :running | :suspended | :completed | :failed | :cancelled

  @doc """
  Returns the low-fidelity per-instance runs topic used by the Observer index.
  """
  @spec runs_topic() :: binary()
  def runs_topic, do: @runs_topic

  @doc """
  Returns the per-run topic used by run detail pages.
  """
  @spec run_topic(binary()) :: binary()
  def run_topic(run_id), do: "continuum:run:#{run_id}"

  @doc """
  Subscribes the caller to coarse run-index updates for an instance.
  """
  @spec subscribe_runs(keyword()) :: :ok | {:error, term()}
  def subscribe_runs(opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    subscribe(instance, runs_topic())
  end

  @doc """
  Subscribes the caller to full-fidelity updates for a single run.
  """
  @spec subscribe_run(binary(), keyword()) :: :ok | {:error, term()}
  def subscribe_run(run_id, opts \\ []) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))
    subscribe(instance, run_topic(run_id))
  end

  @doc false
  @spec broadcast_run_state_changed(Instance.t(), binary(), run_state()) :: :ok
  def broadcast_run_state_changed(%Instance{} = instance, run_id, state) do
    if Process.whereis(instance.pubsub) do
      Phoenix.PubSub.broadcast(
        instance.pubsub,
        runs_topic(),
        {:run_state_changed, run_id, state}
      )
    end

    :ok
  end

  @doc """
  Lists runs for the Observer index.

  Options:

    * `:instance` - Continuum instance name or struct. Defaults to `Continuum`.
    * `:state` - atom/string run state filter.
    * `:workflow` - workflow module substring filter.
    * `:search` - run id or workflow substring filter.
    * `:page` - 1-based page number.
    * `:per_page` - page size, capped at 100.
  """
  @spec list_runs(keyword()) :: {:ok, map()} | {:error, term()}
  def list_runs(opts \\ []) do
    Continuum.Query.list(opts)
  end

  @doc """
  Loads one run for the Observer detail view.
  """
  @spec get_run(binary(), keyword()) :: {:ok, map()} | {:error, :not_found | term()}
  def get_run(run_id, opts \\ []) do
    Continuum.Query.get_run(run_id, opts)
  end

  @doc """
  Returns the run id that this run continued into via `continue_as_new`, or nil.
  """
  @spec successor_run_id(binary(), keyword()) :: binary() | nil
  def successor_run_id(run_id, opts \\ []) do
    case repo_instance(opts) do
      {:ok, instance} ->
        instance.repo.one(from(r in Run, where: r.continued_from_run_id == ^run_id, select: r.id))

      _ ->
        nil
    end
  end

  @doc """
  Lists decoded journal events for a run ordered by sequence.
  """
  @spec list_events(binary(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_events(run_id, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      events =
        instance.repo.all(
          from(e in Event,
            where: e.run_id == ^run_id,
            order_by: [asc: e.seq, asc: e.inserted_at]
          )
        )
        |> Enum.map(&decode_event/1)

      {:ok, events}
    end
  end

  @doc """
  Cancels a run through the public Continuum API using the Observer instance.
  """
  @spec cancel_run(binary(), keyword()) :: :ok | {:error, term()}
  def cancel_run(run_id, opts \\ []) do
    Continuum.cancel(run_id, observer_runtime_opts(opts))
  end

  @doc """
  Sends a signal through the public Continuum API using the Observer instance.
  """
  @spec send_signal(binary(), atom() | binary(), term(), keyword()) :: :ok | {:error, term()}
  def send_signal(run_id, name, payload, opts \\ []) do
    with {:ok, signal_name} <- normalize_signal_name(name) do
      Continuum.signal(run_id, signal_name, payload, observer_runtime_opts(opts))
    end
  end

  @doc """
  Decodes a JSON payload from the Observer signal form.
  """
  @spec decode_signal_payload(binary()) :: {:ok, term()} | {:error, term()}
  def decode_signal_payload(""), do: {:ok, nil}

  def decode_signal_payload(json) when is_binary(json) do
    Jason.decode(json)
  end

  def decode_signal_payload(payload), do: {:ok, payload}

  @doc """
  Pretty prints an event payload for display.
  """
  @spec pretty(term()) :: binary()
  def pretty(term), do: inspect(term, pretty: true, limit: :infinity, printable_limit: :infinity)

  defp subscribe(instance, topic) do
    if Process.whereis(instance.pubsub) do
      Phoenix.PubSub.subscribe(instance.pubsub, topic)
    else
      {:error, :pubsub_not_started}
    end
  end

  defp repo_instance(opts) do
    instance = Instance.lookup(Keyword.get(opts, :instance, Continuum))

    case instance.repo do
      nil -> {:error, :repo_not_configured}
      _repo -> {:ok, instance}
    end
  end

  defp observer_runtime_opts(opts) do
    instance_name = Keyword.get(opts, :instance, Continuum)
    instance = Instance.lookup(instance_name)
    runtime_opts = [instance: instance_name]

    if instance.repo do
      Keyword.put(runtime_opts, :journal, Continuum.Runtime.Journal.Postgres)
    else
      runtime_opts
    end
  end

  defp decode_event(%Event{} = event) do
    type = String.to_atom(event.event_type)
    payload = decode_term(event.payload)

    %{
      run_id: event.run_id,
      seq: event.seq,
      type: type,
      event_type: type,
      payload: payload,
      inserted_at: event.inserted_at
    }
  end

  defp decode_term(nil), do: nil

  defp decode_term(binary) when is_binary(binary) do
    :erlang.binary_to_term(binary)
  rescue
    error -> {:decode_error, error}
  end

  defp decode_term(other), do: other

  defp normalize_signal_name(name) when is_atom(name), do: {:ok, name}

  defp normalize_signal_name(name) when is_binary(name) do
    name
    |> String.trim()
    |> String.to_existing_atom()
    |> then(&{:ok, &1})
  rescue
    ArgumentError -> {:error, {:unknown_signal, name}}
  end
end
