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
  @default_per_page 25
  @max_per_page 100

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
    with {:ok, instance} <- repo_instance(opts) do
      page = opts |> Keyword.get(:page, 1) |> positive_integer(1)

      per_page =
        opts |> Keyword.get(:per_page, @default_per_page) |> positive_integer(@default_per_page)

      per_page = min(per_page, @max_per_page)
      offset = (page - 1) * per_page

      query =
        Run
        |> filter_state(Keyword.get(opts, :state))
        |> filter_workflow(Keyword.get(opts, :workflow))
        |> filter_search(Keyword.get(opts, :search))

      total = instance.repo.one(from(r in query, select: count(r.id)))

      entries =
        instance.repo.all(
          from(r in query,
            order_by: [desc: r.started_at, desc: r.id],
            limit: ^per_page,
            offset: ^offset
          )
        )
        |> Enum.map(&decode_run/1)

      {:ok,
       %{
         entries: entries,
         page: page,
         per_page: per_page,
         total: total,
         total_pages: max(ceil_div(total, per_page), 1)
       }}
    end
  end

  @doc """
  Loads one run for the Observer detail view.
  """
  @spec get_run(binary(), keyword()) :: {:ok, map()} | {:error, :not_found | term()}
  def get_run(run_id, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      case instance.repo.one(from(r in Run, where: r.id == ^run_id)) do
        nil -> {:error, :not_found}
        run -> {:ok, decode_run(run)}
      end
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

  defp filter_state(query, nil), do: query
  defp filter_state(query, ""), do: query

  defp filter_state(query, state) do
    state = state |> to_string() |> String.downcase()

    case state do
      "cancelled" ->
        cancelled = encode_term(:cancelled)
        from(r in query, where: r.state == "failed" and r.error == ^cancelled)

      "failed" ->
        cancelled = encode_term(:cancelled)

        from(r in query,
          where: r.state == "failed" and (is_nil(r.error) or r.error != ^cancelled)
        )

      _ ->
        from(r in query, where: r.state == ^state)
    end
  end

  defp filter_workflow(query, nil), do: query
  defp filter_workflow(query, ""), do: query

  defp filter_workflow(query, workflow) do
    pattern = "%#{workflow}%"
    from(r in query, where: ilike(r.workflow, ^pattern))
  end

  defp filter_search(query, nil), do: query
  defp filter_search(query, ""), do: query

  defp filter_search(query, search) do
    pattern = "%#{search}%"

    from(r in query,
      where: fragment("?::text ILIKE ?", r.id, ^pattern) or ilike(r.workflow, ^pattern)
    )
  end

  defp decode_run(%Run{} = run) do
    error = decode_term(run.error)

    %{
      id: run.id,
      run_id: run.id,
      workflow: run.workflow,
      state: display_state(run.state, error),
      input: decode_term(run.input),
      result: decode_term(run.result),
      error: error,
      trace_context: run.trace_context,
      started_at: run.started_at,
      completed_at: run.completed_at,
      lease_owner: run.lease_owner,
      lease_token: run.lease_token,
      lease_expires_at: run.lease_expires_at,
      next_wakeup_at: run.next_wakeup_at,
      retention_until: run.retention_until
    }
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

  defp display_state("failed", :cancelled), do: :cancelled
  defp display_state(state, _error), do: String.to_atom(state)

  defp encode_term(term), do: :erlang.term_to_binary(term)

  defp normalize_signal_name(name) when is_atom(name), do: {:ok, name}

  defp normalize_signal_name(name) when is_binary(name) do
    name
    |> String.trim()
    |> String.to_existing_atom()
    |> then(&{:ok, &1})
  rescue
    ArgumentError -> {:error, {:unknown_signal, name}}
  end

  defp positive_integer(value, _fallback) when is_integer(value) and value > 0, do: value

  defp positive_integer(value, fallback) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> fallback
    end
  end

  defp positive_integer(_value, fallback), do: fallback

  defp ceil_div(0, _denominator), do: 0
  defp ceil_div(numerator, denominator), do: div(numerator + denominator - 1, denominator)
end
