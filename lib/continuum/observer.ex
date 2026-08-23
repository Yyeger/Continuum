defmodule Continuum.Observer do
  @moduledoc """
  Data and action helpers for the optional Continuum Observer.

  The Observer is mounted from a host Phoenix router with
  `Continuum.Observer.Router.continuum_observer/2`. Continuum does not start an
  Observer supervisor and does not provide authentication; mount it only inside
  an authenticated admin scope.

  Query helpers in this module are Phoenix-independent and operate on the
  configured Continuum instance repo. Event payloads are decoded through
  `Continuum.DurableTerm.decode!/1`, which never creates atoms; the Observer is
  not a boundary for untrusted database writes, but a corrupt payload surfaces
  as a `{:decode_error, _}` cell rather than as an atom-table leak.
  """

  import Ecto.Query

  alias Continuum.Runtime.Instance
  alias Continuum.Schema.{ActivityTask, Event, Run}

  @runs_topic "continuum:runs"
  @default_event_limit 50
  @max_event_limit 100
  @default_payload_bytes 65_536
  @default_display_bytes 4_096
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
  @spec list_runs(keyword()) ::
          {:ok, Continuum.Page.t(Continuum.Run.t())} | {:error, term()}
  def list_runs(opts \\ []) do
    opts |> observer_query_opts() |> Continuum.Query.list()
  end

  @doc """
  Loads one run for the Observer detail view.
  """
  @spec get_run(binary(), keyword()) ::
          {:ok, Continuum.Run.t()} | {:error, :not_found | term()}
  def get_run(run_id, opts \\ []) do
    Continuum.Query.get_run(run_id, observer_query_opts(opts))
  end

  @doc """
  Builds the operational health report shown by the Observer health panel.
  """
  @spec health(keyword()) :: {:ok, map()} | {:error, term()}
  def health(opts \\ []), do: Continuum.Health.report(opts)

  @doc """
  Plans or executes a fenced operational repair from the Observer.

  Repairs remain dry-run by default; pass `execute: true` after presenting an
  explicit confirmation to the operator.
  """
  @spec repair_health(atom() | binary(), binary(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def repair_health(action, subject_id, opts \\ []) do
    Continuum.Health.repair(action, subject_id, opts)
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
  Lists a bounded keyset page of decoded journal events ordered by sequence.

  Pass `:after_seq` to continue from a previous page, `:limit` (capped at
  #{@max_event_limit}), `:max_payload_bytes` to reject oversized encoded
  payloads before decoding, and `:redactor` as a unary function or module that
  exports `redact/1`. The configured `:observer_redactor` application setting
  is used when `:redactor` is omitted.
  """
  @spec list_events(binary(), keyword()) ::
          {:ok, Continuum.Page.t(map())} | {:error, term()}
  def list_events(run_id, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      limit = event_limit(opts)
      after_seq = after_seq(opts)

      query =
        from(e in Event,
          where: e.run_id == ^run_id,
          order_by: [asc: e.seq, asc: e.inserted_at],
          limit: ^(limit + 1)
        )

      query = if after_seq, do: where(query, [e], e.seq > ^after_seq), else: query
      rows = instance.repo.all(query)
      page_rows = Enum.take(rows, limit)
      entries = Enum.map(page_rows, &decode_event(&1, opts))

      next_cursor =
        if length(rows) > limit, do: page_rows |> List.last() |> Map.fetch!(:seq), else: nil

      {:ok, %Continuum.Page{entries: entries, per_page: limit, next_cursor: next_cursor}}
    end
  end

  @doc """
  Lists bounded activity task state and last-heartbeat progress for a run.

  Heartbeat details pass through the same configurable Observer redactor as
  event payloads.
  """
  @spec list_activity_tasks(binary(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_activity_tasks(run_id, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      tasks =
        instance.repo.all(
          from(t in ActivityTask,
            where: t.run_id == ^run_id,
            order_by: [asc: t.seq, asc: t.id],
            limit: 100
          )
        )
        |> Enum.map(fn task ->
          decoded_task = decode_payload(task.mfa, opts)

          %{
            id: task.id,
            run_id: task.run_id,
            seq: task.seq,
            state: Continuum.DurableTerm.atom_from_binary!(task.state, :activity_task_state),
            attempt: task.attempt,
            queue: task.queue,
            priority: task.priority,
            mfa: Map.get(decoded_task, :mfa, decoded_task),
            last_heartbeat_at: task.last_heartbeat_at,
            heartbeat_details: decode_payload(task.heartbeat_details, opts)
          }
        end)

      {:ok, tasks}
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

  @doc "Returns the signal contracts declared by the run's workflow version."
  @spec signal_contracts(binary(), keyword()) ::
          {:ok, Continuum.SignalContract.contracts()} | {:error, term()}
  def signal_contracts(run_id, opts \\ []) do
    with {:ok, instance} <- repo_instance(opts) do
      Continuum.SignalContract.contracts_for_run(
        instance,
        Continuum.Runtime.Journal.Postgres,
        run_id
      )
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
  Pretty prints an event payload for display with a hard byte cap.
  """
  @spec pretty(term(), keyword()) :: binary()
  def pretty(term, opts \\ []) do
    max_bytes = positive_option!(opts, :max_bytes, @default_display_bytes)

    term
    |> inspect(pretty: true, limit: 50, printable_limit: max_bytes, width: 100)
    |> truncate_display(max_bytes)
  end

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

  defp decode_event(%Event{} = event, opts) do
    type = Continuum.EventType.from_string!(event.event_type)
    payload = decode_payload(event.payload, opts)

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
    Continuum.DurableTerm.decode!(binary)
  rescue
    error -> {:decode_error, error}
  end

  defp decode_term(other), do: other

  defp decode_payload(payload, opts) do
    max_payload_bytes = positive_option!(opts, :max_payload_bytes, @default_payload_bytes)

    case payload do
      payload when is_binary(payload) and byte_size(payload) > max_payload_bytes ->
        %{omitted: :payload_too_large, encoded_bytes: byte_size(payload)}

      payload ->
        payload |> decode_term() |> redact(opts)
    end
  end

  defp observer_query_opts(opts) do
    opts = Keyword.put_new(opts, :max_payload_bytes, @default_payload_bytes)

    if Keyword.has_key?(opts, :redactor) do
      opts
    else
      Keyword.put(opts, :redactor, Application.get_env(:continuum, :observer_redactor))
    end
  end

  defp event_limit(opts) do
    opts
    |> positive_option!(:limit, @default_event_limit)
    |> min(@max_event_limit)
  end

  defp after_seq(opts) do
    case Keyword.get(opts, :after_seq) do
      nil ->
        nil

      value when is_integer(value) and value >= 0 ->
        value

      value ->
        raise ArgumentError,
              "expected :after_seq to be a non-negative integer, got: #{inspect(value)}"
    end
  end

  defp positive_option!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "expected #{inspect(key)} to be a positive integer, got: #{inspect(value)}"
    end
  end

  defp redact(payload, opts) do
    case Keyword.get(opts, :redactor, Application.get_env(:continuum, :observer_redactor)) do
      nil -> payload
      redactor when is_function(redactor, 1) -> redactor.(payload)
      module when is_atom(module) -> module.redact(payload)
      redactor -> raise ArgumentError, "invalid Observer redactor: #{inspect(redactor)}"
    end
  end

  defp truncate_display(text, max_bytes) when byte_size(text) <= max_bytes, do: text

  defp truncate_display(text, max_bytes) do
    text
    |> binary_part(0, max_bytes)
    |> valid_prefix()
    |> Kernel.<>("…")
  end

  defp valid_prefix(prefix) do
    if String.valid?(prefix),
      do: prefix,
      else: valid_prefix(binary_part(prefix, 0, byte_size(prefix) - 1))
  end

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
