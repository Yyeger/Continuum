defmodule Continuum.Replay do
  @moduledoc """
  Replay a journaled history against workflow code, without touching anything.

  This is the replay loop with the runtime removed: no lease is taken, no
  engine is started, no row is written. The context is handed its history and
  snapshot up front and carries `Continuum.Runtime.Journal.ReadOnly`, so the
  two paths that used to mutate during "read-only" replay — the in-memory
  adapter running an activity body inline past the journaled tail, and the
  Postgres adapter consuming a pending signal row to resolve a tail await —
  both refuse instead.

  `run/4` replays a history you already hold. `of_run/2` loads one out of
  Postgres first, resolving the run's journaled `(workflow, version_hash)` pair
  through `Continuum.VersionRegistry` exactly the way a resuming engine would,
  and reports what the workflow *would* do next.

  `mix continuum.replay` is the operator-facing front end for `of_run/2`.

  ## Outcomes

  Replay ends in one of four ways:

    * `{:ok, result}` — the history carried the workflow to a return value.
    * `{:suspended, reason}` — the workflow stopped at a pending effect. A
      `{:history_exhausted, _}` reason means the history ran out *before* the
      workflow was done, which under a live journal is where the next effect
      would have been performed.
    * `{:continued, next_run_id}` — the workflow tail-called `continue_as_new/1`.
    * `{:error, reason}` — most usefully `{:error, {:error, %Continuum.ReplayDriftError{}, _}}`,
      which names the cursor and command id where code and history disagree.
  """
  @moduledoc since: "0.8.0"

  import Ecto.Query

  alias Continuum.Runtime.{Context, Instance, Journal}
  alias Continuum.Schema.Run

  @type result ::
          {:ok, term()}
          | {:suspended, term()}
          | {:continued, binary()}
          | {:error, term()}

  @typedoc "What `of_run/2` reports back about a durable run."
  @type report :: %{
          run_id: binary(),
          workflow: String.t(),
          version_hash: binary(),
          entrypoint: module(),
          namespace: String.t(),
          stored_state: atom(),
          stored_result: term(),
          event_count: non_neg_integer(),
          snapshot: %{through_seq: integer(), format_version: integer()} | nil,
          outcome: :completed | :suspended | :continued | :drift | :error,
          detail: term(),
          agrees_with_stored_result?: boolean() | nil
        }

  @doc """
  Replay `history` against `workflow_module` and return the outcome.

  Options:

    * `:run_id` — the id reported in drift errors. Defaults to `"continuum-replay"`.
    * `:snapshot` — a `Continuum.Snapshot` to replay the compacted prefix from.
      Ignored when its version hash does not match the module's.
    * `:instance` — the instance whose name appears in the context.
    * `:journal` — defaults to `Continuum.Runtime.Journal.ReadOnly`. Overriding
      it gives up the read-only guarantee; `Continuum.Test` does so deliberately
      for adapter-specific tests.
    * `:lease_token` — only meaningful alongside a writable `:journal`.
    * `:follow_journaled_entrypoint` — when replay drifts on the very first
      event because the history names a generated `V_<hash>` entrypoint, retry
      through that entrypoint. Defaults to `true`, which is what makes
      `replay(MyFlow, ...)` work against a durable history. `of_run/2` sets it
      to `false`: it has already resolved the entrypoint, and following the
      journaled one would silently override `:against`.
  """
  @spec run(module(), term(), [map()], keyword()) :: result()
  def run(workflow_module, input, history, opts \\ []) do
    result = do_run(workflow_module, input, history, opts)

    if Keyword.get(opts, :follow_journaled_entrypoint, true) do
      retry_through_generated_entrypoint(result, workflow_module, input, history, opts)
    else
      result
    end
  end

  @doc """
  Load a durable run and replay it read-only.

  Resolves the run's journaled `(workflow, version_hash)` through
  `Continuum.VersionRegistry`. A version this node cannot load is reported as
  `{:error, {:unknown_version, _}}` rather than replayed against whatever code
  happens to be loaded — replaying the wrong version reports drift that is an
  artifact of the deploy, not of the run.

  Options:

    * `:instance` / `:repo` — where to read from.
    * `:snapshot` — `false` to ignore stored snapshots and replay from events
      alone. Defaults to `true`.
    * `:against` — replay against this module instead of the journaled
      entrypoint, to see what a code change would do to a run in flight.
    * `:redactor` — applied to the reported result and suspend reason. Defaults
      to the `:observer_redactor` application setting.
  """
  @spec of_run(binary(), keyword()) :: {:ok, report()} | {:error, term()}
  def of_run(run_id, opts \\ []) when is_binary(run_id) do
    instance = resolve_instance(opts)

    with {:ok, run} <- fetch_run(instance, run_id),
         {:ok, entrypoint} <- resolve_entrypoint(run, opts) do
      {snapshot, events} = replay_history(instance, run_id, opts)
      input = Continuum.DurableTerm.decode!(run.input)

      outcome =
        run(entrypoint, input, events,
          run_id: run_id,
          instance: instance.name,
          snapshot: snapshot,
          follow_journaled_entrypoint: false
        )

      {:ok, build_report(run, entrypoint, events, snapshot, outcome, opts)}
    end
  end

  # ---------------------------------------------------------------------------
  # Kernel

  defp do_run(workflow_module, input, history, opts) do
    snapshot = compatible_snapshot(Keyword.get(opts, :snapshot), workflow_module)

    ctx = %Context{
      run_id: Keyword.get(opts, :run_id, "continuum-replay"),
      history: history,
      history_offset: history_offset(snapshot),
      snapshot_steps: snapshot_steps(snapshot),
      cursor: 0,
      workflow_module: workflow_module,
      lease_token: Keyword.get(opts, :lease_token),
      instance: Instance.lookup(Keyword.get(opts, :instance, Continuum)),
      journal: Keyword.get(opts, :journal, Journal.ReadOnly)
    }

    Context.put(ctx)

    try do
      result = workflow_module.run(input)

      case unconsumed_history(history) do
        nil -> {:ok, result}
        leftover -> {:error, {:history_not_consumed, leftover}}
      end
    catch
      {token, reason} when token == :continuum_suspend ->
        {:suspended, reason}

      {token, next_run_id} when token == :continuum_continued_as_new ->
        {:continued, next_run_id}

      kind, reason ->
        {:error, {kind, reason, __STACKTRACE__}}
    after
      Context.clear()
    end
  end

  defp unconsumed_history(history) do
    ctx = Context.get()
    consumed = ctx.cursor
    expected = (ctx.history_offset || 0) + length(history)

    if consumed == expected, do: nil, else: %{consumed: consumed, expected: expected}
  end

  # A history journaled by a durable run carries the generated `V_<hash>`
  # entrypoint in its command ids, so replaying it against the public module
  # drifts on the very first event. When the drift names an entrypoint this
  # node has loaded, replay again through it.
  defp retry_through_generated_entrypoint(
         {:error, {:error, %Continuum.ReplayDriftError{expected: expected}, _stacktrace}} = result,
         workflow_module,
         input,
         history,
         opts
       ) do
    entrypoint = expected_entrypoint(expected)

    if entrypoint && entrypoint != workflow_module do
      do_run(entrypoint, input, history, opts)
    else
      result
    end
  end

  defp retry_through_generated_entrypoint(result, _workflow_module, _input, _history, _opts) do
    result
  end

  defp expected_entrypoint(expected) when is_map(expected) do
    expected
    |> Map.get(:command_id, Map.get(expected, "command_id"))
    |> command_entrypoint()
  end

  defp expected_entrypoint(_expected), do: nil

  defp command_entrypoint(module) when is_atom(module) do
    if generated_workflow_entrypoint?(module) and function_exported?(module, :run, 1) do
      module
    end
  end

  defp command_entrypoint(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.find_value(&command_entrypoint/1)
  end

  defp command_entrypoint(list) when is_list(list),
    do: Enum.find_value(list, &command_entrypoint/1)

  defp command_entrypoint(_other), do: nil

  defp generated_workflow_entrypoint?(module) do
    metadata = module.__continuum_workflow__()
    metadata.entrypoint == module and Map.get(metadata, :source_module, module) != module
  rescue
    UndefinedFunctionError -> false
  end

  @doc false
  def compatible_snapshot(nil, _workflow_module), do: nil

  def compatible_snapshot(%Continuum.Snapshot{version_hash: hash} = snapshot, workflow_module) do
    if hash == workflow_version_hash(workflow_module), do: snapshot, else: nil
  end

  defp workflow_version_hash(workflow_module) do
    workflow_module.__continuum_workflow__().version_hash
  rescue
    UndefinedFunctionError -> <<0::256>>
  end

  defp history_offset(nil), do: 0
  defp history_offset(%Continuum.Snapshot{through_seq: through_seq}), do: through_seq + 1

  defp snapshot_steps(nil), do: %{}
  defp snapshot_steps(%Continuum.Snapshot{steps_by_seq: steps}), do: steps || %{}

  # ---------------------------------------------------------------------------
  # Durable lookup

  defp replay_history(instance, run_id, opts) do
    if Keyword.get(opts, :snapshot, true) do
      Journal.Postgres.load_for_replay(instance, run_id)
    else
      # `load_for_replay/2` intentionally returns only the suffix after the
      # newest snapshot. Dropping that snapshot after the read would strand the
      # suffix at cursor zero, so event-only replay must load the full journal.
      {nil, Journal.Postgres.load(instance, run_id)}
    end
  end

  defp resolve_instance(opts) do
    case Keyword.get(opts, :repo) do
      nil -> Instance.lookup(Keyword.get(opts, :instance, Continuum))
      repo -> %{Instance.lookup(Keyword.get(opts, :instance, Continuum)) | repo: repo}
    end
  end

  defp fetch_run(%{repo: nil}, _run_id), do: {:error, :no_repo}

  defp fetch_run(instance, run_id) do
    case instance.repo.one(from(r in Run, where: r.id == ^run_id)) do
      nil -> {:error, {:run_not_found, run_id}}
      run -> {:ok, run}
    end
  end

  defp resolve_entrypoint(run, opts) do
    case Keyword.get(opts, :against) do
      nil ->
        case Continuum.VersionRegistry.resolve(run.workflow, run.version_hash) do
          {:ok, %{entrypoint: entrypoint}} ->
            {:ok, entrypoint}

          {:error, _reason} ->
            {:error,
             {:unknown_version,
              %{
                workflow: run.workflow,
                version_hash: printable_hash(run.version_hash)
              }}}
        end

      module ->
        case Code.ensure_loaded(module) do
          {:module, ^module} -> {:ok, entrypoint_of(module)}
          _ -> {:error, {:module_not_loaded, module}}
        end
    end
  end

  defp entrypoint_of(module) do
    module.__continuum_workflow__().entrypoint
  rescue
    UndefinedFunctionError -> module
  end

  defp build_report(run, entrypoint, events, snapshot, outcome, opts) do
    {kind, detail} = classify(outcome)
    stored_result = decode_stored(run.result)

    %{
      run_id: run.id,
      workflow: run.workflow,
      version_hash: printable_hash(run.version_hash),
      entrypoint: entrypoint,
      namespace: run.namespace,
      stored_state: Continuum.DurableTerm.atom_from_binary!(run.state, :run_state),
      stored_result: redact(stored_result, opts),
      event_count: length(events),
      snapshot: snapshot_summary(snapshot),
      outcome: kind,
      detail: redact(detail, opts),
      agrees_with_stored_result?: agrees?(kind, outcome, run, stored_result)
    }
  end

  defp classify({:ok, result}), do: {:completed, result}
  defp classify({:suspended, reason}), do: {:suspended, reason}
  defp classify({:continued, next_run_id}), do: {:continued, next_run_id}

  defp classify({:error, {:error, %Continuum.ReplayDriftError{} = drift, _stacktrace}}) do
    {:drift,
     %{
       cursor: drift.cursor,
       expected: drift.expected,
       actual: drift.actual
     }}
  end

  defp classify({:error, reason}), do: {:error, reason}

  # Only a completed replay has something to compare; anything else says
  # nothing about whether the stored terminal result was right.
  defp agrees?(:completed, {:ok, result}, %{state: "completed"}, stored_result),
    do: result == stored_result

  defp agrees?(:completed, _outcome, _run, _stored), do: false
  defp agrees?(_kind, _outcome, _run, _stored), do: nil

  defp snapshot_summary(nil), do: nil

  defp snapshot_summary(%Continuum.Snapshot{} = snapshot) do
    %{through_seq: snapshot.through_seq, format_version: Continuum.Snapshot.format_version()}
  end

  # `compute_version_hash/2` already returns lowercase hex, so the column holds
  # printable text. Encoding it again would print the hex of the hex.
  defp printable_hash(nil), do: ""

  defp printable_hash(hash) when is_binary(hash) do
    if String.printable?(hash), do: hash, else: Base.encode16(hash, case: :lower)
  end

  defp decode_stored(nil), do: nil
  defp decode_stored(binary) when is_binary(binary), do: Continuum.DurableTerm.decode!(binary)

  defp redact(value, opts) do
    case Keyword.get(opts, :redactor, Application.get_env(:continuum, :observer_redactor)) do
      nil -> value
      redactor when is_function(redactor, 1) -> redactor.(value)
      module when is_atom(module) -> module.redact(value)
      other -> raise ArgumentError, "invalid replay redactor: #{inspect(other)}"
    end
  end
end
