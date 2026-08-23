defmodule Mix.Tasks.Continuum.Replay do
  @moduledoc """
  Replays a durable run's journaled history against the code on this node.

      mix continuum.replay <run_id> --repo MyApp.Repo
      mix continuum.replay <run_id> --repo MyApp.Repo --format json
      mix continuum.replay <run_id> --repo MyApp.Repo --no-snapshot
      mix continuum.replay <run_id> --repo MyApp.Repo --against MyApp.Orders.Checkout
      mix continuum.replay <run_id> --repo MyApp.Repo --strict

  Nothing is written. The run keeps its lease, its history, and its pending
  signals; see `Continuum.Replay` for why that takes a dedicated journal
  adapter rather than a flag.

  Use it on a run that is wedged, or on one that failed with a
  `Continuum.ReplayDriftError`, to get back one of:

    * `completed` — the workflow reaches a return value from this history.
    * `suspended` — it stops at a pending effect, with the reason. A
      `history_exhausted` reason names the cursor and the effect it would have
      performed next.
    * `continued` — it tail-calls `continue_as_new/1`.
    * `drift` — the exact cursor where code and history disagree, with the
      journaled event and the effect the code asked for.

  ## Options

    * `--repo` — the Ecto repo to read from. Defaults to `:continuum, :repo`.
    * `--instance` — a named runtime instance. Defaults to the default instance.
    * `--against` — replay against another workflow module, to see what a code
      change would do to a run already in flight. The journaled version is used
      when omitted.
    * `--no-snapshot` — ignore stored snapshots and replay from events alone.
      Useful for confirming a snapshot and its events agree.
    * `--format json` — machine-readable output.
    * `--strict` — exit 1 unless the outcome is `completed` or `suspended`.

  A version this node cannot load is reported as `unknown_version`, not as
  drift: the mismatch is in the deploy, not in the run.
  """
  @shortdoc "Replays a run's history read-only and reports the outcome"

  use Mix.Task

  @impl true
  def run(args) do
    {opts, positional, _} =
      OptionParser.parse(args,
        switches: [
          repo: :string,
          instance: :string,
          against: :string,
          snapshot: :boolean,
          format: :string,
          strict: :boolean
        ]
      )

    run_id = parse_run_id(positional)
    Mix.Task.run("app.start")

    case Continuum.Replay.of_run(run_id, replay_opts(opts)) do
      {:ok, report} ->
        emit(report, opts)
        maybe_halt(report, opts)

      {:error, reason} ->
        fail(reason, opts)
    end
  end

  defp parse_run_id([run_id]), do: run_id

  defp parse_run_id(_other) do
    Mix.raise("expected exactly one run id: mix continuum.replay <run_id> --repo MyApp.Repo")
  end

  defp replay_opts(opts) do
    []
    |> put_present(:repo, opts[:repo] && Module.concat([opts[:repo]]))
    |> put_present(:instance, opts[:instance] && Module.concat([opts[:instance]]))
    |> put_present(:against, opts[:against] && Module.concat([opts[:against]]))
    |> Keyword.put(:snapshot, Keyword.get(opts, :snapshot, true))
  end

  defp put_present(opts, _key, nil), do: opts
  defp put_present(opts, key, value), do: Keyword.put(opts, key, value)

  defp emit(report, opts) do
    case Keyword.get(opts, :format, "text") do
      "json" -> Mix.shell().info(Jason.encode!(jsonable(report)))
      _ -> print_text(report)
    end
  end

  # `Mix.raise` rather than `System.halt/1`: it exits non-zero all the same, and
  # it keeps the failure inspectable from a test. `--strict` keeps the halt so
  # it matches `mix continuum.audit`.
  defp fail(reason, opts) do
    message = describe_error(reason)

    if Keyword.get(opts, :format) == "json" do
      Mix.shell().info(Jason.encode!(%{outcome: "error", detail: message}))
    end

    Mix.raise(message)
  end

  defp describe_error({:unknown_version, %{workflow: workflow, version_hash: hash}}) do
    "this node cannot replay this version: no loaded code registers " <>
      "#{workflow} at version #{hash}. Deploy the release that ran it, or pass " <>
      "--against to replay through a different module."
  end

  defp describe_error({:run_not_found, run_id}), do: "no run #{run_id} in this repo"

  defp describe_error({:module_not_loaded, module}),
    do: "--against #{inspect(module)} is not loaded by this node"

  defp describe_error(:no_repo),
    do: "no repo configured. Pass --repo MyApp.Repo or set :continuum, :repo"

  defp describe_error(reason), do: "replay failed: #{inspect(reason)}"

  defp print_text(report) do
    Mix.shell().info("Continuum replay #{report.run_id}")
    Mix.shell().info("workflow: #{report.workflow} #{report.version_hash}")
    Mix.shell().info("entrypoint: #{inspect(report.entrypoint)}")
    Mix.shell().info("namespace: #{report.namespace}")
    Mix.shell().info("stored_state: #{report.stored_state}")
    Mix.shell().info("events: #{report.event_count}#{snapshot_note(report.snapshot)}")
    Mix.shell().info("")
    Mix.shell().info("outcome: #{report.outcome}")
    print_detail(report)
    print_agreement(report)
  end

  defp print_detail(%{outcome: :completed, detail: result}) do
    Mix.shell().info("result: #{inspect(result, pretty: true)}")
  end

  defp print_detail(%{outcome: :suspended, detail: {:history_exhausted, detail}}) do
    Mix.shell().info("history ends at cursor #{detail.cursor}")
    Mix.shell().info("next effect: #{inspect(detail.effect, pretty: true)}")
  end

  defp print_detail(%{outcome: :suspended, detail: reason}) do
    Mix.shell().info("reason: #{inspect(reason, pretty: true)}")
  end

  defp print_detail(%{outcome: :continued, detail: next_run_id}) do
    Mix.shell().info("continued as: #{next_run_id}")
  end

  defp print_detail(%{outcome: :drift, detail: detail}) do
    Mix.shell().info("cursor: #{detail.cursor}")
    Mix.shell().info("journaled: #{inspect(detail.expected, pretty: true)}")
    Mix.shell().info("code asked for: #{inspect(detail.actual, pretty: true)}")
  end

  defp print_detail(%{detail: detail}) do
    Mix.shell().info("detail: #{inspect(detail, pretty: true)}")
  end

  defp print_agreement(%{agrees_with_stored_result?: nil}), do: :ok

  defp print_agreement(%{agrees_with_stored_result?: true}) do
    Mix.shell().info("agrees with the stored terminal result")
  end

  defp print_agreement(%{agrees_with_stored_result?: false, stored_result: stored}) do
    Mix.shell().info("DIFFERS from the stored terminal result: #{inspect(stored, pretty: true)}")
  end

  defp snapshot_note(nil), do: " (no snapshot)"

  defp snapshot_note(%{through_seq: through_seq}),
    do: " (replayed from a snapshot through seq #{through_seq})"

  defp jsonable(report) do
    %{
      run_id: report.run_id,
      workflow: report.workflow,
      version_hash: report.version_hash,
      entrypoint: inspect(report.entrypoint),
      namespace: report.namespace,
      stored_state: to_string(report.stored_state),
      stored_result: inspect(report.stored_result),
      event_count: report.event_count,
      snapshot_through_seq: report.snapshot && report.snapshot.through_seq,
      outcome: to_string(report.outcome),
      detail: inspect(report.detail),
      agrees_with_stored_result: report.agrees_with_stored_result?
    }
  end

  defp maybe_halt(report, opts) do
    if Keyword.get(opts, :strict, false) and report.outcome not in [:completed, :suspended] do
      System.halt(1)
    end
  end
end
