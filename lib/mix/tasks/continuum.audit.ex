defmodule Mix.Tasks.Continuum.Audit do
  @moduledoc """
  Audits loaded Continuum workflows and durable patch markers.

      mix continuum.audit --repo MyApp.Repo
      mix continuum.audit --repo MyApp.Repo --format json
      mix continuum.audit --repo MyApp.Repo --strict
  """
  use Mix.Task

  import Ecto.Query

  alias Continuum.Schema.{ActivityTask, Event, Run, WorkflowVersion}

  @shortdoc "Audits determinism metadata and stale patch markers"
  @non_terminal_states ~w(running suspended stuck_unknown_version)

  @impl true
  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args, switches: [repo: :string, format: :string, strict: :boolean])

    Mix.Task.run("app.start")

    repo = parse_repo(opts)
    report = build_report(repo)

    case Keyword.get(opts, :format, "text") do
      "json" -> Mix.shell().info(Jason.encode!(report))
      _ -> print_text(report)
    end

    if Keyword.get(opts, :strict, false) and strict_failure?(report) do
      System.halt(1)
    end
  end

  defp build_report(repo) do
    entries = Continuum.VersionRegistry.entries()

    workflows =
      entries
      |> Enum.sort_by(&{&1.workflow_string, Base.encode16(&1.version_hash)})
      |> Enum.map(&audit_workflow(repo, &1))

    %{
      workflows: workflows,
      workflow_registration: workflow_registration(repo, entries),
      stuck_unknown_version_runs: stuck_unknown_version_count(repo),
      expired_leased_activity_tasks: expired_leased_task_count(repo)
    }
  end

  defp workflow_registration(repo, entries) do
    durable_versions =
      repo.all(from(v in WorkflowVersion, select: {v.workflow, v.version_hash}))
      |> MapSet.new()

    missing =
      entries
      |> Enum.reject(&MapSet.member?(durable_versions, {&1.workflow_string, &1.version_hash}))
      |> Enum.map(fn entry ->
        %{
          workflow: entry.workflow_string,
          version_hash: Base.encode16(entry.version_hash, case: :lower)
        }
      end)

    registrar = Continuum.VersionRegistry.status()

    %{
      registrar_state: to_string(registrar.state),
      registered_count: registrar.registered_count,
      pending_count: Map.get(registrar, :pending_count, 0),
      last_error: registrar.last_error,
      loaded_versions: length(entries),
      durable_loaded_versions: length(entries) - length(missing),
      missing_durable_versions: missing
    }
  end

  defp audit_workflow(repo, entry) do
    metadata = entry.entrypoint.__continuum_workflow__()
    patch_sites = Map.get(metadata, :patch_sites, [])

    %{
      workflow: entry.workflow_string,
      version_hash: Base.encode16(entry.version_hash, case: :lower),
      patch_sites: Enum.map(patch_sites, &audit_patch_site(repo, entry, &1))
    }
  end

  defp audit_patch_site(repo, entry, site) do
    runs =
      repo.all(
        from(r in Run,
          where:
            r.workflow == ^entry.workflow_string and
              r.version_hash == ^entry.version_hash and
              r.state in ^@non_terminal_states,
          select: r.id
        )
      )

    patched_events = patched_events(repo, runs)

    pre_patch_count =
      Enum.count(runs, fn run_id ->
        not Enum.any?(Map.get(patched_events, run_id, []), &matches_site?(&1, site.command_id))
      end)

    first_seen_at =
      patched_events
      |> Map.values()
      |> List.flatten()
      |> Enum.filter(&matches_site?(&1, site.command_id))
      |> Enum.map(& &1.inserted_at)
      |> Enum.min(DateTime, fn -> nil end)

    %{
      name: inspect(site.name),
      file: site.file,
      line: site.line,
      verdict: if(pre_patch_count == 0, do: "safe-to-remove", else: "still-in-use"),
      in_flight_pre_patch: pre_patch_count,
      first_seen_at: first_seen_at
    }
  end

  defp patched_events(_repo, []), do: %{}

  defp patched_events(repo, run_ids) do
    repo.all(
      from(e in Event,
        where: e.run_id in ^run_ids and e.event_type == "patched",
        order_by: [asc: e.inserted_at]
      )
    )
    |> Enum.map(fn event ->
      payload = Continuum.DurableTerm.decode!(event.payload)
      %{run_id: event.run_id, inserted_at: event.inserted_at, command_id: payload.command_id}
    end)
    |> Enum.group_by(& &1.run_id)
  end

  defp matches_site?(%{command_id: command_id}, base) when is_tuple(command_id) do
    tuple_size(command_id) == tuple_size(base) + 1 and
      Tuple.delete_at(command_id, tuple_size(command_id) - 1) == base
  end

  defp matches_site?(_event, _base), do: false

  defp stuck_unknown_version_count(repo) do
    repo.one(from(r in Run, where: r.state == "stuck_unknown_version", select: count(r.id)))
  end

  # Tasks still 'leased' past their lease expiry are stranded between worker
  # death and the steady-state sweep — a persistent non-zero count means the
  # sweep is not running or workers are dying faster than it rescues.
  defp expired_leased_task_count(repo) do
    repo.one(
      from(t in ActivityTask,
        where: t.state == "leased" and t.lease_expires_at < fragment("clock_timestamp()"),
        select: count(t.id)
      )
    )
  end

  defp strict_failure?(%{
         workflows: workflows,
         workflow_registration: registration,
         stuck_unknown_version_runs: stuck
       }) do
    stuck > 0 or registration.missing_durable_versions != [] or
      Enum.any?(workflows, fn workflow ->
        Enum.any?(workflow.patch_sites, &(&1.verdict == "safe-to-remove"))
      end)
  end

  defp print_text(report) do
    Mix.shell().info("Continuum audit")
    Mix.shell().info("stuck_unknown_version_runs: #{report.stuck_unknown_version_runs}")

    Mix.shell().info("expired_leased_activity_tasks: #{report.expired_leased_activity_tasks}")

    registration = report.workflow_registration

    Mix.shell().info(
      "workflow_registration: #{registration.registrar_state}, " <>
        "loaded=#{registration.loaded_versions}, " <>
        "durable=#{registration.durable_loaded_versions}, " <>
        "missing=#{length(registration.missing_durable_versions)}, " <>
        "pending=#{registration.pending_count}"
    )

    Enum.each(registration.missing_durable_versions, fn missing ->
      Mix.shell().info("  missing #{missing.workflow} #{missing.version_hash}")
    end)

    Enum.each(report.workflows, fn workflow ->
      Mix.shell().info("#{workflow.workflow} #{workflow.version_hash}")

      Enum.each(workflow.patch_sites, fn site ->
        Mix.shell().info(
          "  patch #{site.name}: #{site.verdict}, pre_patch=#{site.in_flight_pre_patch}, first_seen=#{format_seen(site.first_seen_at)}"
        )
      end)
    end)
  end

  defp format_seen(nil), do: "-"
  defp format_seen(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp parse_repo(opts) do
    case opts[:repo] do
      nil ->
        Application.get_env(:continuum, :repo) ||
          Mix.raise("no repo configured. Pass --repo MyApp.Repo or set :continuum, :repo")

      repo ->
        Module.concat([repo])
    end
  end
end
