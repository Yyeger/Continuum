## Boot, narration, and evidence-printing for `mix continuum.demo`.

defmodule ContinuumDemo.Out do
  @moduledoc false

  def continuum(msg), do: tag(:cyan, "continuum", msg)
  def workflow(msg), do: tag(:green, "workflow", msg)
  def demo(msg), do: tag(:yellow, "demo", msg)
  def ledger(msg), do: tag(:magenta, "ledger", msg)

  def bang(msg) do
    IO.puts([IO.ANSI.red(), IO.ANSI.bright(), "[demo] *** ", msg, " ***", IO.ANSI.reset()])
  end

  def rule(title) do
    IO.puts([
      "\n",
      IO.ANSI.faint(),
      "── ",
      title,
      " ",
      String.duplicate("─", max(0, 66 - String.length(title))),
      IO.ANSI.reset()
    ])
  end

  def blank, do: IO.puts("")

  defp tag(color, label, msg) do
    IO.puts([apply(IO.ANSI, color, []), "[", label, "] ", IO.ANSI.reset(), to_string(msg)])
  end

  # stdout to a terminal is a message to the group leader, and `:erlang.halt/1`
  # does not drain it. Everything printed right before a deliberate kill goes
  # through here.
  def flush do
    Logger.flush()
    Process.sleep(120)
  end
end

defmodule ContinuumDemo.Boot do
  @moduledoc false

  alias ContinuumDemo.Out

  @doc """
  Configures the demo, starts the repo, and starts :continuum — but none of the
  runtime pollers.

  Split from `start_runtime!/1` on purpose: phase 2 wants to read the crashed
  run's journal *before* anything can resume it, and reading needs a registered
  instance with a live repo.
  """
  def connect! do
    Logger.configure(level: :warning)

    Application.put_env(:continuum, ContinuumDemo.Repo, ContinuumDemo.db_config())
    Application.put_env(:continuum, :ecto_repos, [ContinuumDemo.Repo])
    Application.put_env(:continuum, :repo, ContinuumDemo.Repo)
    Application.put_env(:continuum, :journal, Continuum.Runtime.Journal.Postgres)
    Application.put_env(:continuum, :workflow_modules, [ContinuumDemo.Checkout])

    ensure_database!()

    # Continuum never starts the host application's repo — that is the host's
    # job, and the required order is repo first, Continuum children second.
    {:ok, _} = start_repo()
    Ecto.Migrator.run(ContinuumDemo.Repo, "priv/test_repo/migrations", :up, all: true, log: false)

    {:ok, _} = Application.ensure_all_started(:continuum)
    :ok
  end

  @doc """
  Starts the runtime pollers: exactly what a host app puts in its own
  supervision tree. Pass `pollers?: false` for a read-only node.
  """
  def start_runtime!(opts \\ []) do
    ttl = ContinuumDemo.lease_ttl_seconds()
    Application.put_env(:continuum, :heartbeater, ttl_seconds: ttl, interval_ms: 3_000)

    # A read-only node (the Observer pane) must not resume the crashed run out
    # from under the `--resume` pane. Same code, pollers off.
    if Keyword.get(opts, :pollers?, true) do
      Application.put_env(:continuum, :dispatcher, interval_ms: 500, ttl_seconds: ttl)
      Application.put_env(:continuum, :activity_dispatcher, interval_ms: 300)
      Application.put_env(:continuum, :timer_wheel, [])
      Application.put_env(:continuum, :signal_router, [])
      Application.put_env(:continuum, :recovery, [])
    else
      Application.put_env(:continuum, :dispatcher, false)
      Application.put_env(:continuum, :activity_dispatcher, false)
      Application.put_env(:continuum, :activity_worker, false)
      Application.put_env(:continuum, :timer_wheel, false)
      Application.put_env(:continuum, :recovery, false)
      Application.put_env(:continuum, :signal_router, listen?: false)
    end

    {:ok, sup} =
      Supervisor.start_link(
        Continuum.children(
          repo: ContinuumDemo.Repo,
          workflow_modules: [ContinuumDemo.Checkout]
        ),
        strategy: :one_for_one,
        name: ContinuumDemo.Supervisor
      )

    Process.unlink(sup)
    :ok
  end

  @doc "Creates the demo database if it is missing. Idempotent."
  def ensure_database! do
    config = ContinuumDemo.db_config() ++ [name: nil]

    case Ecto.Adapters.Postgres.storage_up(config) do
      :ok ->
        Out.demo("created database #{Keyword.fetch!(config, :database)}")

      {:error, :already_up} ->
        :ok

      {:error, reason} ->
        Out.bang("cannot reach Postgres: #{inspect(reason)}")
        Out.demo("start it with:  docker compose up -d      (or podman-compose up -d)")
        System.halt(1)
    end

    :ok
  end

  @doc """
  Empties every Continuum table so a demo run starts from nothing.

  Truncation rather than DROP DATABASE: phase 1 halts the VM with `kill -9`
  semantics, so Postgres keeps its side of those sockets around for a while and
  `storage_down` fails with `object_in_use`.
  """
  def clean_slate! do
    {:ok, %{rows: rows}} =
      ContinuumDemo.Repo.query("""
      SELECT tablename FROM pg_tables
      WHERE schemaname = 'public' AND tablename LIKE 'continuum%'
      """)

    case Enum.map(rows, fn [table] -> ~s("#{table}") end) do
      [] ->
        :ok

      tables ->
        {:ok, _} = ContinuumDemo.Repo.query("TRUNCATE TABLE #{Enum.join(tables, ", ")} CASCADE")
        Out.demo("cleared #{length(tables)} continuum table(s)")
    end

    :ok
  end

  # Unlinked so the repo outlives the `mix run` eval process.
  defp start_repo do
    pid =
      case ContinuumDemo.Repo.start_link() do
        {:ok, pid} -> pid
        {:error, {:already_started, pid}} -> pid
      end

    Process.unlink(pid)
    {:ok, pid}
  end
end

defmodule ContinuumDemo.Narrator do
  @moduledoc false

  alias ContinuumDemo.Out

  @events [
    [:continuum, :run, :started],
    [:continuum, :run, :suspended],
    [:continuum, :run, :completed],
    [:continuum, :run, :failed],
    [:continuum, :workflow, :log],
    [:continuum, :activity, :scheduled],
    [:continuum, :activity, :completed],
    [:continuum, :dispatcher, :claimed],
    [:continuum, :recovery, :completed],
    [:continuum, :lease, :acquired]
  ]

  @doc """
  Attaches the demo's narration.

  `:kill_on_step` names a `Continuum.log/3` checkpoint; when the workflow
  reaches it the node is killed with `:erlang.halt/1`. The handler runs inside
  the engine process, synchronously, between the journal append for that log
  event and the next line of workflow code — so "the shipment activity had not
  been scheduled yet" is a fact about the code path, not a race we won.
  """
  def attach(opts \\ []) do
    # `mix run --no-start` leaves :telemetry down, and the handler table is a
    # process. Phase 2 attaches before booting Continuum so the boot-time
    # recovery scan is narrated too.
    {:ok, _} = Application.ensure_all_started(:telemetry)

    :telemetry.attach_many(
      "continuum-demo-narrator",
      @events,
      &__MODULE__.handle/4,
      Map.new(opts)
    )
  end

  def handle([:continuum, :workflow, :log], _measure, meta, config) do
    Out.workflow(meta.message)

    # Counted so `--resume` can state, as a number rather than a claim, how
    # many log lines this VM emitted versus how many the journal already held.
    if ref = config[:log_counter], do: :counters.add(ref, 1, 1)

    if config[:kill_on_step] && config[:kill_on_step] == step(meta) do
      Out.blank()
      Out.bang("KILLING THE BEAM (erlang:halt/1, no shutdown, no cleanup)")
      Out.demo("the shipment has not been scheduled — the charge is already journaled")
      Out.flush()
      :erlang.halt(9)
    end
  end

  def handle([:continuum, :run, :started], _m, meta, _c) do
    if meta[:resumed?] do
      Out.continuum("run #{short(meta[:run_id])} resumed — replaying its journal from event 0")
    else
      Out.continuum("run #{short(meta[:run_id])} started")
    end
  end

  def handle([:continuum, :run, :suspended], _m, meta, _c),
    do: Out.continuum("run #{short(meta[:run_id])} suspended — waiting on durable work")

  def handle([:continuum, :run, :completed], _m, meta, _c),
    do: Out.continuum("run #{short(meta[:run_id])} completed")

  def handle([:continuum, :run, :failed], _m, meta, _c),
    do: Out.continuum("run #{short(meta[:run_id])} FAILED")

  def handle([:continuum, :activity, :scheduled], _m, meta, _c),
    do: Out.continuum("activity scheduled: #{mfa(meta[:mfa])}")

  def handle([:continuum, :activity, :completed], _m, meta, _c),
    do: Out.continuum("activity completed: #{mfa(meta[:mfa])}")

  def handle([:continuum, :dispatcher, :claimed], %{count: count}, _meta, _c) when count > 0,
    do: Out.continuum("dispatcher claimed #{count} orphaned run(s) — lease had expired")

  def handle([:continuum, :recovery, :completed], _m, meta, _c) do
    if (meta[:runs] || 0) + (meta[:activity_tasks] || 0) > 0 do
      Out.continuum(
        "boot recovery released #{meta[:runs]} run(s) and #{meta[:activity_tasks]} task(s)"
      )
    end
  end

  def handle(_event, _measure, _meta, _config), do: :ok

  defp step(meta) do
    meta |> Map.get(:metadata, []) |> Keyword.get(:step)
  end

  defp short(nil), do: "?"
  defp short(id), do: id |> to_string() |> binary_part(0, 8)

  defp mfa({mod, fun, _args}), do: "#{inspect(mod)}.#{fun}"
  defp mfa(other), do: inspect(other)
end

defmodule ContinuumDemo.Evidence do
  @moduledoc false

  alias ContinuumDemo.{Ledger, Out}

  @doc "Prints the durable journal for a run: the thing replay reads."
  def journal(run_id) do
    Out.rule("continuum_events for run #{String.slice(run_id, 0, 8)}")

    case Continuum.Observer.list_events(run_id, limit: 100) do
      {:ok, page} ->
        Enum.each(page.entries, fn event ->
          IO.puts([
            IO.ANSI.faint(),
            String.pad_leading(to_string(event.seq), 4),
            "  ",
            event.inserted_at |> DateTime.to_time() |> Time.truncate(:millisecond) |> to_string(),
            "  ",
            IO.ANSI.reset(),
            String.pad_trailing(to_string(event.event_type), 22),
            IO.ANSI.faint(),
            payload_summary(event),
            IO.ANSI.reset()
          ])
        end)

      {:error, reason} ->
        Out.demo("could not read events: #{inspect(reason)}")
    end
  end

  @doc "Prints the external side-effect log the activities append to."
  def ledger do
    Out.rule("tmp/continuum_demo/ledger.log — the external world")

    case Ledger.lines() do
      [] -> IO.puts(IO.ANSI.faint() <> "  (empty)" <> IO.ANSI.reset())
      lines -> Enum.each(lines, &IO.puts(["  ", &1]))
    end
  end

  @doc "Prints the run row as the operator sees it."
  def run(run_id) do
    case Continuum.get_run(run_id) do
      {:ok, run} ->
        Out.continuum(
          "run #{String.slice(run_id, 0, 8)} state=#{run.state} workflow=#{inspect(run.workflow)}"
        )

      other ->
        Out.demo("get_run: #{inspect(other)}")
    end
  end

  defp payload_summary(event) do
    event
    |> Map.get(:payload)
    |> inspect(limit: 4, printable_limit: 90)
    |> String.slice(0, 90)
  end
end
