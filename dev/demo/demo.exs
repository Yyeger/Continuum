## `mix continuum.demo` — the README's crash-recovery claim, executable.
##
##   mix continuum.demo              # start a checkout, charge the card, kill -9 the VM
##   mix continuum.demo --resume     # restart, replay, ship without re-charging
##   mix continuum.demo --observer   # read-only Observer on http://localhost:4000/continuum
##   mix continuum.demo --reset      # empty the demo tables and the ledger
##
## Run with the app stopped (`mix run --no-start`, wired up by the mix alias).

here = Path.dirname(__ENV__.file)
Code.require_file("support.exs", here)
Code.require_file("harness.exs", here)

alias ContinuumDemo.{Boot, Evidence, Ledger, Narrator, Out}

defmodule ContinuumDemo.CLI do
  @moduledoc false

  alias ContinuumDemo.{Boot, Evidence, Ledger, Narrator, Out}

  # ── phase 1: crash ────────────────────────────────────────────────────────

  def crash do
    Out.rule("phase 1 — a checkout that dies halfway through")
    Ledger.reset()
    Boot.connect!()
    Boot.clean_slate!()
    Boot.start_runtime!()
    Narrator.attach(kill_on_step: "charged")

    order_id = ContinuumDemo.order_id()

    Out.demo("starting checkout for order ##{order_id} (#{ContinuumDemo.total_cents()} cents)")
    Out.blank()

    {:ok, run_id} =
      Continuum.start(
        ContinuumDemo.Checkout,
        %{"order_id" => order_id, "total_cents" => ContinuumDemo.total_cents()},
        lease_ttl_seconds: ContinuumDemo.lease_ttl_seconds()
      )

    File.mkdir_p!("tmp/continuum_demo")
    File.write!("tmp/continuum_demo/run_id", run_id)

    # The narrator halts this VM from inside the engine process. If it somehow
    # does not, fail loudly rather than print a success the demo did not earn.
    Process.sleep(20_000)
    Out.bang("the workflow never reached the charge checkpoint — demo is broken")
    System.halt(1)
  end

  # ── phase 2: resume ───────────────────────────────────────────────────────

  def resume do
    Out.rule("phase 2 — a brand new VM picks the run back up")

    log_counter = :counters.new(1, [:atomics])
    Narrator.attach(log_counter: log_counter)

    started_at = System.monotonic_time(:millisecond)

    # Repo and instance up, pollers still down: the journal below is exactly
    # what the dead node left behind, not a snapshot racing a resume.
    Boot.connect!()

    case find_orphan() do
      nil ->
        Out.bang("no crashed run found — run `mix continuum.demo` first")
        System.halt(1)

      %{state: state} = run when state in [:completed, :failed, :cancelled] ->
        Out.bang("run #{String.slice(run.id, 0, 8)} is already #{state} — nothing to resume")
        Out.demo("run `mix continuum.demo` to crash a fresh one first")
        System.halt(1)

      run ->
        Out.continuum(
          "found run #{String.slice(run.id, 0, 8)} in state=#{run.state}, " <>
            "leased by a node that no longer exists"
        )

        Evidence.journal(run.id)
        Evidence.ledger()

        Out.blank()

        Out.demo(
          "the charge is already journaled, so replay will not re-run it — " <>
            "the dead node's lease frees up #{ContinuumDemo.lease_ttl_seconds()}s after it died " <>
            "(this demo's TTL; the default is 30s)"
        )

        Out.blank()

        Boot.start_runtime!()

        case Continuum.await(run.id, 90_000) do
          {:ok, %{state: :completed, result: result}} ->
            elapsed = System.monotonic_time(:millisecond) - started_at
            Out.blank()
            Out.demo("workflow returned #{inspect(result)}")

            Out.demo(
              "this VM took #{Float.round(elapsed / 1000, 1)}s to finish someone else's work"
            )

            Evidence.journal(run.id)
            Evidence.ledger()
            verdict(log_counter)

          other ->
            Out.blank()
            Out.bang("run did not complete: #{inspect(other)}")
            Evidence.journal(run.id)
            Evidence.ledger()
            System.halt(1)
        end
    end
  end

  defp verdict(log_counter) do
    charges = Ledger.count("CHARGE")
    ships = Ledger.count("SHIP")
    logs = :counters.get(log_counter, 1)

    Out.rule("verdict")

    checks = [
      line("card charged exactly once", charges == 1, "#{charges} CHARGE line(s) in the ledger"),
      line("order shipped exactly once", ships == 1, "#{ships} SHIP line(s) in the ledger"),
      line(
        "replay stayed silent",
        logs == 1,
        "this VM printed #{logs} workflow log line, not the 3 in the journal"
      ),
      line(
        "the whole function body ran again, harmlessly",
        charges == 1 and ships == 1,
        "run/1 executed twice; its side effects executed once each"
      )
    ]

    Out.blank()

    if Enum.all?(checks) do
      IO.puts([
        IO.ANSI.green(),
        IO.ANSI.bright(),
        "The function survived the VM dying halfway through it.",
        IO.ANSI.reset()
      ])

      Out.blank()
    else
      Out.bang("at least one check failed — do not record this take")
      System.halt(1)
    end
  end

  # Prints the check and returns whether it passed, so the closing banner can
  # never contradict a red line above it.
  defp line(label, passed?, detail) do
    {mark, color} = if passed?, do: {"✓", IO.ANSI.green()}, else: {"✗", IO.ANSI.red()}

    IO.puts([
      color,
      "  ",
      mark,
      " ",
      IO.ANSI.reset(),
      label,
      IO.ANSI.faint(),
      " — ",
      detail,
      IO.ANSI.reset()
    ])

    passed?
  end

  defp find_orphan do
    with {:ok, run_id} <- File.read("tmp/continuum_demo/run_id"),
         {:ok, run} <- Continuum.get_run(String.trim(run_id)) do
      run
    else
      _ -> newest_run()
    end
  end

  defp newest_run do
    case Continuum.list_runs(workflow: "ContinuumDemo.Checkout", limit: 1) do
      {:ok, %{entries: [run | _]}} -> run
      _ -> nil
    end
  end

  # ── the Observer pane ─────────────────────────────────────────────────────

  def observer do
    Code.require_file("observer.exs", Path.dirname(__ENV__.file))
  end

  # ── housekeeping ──────────────────────────────────────────────────────────

  def reset do
    Ledger.reset()
    Out.demo("removed tmp/continuum_demo")
    Boot.connect!()
    Boot.clean_slate!()
  end

  def help do
    IO.puts("""

    #{IO.ANSI.bright()}mix continuum.demo#{IO.ANSI.reset()} — durable execution you can watch

      mix continuum.demo             start a checkout, charge the card, kill the BEAM
      mix continuum.demo --resume    restart: replay the journal, ship, never re-charge
      mix continuum.demo --observer  read-only Observer at http://localhost:4000/continuum
      mix continuum.demo --reset     empty the demo tables and the ledger file
      mix continuum.demo --help      this message

    Needs Postgres on localhost:5433 (`docker compose up -d` at the repo root).
    Override with CONTINUUM_DEMO_PGHOST / PGPORT / PGUSER / PGPASSWORD / PGDATABASE.
    """)
  end
end

case System.argv() do
  [] ->
    ContinuumDemo.CLI.crash()

  ["--resume"] ->
    ContinuumDemo.CLI.resume()

  ["--observer"] ->
    ContinuumDemo.CLI.observer()

  ["--reset"] ->
    ContinuumDemo.CLI.reset()

  ["--help"] ->
    ContinuumDemo.CLI.help()

  other ->
    Out.bang("unknown arguments: #{Enum.join(other, " ")}")
    ContinuumDemo.CLI.help()
    System.halt(1)
end
