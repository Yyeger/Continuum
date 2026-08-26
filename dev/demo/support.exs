## Shared support for `mix continuum.demo`.
##
## Loaded by `dev/demo/demo.exs` before the :continuum application starts, so
## the workflow module is already loaded when the runtime's version registry
## and dispatcher come up.
##
## Nothing in this directory ships in the Hex package (see `package/0` in
## mix.exs) — it exists so the README's crash-recovery claim is reproducible in
## three commands.

defmodule ContinuumDemo do
  @moduledoc false

  # The whole demo hangs off one number: how long a lease survives the node
  # that owns it. Production default is 30s; the demo tightens it so a
  # screen recording does not spend 30 seconds watching a countdown. Nothing
  # else about the recovery path changes.
  def lease_ttl_seconds, do: 8

  def order_id, do: System.get_env("CONTINUUM_DEMO_ORDER") || "123"
  def total_cents, do: 4_200

  def db_config do
    [
      username: System.get_env("CONTINUUM_DEMO_PGUSER", "continuum"),
      password: System.get_env("CONTINUUM_DEMO_PGPASSWORD", "continuum"),
      hostname: System.get_env("CONTINUUM_DEMO_PGHOST", "localhost"),
      port: String.to_integer(System.get_env("CONTINUUM_DEMO_PGPORT", "5433")),
      database: System.get_env("CONTINUUM_DEMO_PGDATABASE", "continuum_demo"),
      pool_size: 10,
      log: false,
      priv: "priv/test_repo"
    ]
  end
end

defmodule ContinuumDemo.Repo do
  @moduledoc false
  use Ecto.Repo, otp_app: :continuum, adapter: Ecto.Adapters.Postgres
end

defmodule ContinuumDemo.Ledger do
  @moduledoc false

  # The demo's external world. Activities append here; workflow code never
  # touches it. This file is the only evidence that matters: if replay
  # re-executed the charge, there would be two CHARGE lines.

  @path "tmp/continuum_demo/ledger.log"

  def path, do: @path

  def append(kind, fields) do
    File.mkdir_p!(Path.dirname(@path))

    line =
      [
        DateTime.utc_now() |> DateTime.truncate(:millisecond) |> DateTime.to_iso8601(),
        String.pad_trailing(kind, 6),
        Enum.map_join(fields, " ", fn {k, v} -> "#{k}=#{v}" end)
      ]
      |> Enum.join("  ")

    File.write!(@path, line <> "\n", [:append])
    :ok
  end

  def lines do
    case File.read(@path) do
      {:ok, contents} -> contents |> String.split("\n", trim: true)
      {:error, _} -> []
    end
  end

  def count(kind) do
    Enum.count(lines(), &String.contains?(&1, kind))
  end

  def reset, do: File.rm_rf!(Path.dirname(@path))
end

defmodule ContinuumDemo.ChargeCard do
  @moduledoc false
  use Continuum.Activity,
    retry: [max_attempts: 3, backoff: :exponential, base_ms: 200],
    timeout: {:seconds, 10}

  # Deliberately NO idempotency_key/1. Continuum can dedupe activities through
  # `continuum_activity_results`, but that would muddy the demo: the point is
  # that *replay* never re-runs a completed activity, not that a side table
  # caught a second attempt.
  @impl true
  def run(%{"order_id" => order_id, "total_cents" => cents}) do
    payment_id = "pay_" <> (:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower))
    ContinuumDemo.Ledger.append("CHARGE", order: order_id, payment: payment_id, cents: cents)
    {:ok, %{payment_id: payment_id, total_cents: cents}}
  end
end

defmodule ContinuumDemo.ShipOrder do
  @moduledoc false
  use Continuum.Activity,
    retry: [max_attempts: 3, backoff: :exponential, base_ms: 200],
    timeout: {:seconds, 10}

  @impl true
  def run(%{"order_id" => order_id}) do
    shipment_id = "ship_" <> (:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower))
    ContinuumDemo.Ledger.append("SHIP", order: order_id, shipment: shipment_id)
    {:ok, %{shipment_id: shipment_id}}
  end
end

defmodule ContinuumDemo.Checkout do
  @moduledoc false
  use Continuum.Workflow, version: 1

  alias ContinuumDemo.{ChargeCard, ShipOrder}

  # Ordinary straight-line Elixir. No callbacks, no state machine, no explicit
  # persistence. `Continuum.log/3` journals its message, which is why the
  # "card charged" line prints exactly once across both phases even though
  # this function body runs twice.
  def run(%{"order_id" => order_id, "total_cents" => total_cents}) do
    Continuum.log(:info, "checkout started", order_id: order_id, step: "start")

    {:ok, charge} =
      activity(ChargeCard.run(%{"order_id" => order_id, "total_cents" => total_cents}))

    Continuum.log(:info, "card charged #{charge.payment_id}",
      order_id: order_id,
      step: "charged"
    )

    {:ok, shipment} = activity(ShipOrder.run(%{"order_id" => order_id}))

    Continuum.log(:info, "order shipped #{shipment.shipment_id}",
      order_id: order_id,
      step: "shipped"
    )

    {:ok, %{order_id: order_id, payment_id: charge.payment_id, shipment_id: shipment.shipment_id}}
  end
end
