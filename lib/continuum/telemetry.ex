defmodule Continuum.Telemetry do
  @moduledoc """
  Telemetry event names emitted by Continuum.

  Continuum uses the `:telemetry` package directly. Event names are stable
  lists under the `[:continuum, ...]` prefix and metadata always includes the
  most specific identifiers available for the transition, such as `:run_id`,
  `:task_id`, `:timer_id`, `:signal_name`, `:owner`, or `:lease_token`.

  ## Runtime events

    * `[:continuum, :run, :started]`
    * `[:continuum, :run, :suspended]`
    * `[:continuum, :run, :completed]`
    * `[:continuum, :run, :failed]`
    * `[:continuum, :run, :cancelled]`
    * `[:continuum, :run, :lease_lost]`
    * `[:continuum, :activity, :scheduled]`
    * `[:continuum, :activity, :started]`
    * `[:continuum, :activity, :completed]`
    * `[:continuum, :activity, :failed]`
    * `[:continuum, :activity, :idempotency_hit]`
    * `[:continuum, :activity, :retried]`
    * `[:continuum, :timer, :scheduled]`
    * `[:continuum, :timer, :fired]`
    * `[:continuum, :signal, :awaited]`
    * `[:continuum, :signal, :delivered]`
    * `[:continuum, :signal, :received]`
    * `[:continuum, :lease, :acquired]`
    * `[:continuum, :lease, :renewed]`
    * `[:continuum, :lease, :lost]`
    * `[:continuum, :dispatcher, :polled]`
    * `[:continuum, :dispatcher, :claimed]`
    * `[:continuum, :activity_dispatcher, :polled]`
    * `[:continuum, :activity_dispatcher, :claimed]`
    * `[:continuum, :recovery, :completed]`

  Measurements are intentionally small and conventional: most events emit
  `%{count: n}` or `%{duration_ms: n}` when there is a meaningful number,
  otherwise `%{}`.

  When a Postgres-backed `await signal(...)` consumes a signal that was already
  pending in the durable mailbox, Continuum emits `[:continuum, :signal,
  :received]` without a preceding `[:continuum, :signal, :awaited]`; there was
  no suspended await period to bracket.

  ## OpenTelemetry bridge

  `Continuum.OpenTelemetry.setup/1` can attach an optional bridge that turns
  the run and activity lifecycle events above into OpenTelemetry spans. The
  bridge is opt-in and uses runtime checks, so Continuum compiles and runs
  without OpenTelemetry packages installed.
  """

  @events [
    [:continuum, :run, :started],
    [:continuum, :run, :suspended],
    [:continuum, :run, :completed],
    [:continuum, :run, :failed],
    [:continuum, :run, :cancelled],
    [:continuum, :run, :lease_lost],
    [:continuum, :activity, :scheduled],
    [:continuum, :activity, :started],
    [:continuum, :activity, :completed],
    [:continuum, :activity, :failed],
    [:continuum, :activity, :idempotency_hit],
    [:continuum, :activity, :retried],
    [:continuum, :timer, :scheduled],
    [:continuum, :timer, :fired],
    [:continuum, :signal, :awaited],
    [:continuum, :signal, :delivered],
    [:continuum, :signal, :received],
    [:continuum, :lease, :acquired],
    [:continuum, :lease, :renewed],
    [:continuum, :lease, :lost],
    [:continuum, :dispatcher, :polled],
    [:continuum, :dispatcher, :claimed],
    [:continuum, :activity_dispatcher, :polled],
    [:continuum, :activity_dispatcher, :claimed],
    [:continuum, :recovery, :completed]
  ]

  @doc """
  Returns all documented telemetry event names.
  """
  @spec events() :: [[atom()]]
  def events, do: @events

  @doc false
  @spec execute([atom()], map(), map()) :: :ok
  def execute(event, measurements \\ %{}, metadata \\ %{}) do
    :telemetry.execute(event, measurements, metadata)
  end
end
