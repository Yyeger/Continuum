defmodule Continuum.Runtime.SignalRouter do
  @moduledoc """
  Routes external signals to the local workflow process.

  v0.1 single-node implementation: looks the run up in the local Registry
  and delivers via `Continuum.Runtime.Engine.deliver_signal/3`.

  Postgres LISTEN-based cross-node delivery is added in the durable v0.1
  follow-up; the public API does not change.
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Deliver a signal to a run."
  @spec deliver(binary(), atom(), term()) :: :ok | {:error, term()}
  def deliver(run_id, name, payload) do
    case Registry.lookup(Continuum.Runtime.Registry, run_id) do
      [{_pid, _value}] ->
        Continuum.Runtime.Engine.deliver_signal(run_id, name, payload)
        :ok

      [] ->
        {:error, :not_found}
    end
  end

  @impl true
  def init(_opts), do: {:ok, %{}}
end
