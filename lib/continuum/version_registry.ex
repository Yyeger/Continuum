defmodule Continuum.VersionRegistry do
  @moduledoc """
  In-memory registry mapping `(workflow_module, version_hash) -> module`.

  The plan calls for content-addressed module names like
  `MyApp.OrderFlow.V_<hash>` so old versions of a workflow can coexist with
  new ones. v0.1 stores the latest hash per module; the full versioned
  module-name mechanism lands with versioning in v0.3.
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Record the latest version hash for a workflow module."
  def register(module, version, hash) do
    GenServer.call(__MODULE__, {:register, module, version, hash})
  end

  @doc "Look up the stored metadata for a module (latest registered)."
  def lookup(module) do
    GenServer.call(__MODULE__, {:lookup, module})
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:register, module, version, hash}, _from, state) do
    {:reply, :ok, Map.put(state, module, %{version: version, hash: hash})}
  end

  def handle_call({:lookup, module}, _from, state) do
    {:reply, Map.get(state, module), state}
  end
end
