if Code.ensure_loaded?(Phoenix.LiveView) do
  defmodule Continuum.Observer.RunsLive do
    @moduledoc false

    use Phoenix.LiveView

    import Continuum.Observer.Components

    @refresh_ms 75

    @impl true
    def mount(params, session, socket) do
      instance = Map.get(session, "instance", Continuum)
      observer_path = Map.get(session, "observer_path", "/continuum")

      if connected?(socket) do
        Continuum.Observer.subscribe_runs(instance: instance)
      end

      socket =
        socket
        |> assign(:instance, instance)
        |> assign(:observer_path, observer_path)
        |> assign(:refresh_ref, nil)
        |> assign(:runs, [])
        |> assign(:total, 0)
        |> assign(:total_pages, 1)
        |> assign_filters(params)

      {:ok, socket}
    end

    @impl true
    def handle_params(params, _uri, socket) do
      {:noreply, socket |> assign_filters(params) |> load_runs()}
    end

    @impl true
    def handle_event("filter", %{"filters" => filters}, socket) do
      {:noreply, socket |> assign_filters(filters) |> load_runs()}
    end

    def handle_event("page", %{"page" => page}, socket) do
      {:noreply, socket |> assign(:page, page) |> load_runs()}
    end

    @impl true
    def handle_info({:run_state_changed, _run_id, _state}, socket) do
      {:noreply, schedule_refresh(socket)}
    end

    def handle_info(:refresh_runs, socket) do
      {:noreply, socket |> assign(:refresh_ref, nil) |> load_runs()}
    end

    @impl true
    def render(assigns) do
      ~H"""
      <main class="co-shell">
        <header class="co-header">
          <div>
            <h1>Continuum</h1>
            <p>Workflow runs for <code><%= inspect(@instance) %></code></p>
          </div>
        </header>

        <form id="co-runs-filter" phx-change="filter" class="co-toolbar">
          <input type="search" name="filters[search]" value={@search} placeholder="Run id or workflow" />
          <select name="filters[state]">
            <option value="" selected={@state in [nil, ""]}>All states</option>
            <%= for state <- ~w(running suspended completed failed cancelled) do %>
              <option value={state} selected={@state == state}><%= state %></option>
            <% end %>
          </select>
          <input type="search" name="filters[workflow]" value={@workflow} placeholder="Workflow" />
        </form>

        <section class="co-table-wrap">
          <table class="co-table">
            <thead>
              <tr>
                <th>Run</th>
                <th>State</th>
                <th>Workflow</th>
                <th>Started</th>
                <th>Completed</th>
              </tr>
            </thead>
            <tbody>
              <%= if @runs == [] do %>
                <tr><td colspan="5" class="co-empty">No runs found.</td></tr>
              <% else %>
                <%= for run <- @runs do %>
                  <tr>
                    <td><a href={"#{@observer_path}/runs/#{run.run_id}"}><code><%= run.run_id %></code></a></td>
                    <td><.state_badge state={run.state} /></td>
                    <td><%= run.workflow %></td>
                    <td><.timestamp value={run.started_at} /></td>
                    <td><.timestamp value={run.completed_at} /></td>
                  </tr>
                <% end %>
              <% end %>
            </tbody>
          </table>
        </section>

        <footer class="co-pagination">
          <button phx-click="page" phx-value-page={max(@page - 1, 1)} disabled={@page <= 1}>Previous</button>
          <span>Page <%= @page %> / <%= @total_pages %> · <%= @total %> runs</span>
          <button phx-click="page" phx-value-page={min(@page + 1, @total_pages)} disabled={@page >= @total_pages}>Next</button>
        </footer>
      </main>
      """
    end

    defp assign_filters(socket, params) do
      socket
      |> assign(:search, Map.get(params, "search", ""))
      |> assign(:state, Map.get(params, "state", ""))
      |> assign(:workflow, Map.get(params, "workflow", ""))
      |> assign(:page, Map.get(params, "page", "1"))
    end

    defp load_runs(socket) do
      opts = [
        instance: socket.assigns.instance,
        search: socket.assigns.search,
        state: socket.assigns.state,
        workflow: socket.assigns.workflow,
        page: socket.assigns.page
      ]

      case Continuum.Observer.list_runs(opts) do
        {:ok, page} ->
          socket
          |> assign(:runs, page.entries)
          |> assign(:page, page.page)
          |> assign(:total, page.total)
          |> assign(:total_pages, page.total_pages)

        {:error, reason} ->
          socket
          |> assign(:runs, [])
          |> assign(:total, 0)
          |> assign(:total_pages, 1)
          |> put_flash(:error, "Observer query failed: #{inspect(reason)}")
      end
    end

    defp schedule_refresh(%{assigns: %{refresh_ref: nil}} = socket) do
      assign(socket, :refresh_ref, Process.send_after(self(), :refresh_runs, @refresh_ms))
    end

    defp schedule_refresh(socket), do: socket
  end
else
  defmodule Continuum.Observer.RunsLive do
    @moduledoc false

    def mount(_params, _session, _socket), do: raise_missing_live_view!()
    def handle_params(_params, _uri, socket), do: {:noreply, socket}
    def handle_event(_event, _params, socket), do: {:noreply, socket}
    def handle_info(_message, socket), do: {:noreply, socket}

    defp raise_missing_live_view! do
      raise "Continuum.Observer requires phoenix_live_view and phoenix_html"
    end
  end
end
