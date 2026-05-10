if Code.ensure_loaded?(Phoenix.LiveView) do
  defmodule Continuum.Observer.RunLive do
    @moduledoc false

    use Phoenix.LiveView

    import Continuum.Observer.Components

    @impl true
    def mount(%{"id" => run_id}, session, socket) do
      instance = Map.get(session, "instance", Continuum)
      observer_path = Map.get(session, "observer_path", "/continuum")

      if connected?(socket) do
        Continuum.Observer.subscribe_run(run_id, instance: instance)
        Continuum.Observer.subscribe_runs(instance: instance)
      end

      socket =
        socket
        |> assign(:instance, instance)
        |> assign(:observer_path, observer_path)
        |> assign(:run_id, run_id)
        |> assign(:signal_name, "")
        |> assign(:signal_payload, "{}")
        |> load_run()

      {:ok, socket}
    end

    @impl true
    def handle_info({:run_finished, _run_id, _state, _payload}, socket) do
      {:noreply, load_run(socket)}
    end

    def handle_info({:run_state_changed, run_id, _state}, %{assigns: %{run_id: run_id}} = socket) do
      {:noreply, load_run(socket)}
    end

    def handle_info({:run_state_changed, _run_id, _state}, socket), do: {:noreply, socket}

    @impl true
    def handle_event("cancel", _params, socket) do
      case Continuum.Observer.cancel_run(socket.assigns.run_id, instance: socket.assigns.instance) do
        :ok ->
          {:noreply, load_run(socket)}

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Cancel failed: #{inspect(reason)}")}
      end
    end

    def handle_event("signal", %{"signal" => signal}, socket) do
      name = Map.get(signal, "name", "")
      payload = Map.get(signal, "payload", "")

      with {:ok, decoded} <- Continuum.Observer.decode_signal_payload(payload),
           :ok <-
             Continuum.Observer.send_signal(socket.assigns.run_id, name, decoded,
               instance: socket.assigns.instance
             ) do
        {:noreply,
         socket
         |> assign(:signal_name, "")
         |> assign(:signal_payload, "{}")
         |> load_run()}
      else
        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Signal failed: #{inspect(reason)}")}
      end
    end

    @impl true
    def render(assigns) do
      ~H"""
      <main class="co-shell">
        <header class="co-header">
          <div>
            <a href={@observer_path} class="co-back">Runs</a>
            <h1><code><%= @run_id %></code></h1>
            <%= if @run do %>
              <p><%= @run.workflow %></p>
            <% end %>
          </div>
          <%= if @run do %>
            <.state_badge state={@run.state} />
          <% end %>
        </header>

        <%= if @run do %>
          <section class="co-grid">
            <div><span>Started</span><.timestamp value={@run.started_at} /></div>
            <div><span>Completed</span><.timestamp value={@run.completed_at} /></div>
            <div><span>Lease owner</span><code><%= @run.lease_owner || "-" %></code></div>
            <div><span>Lease expires</span><.timestamp value={@run.lease_expires_at} /></div>
            <div><span>Retention</span><.timestamp value={@run.retention_until} /></div>
            <div><span>Next wakeup</span><.timestamp value={@run.next_wakeup_at} /></div>
          </section>

          <section class="co-actions">
            <button id="co-cancel-run" phx-click="cancel" disabled={@run.state in [:completed, :failed, :cancelled]}>Cancel</button>

            <form id="co-signal-form" phx-submit="signal" class="co-signal-form">
              <input name="signal[name]" value={@signal_name} placeholder="signal name" />
              <textarea name="signal[payload]" rows="3"><%= @signal_payload %></textarea>
              <button type="submit">Send Signal</button>
            </form>
          </section>

          <section>
            <h2>Event Timeline</h2>
            <ol class="co-timeline">
              <%= for event <- @events do %>
                <li>
                  <header>
                    <code>#<%= event.seq %></code>
                    <strong><%= event.type %></strong>
                    <.timestamp value={event.inserted_at} />
                  </header>
                  <.payload payload={event.payload} />
                </li>
              <% end %>
            </ol>
          </section>
        <% else %>
          <p class="co-empty">Run not found.</p>
        <% end %>
      </main>
      """
    end

    defp load_run(socket) do
      run_id = socket.assigns.run_id
      instance = socket.assigns.instance

      with {:ok, run} <- Continuum.Observer.get_run(run_id, instance: instance),
           {:ok, events} <- Continuum.Observer.list_events(run_id, instance: instance) do
        socket
        |> assign(:run, run)
        |> assign(:events, events)
      else
        {:error, :not_found} ->
          socket
          |> assign(:run, nil)
          |> assign(:events, [])

        {:error, reason} ->
          socket
          |> assign(:run, nil)
          |> assign(:events, [])
          |> put_flash(:error, "Observer query failed: #{inspect(reason)}")
      end
    end
  end
else
  defmodule Continuum.Observer.RunLive do
    @moduledoc false

    def mount(_params, _session, _socket), do: raise_missing_live_view!()
    def handle_event(_event, _params, socket), do: {:noreply, socket}
    def handle_info(_message, socket), do: {:noreply, socket}

    defp raise_missing_live_view! do
      raise "Continuum.Observer requires phoenix_live_view and phoenix_html"
    end
  end
end
