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
        |> assign(:signal_contracts, nil)
        |> assign(:signal_payload, "{}")
        |> assign(:event_cursor, nil)
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

    def handle_event("load-events", _params, socket) do
      {:noreply, load_more_events(socket)}
    end

    @impl true
    def render(assigns) do
      ~H"""
      <main class="co-shell">
        <header class="co-header">
          <div>
            <a href={Continuum.Observer.Path.runs(@observer_path)} class="co-back">Runs</a>
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
            <div><span>Lease acquired</span><.timestamp value={@run.lease_acquired_at} /></div>
            <div><span>Lease heartbeat</span><.timestamp value={@run.lease_heartbeat_at} /></div>
            <div><span>Lease expires</span><.timestamp value={@run.lease_expires_at} /></div>
            <div><span>Retention</span><.timestamp value={@run.retention_until} /></div>
            <div><span>Next wakeup</span><.timestamp value={@run.next_wakeup_at} /></div>
          </section>

          <%= if @run.parent_run_id || @run.continued_from_run_id || @continues_to do %>
            <section class="co-lineage">
              <%= if @run.parent_run_id do %>
                <div>
                  <span>Parent</span>
                  <a href={Continuum.Observer.Path.run(@observer_path, @run.parent_run_id)}><code><%= @run.parent_run_id %></code></a>
                </div>
              <% end %>
              <%= if @run.continued_from_run_id do %>
                <div>
                  <span>Continued from</span>
                  <a href={Continuum.Observer.Path.run(@observer_path, @run.continued_from_run_id)}><code><%= @run.continued_from_run_id %></code></a>
                </div>
              <% end %>
              <%= if @continues_to do %>
                <div>
                  <span>Continues to</span>
                  <a href={Continuum.Observer.Path.run(@observer_path, @continues_to)}><code><%= @continues_to %></code></a>
                </div>
              <% end %>
            </section>
          <% end %>

          <section class="co-actions">
            <button id="co-cancel-run" phx-click="cancel" disabled={@run.state in [:completed, :failed, :cancelled]}>Cancel</button>

            <form id="co-signal-form" phx-submit="signal" class="co-signal-form">
              <select :if={is_map(@signal_contracts)} name="signal[name]">
                <option value="">Select signal</option>
                <option :for={{name, validator} <- @signal_contracts} value={name}>
                  <%= name %> (<%= inspect(validator) %>)
                </option>
              </select>
              <input :if={is_nil(@signal_contracts)} name="signal[name]" value={@signal_name} placeholder="signal name" />
              <textarea name="signal[payload]" rows="3"><%= @signal_payload %></textarea>
              <button type="submit">Send Signal</button>
            </form>
          </section>

          <section :if={@activities != []}>
            <h2>Activities</h2>
            <ol class="co-timeline co-activities">
              <%= for activity <- @activities do %>
                <li>
                  <header>
                    <code><%= activity.id %></code>
                    <strong><%= activity.state %></strong>
                    <span>attempt <%= activity.attempt %></span>
                  </header>
                  <div :if={activity.last_heartbeat_at}>
                    <span>Last heartbeat</span>
                    <.timestamp value={activity.last_heartbeat_at} />
                    <.payload payload={activity.heartbeat_details} />
                  </div>
                </li>
              <% end %>
            </ol>
          </section>

          <section>
            <h2>Event Timeline</h2>
            <ol class="co-timeline">
              <%= for event <- @events do %>
                <li class={event_class(event.type)}>
                  <header>
                    <code>#<%= event.seq %></code>
                    <strong><%= event.type %></strong>
                    <.timestamp value={event.inserted_at} />
                  </header>
                  <.payload payload={event.payload} />
                </li>
              <% end %>
            </ol>
            <button :if={@event_cursor} id="co-load-events" phx-click="load-events">Load more events</button>
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
           {:ok, event_page} <- Continuum.Observer.list_events(run_id, instance: instance),
           {:ok, activities} <-
             Continuum.Observer.list_activity_tasks(run_id, instance: instance),
           {:ok, signal_contracts} <-
             Continuum.Observer.signal_contracts(run_id, instance: instance) do
        socket
        |> assign(:run, run)
        |> assign(:events, event_page.entries)
        |> assign(:event_cursor, event_page.next_cursor)
        |> assign(:activities, activities)
        |> assign(:signal_contracts, signal_contracts)
        |> assign(:continues_to, Continuum.Observer.successor_run_id(run_id, instance: instance))
      else
        {:error, :not_found} ->
          socket
          |> assign(:run, nil)
          |> assign(:events, [])
          |> assign(:event_cursor, nil)
          |> assign(:activities, [])
          |> assign(:signal_contracts, nil)
          |> assign(:continues_to, nil)

        {:error, reason} ->
          socket
          |> assign(:run, nil)
          |> assign(:events, [])
          |> assign(:event_cursor, nil)
          |> assign(:activities, [])
          |> assign(:signal_contracts, nil)
          |> assign(:continues_to, nil)
          |> put_flash(:error, "Observer query failed: #{inspect(reason)}")
      end
    end

    defp load_more_events(%{assigns: %{event_cursor: nil}} = socket), do: socket

    defp load_more_events(socket) do
      opts = [
        instance: socket.assigns.instance,
        after_seq: socket.assigns.event_cursor
      ]

      case Continuum.Observer.list_events(socket.assigns.run_id, opts) do
        {:ok, event_page} ->
          socket
          |> assign(:events, socket.assigns.events ++ event_page.entries)
          |> assign(:event_cursor, event_page.next_cursor)

        {:error, reason} ->
          put_flash(socket, :error, "Observer event query failed: #{inspect(reason)}")
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
