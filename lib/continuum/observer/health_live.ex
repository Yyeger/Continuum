if Code.ensure_loaded?(Phoenix.LiveView) do
  defmodule Continuum.Observer.HealthLive do
    @moduledoc false

    use Phoenix.LiveView

    @impl true
    def mount(_params, session, socket) do
      socket =
        socket
        |> assign(:instance, Map.get(session, "instance", Continuum))
        |> assign(:observer_path, Map.get(session, "observer_path", "/continuum"))
        |> assign(:report, nil)
        |> assign(:pending_repair, nil)
        |> load_report()

      {:ok, socket}
    end

    @impl true
    def handle_event("refresh", _params, socket), do: {:noreply, load_report(socket)}

    def handle_event("repair", params, socket) do
      repair = repair_params(params)

      case run_repair(socket, repair, false) do
        {:ok, result} ->
          {:noreply,
           socket
           |> assign(:pending_repair, repair)
           |> put_flash(:info, "Dry run passed for #{result.action} #{result.subject_id}.")}

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Repair preview failed: #{inspect(reason)}")}
      end
    end

    def handle_event("confirm_repair", _params, %{assigns: %{pending_repair: nil}} = socket) do
      {:noreply, socket}
    end

    def handle_event("confirm_repair", _params, socket) do
      repair = socket.assigns.pending_repair

      case run_repair(socket, repair, true) do
        {:ok, result} ->
          {:noreply,
           socket
           |> assign(:pending_repair, nil)
           |> put_flash(:info, "#{result.action} applied to #{result.subject_id}.")
           |> load_report()}

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Repair failed: #{inspect(reason)}")}
      end
    end

    def handle_event("cancel_repair", _params, socket) do
      {:noreply, assign(socket, :pending_repair, nil)}
    end

    @impl true
    def render(assigns) do
      ~H"""
      <main class="co-shell">
        <header class="co-header">
          <div>
            <a href={@observer_path} class="co-back">Runs</a>
            <h1>Operational health</h1>
            <p>Durable state for <code><%= inspect(@instance) %></code></p>
          </div>
          <button id="co-health-refresh" phx-click="refresh">Refresh</button>
        </header>

        <%= if @report do %>
          <section class="co-health-summary">
            <article><span>Status</span><strong><%= @report.status %></strong></article>
            <article><span>Runtime readiness</span><strong><%= @report.runtime.state %></strong></article>
            <article><span>Missing partitions</span><strong><%= length(@report.partitions.missing) %></strong></article>
            <article><span>Active runs</span><strong><%= @report.runs.active_count %></strong></article>
            <article><span>Lost wakes</span><strong><%= @report.runs.lost_wake_count %></strong></article>
            <article><span>Overdue timers</span><strong><%= @report.timers.overdue_count %></strong></article>
            <article><span>Dead letters</span><strong><%= @report.activities.dead_letter_count %></strong></article>
            <article><span>Pending signals</span><strong><%= @report.signals.pending_count %></strong></article>
          </section>

          <%= if @pending_repair do %>
            <section id="co-repair-confirm" class="co-repair-confirm">
              <p>
                Dry run succeeded. Execute
                <strong><%= @pending_repair.action %></strong> for
                <code><%= @pending_repair.target %></code>?
              </p>
              <button phx-click="confirm_repair">Execute fenced repair</button>
              <button phx-click="cancel_repair" class="co-secondary">Cancel</button>
            </section>
          <% end %>

          <section>
            <h2>Partitions and workflow versions</h2>
            <div class="co-grid">
              <div><span>Partition horizon</span><code><%= @report.partitions.horizon_end %></code></div>
              <div><span>Missing partitions</span><code><%= Enum.join(@report.partitions.missing, ", ") |> blank_dash() %></code></div>
              <div><span>Default partition rows</span><%= @report.partitions.default_partition.row_count %></div>
              <div><span>Loaded versions</span><%= @report.workflow_versions.loaded_count %></div>
              <div><span>Durable versions</span><%= @report.workflow_versions.durable_count %></div>
            </div>
            <%= for version <- @report.workflow_versions.missing_in_database do %>
              <div class="co-health-finding">
                <code><%= version.workflow %> <%= version.version_hash %></code>
                <button phx-click="repair" phx-value-action="retry" phx-value-target={version.workflow}>Preview retry</button>
              </div>
            <% end %>
          </section>

          <section>
            <h2>Active and suspended runs</h2>
            <table class="co-table">
              <thead><tr><th>State</th><th>Reason</th><th>Count</th><th>Oldest age</th></tr></thead>
              <tbody>
                <%= for group <- @report.runs.groups do %>
                  <tr><td><%= group.state %></td><td><%= group.reason %></td><td><%= group.count %></td><td><%= duration(group.oldest_age_ms) %></td></tr>
                <% end %>
              </tbody>
            </table>
          </section>

          <section>
            <h2>Lost-wake candidates and overdue timers</h2>
            <%= if @report.runs.lost_wake_candidates == [] and @report.timers.overdue == [] do %>
              <p class="co-empty">No delayed durable wake evidence.</p>
            <% end %>
            <%= for wake <- @report.runs.lost_wake_candidates do %>
              <.finding finding={wake} label={"Run #{wake.run_id} wake delayed #{duration(wake.lag_ms)}"}>
                <button phx-click="repair" phx-value-action="wake" phx-value-target={wake.run_id} phx-value-lease-token={wake.lease_token}>Preview wake</button>
              </.finding>
            <% end %>
            <%= for timer <- @report.timers.overdue do %>
              <.finding finding={timer} label={"Timer #{timer.timer_id} overdue #{duration(timer.overdue_ms)}"}>
                <%= if timer.lease_token do %>
                  <button phx-click="repair" phx-value-action="wake" phx-value-target={timer.run_id} phx-value-lease-token={timer.lease_token}>Preview wake</button>
                <% end %>
              </.finding>
            <% end %>
          </section>

          <section>
            <h2>Run leases</h2>
            <table class="co-table">
              <thead><tr><th>Run</th><th>Owner</th><th>Epoch</th><th>Age</th><th>Heartbeat lag</th><th>Expires</th><th>Action</th></tr></thead>
              <tbody>
                <%= for lease <- @report.leases.entries do %>
                  <tr>
                    <td><code><%= lease.run_id %></code></td><td><code><%= lease.owner %></code></td><td><%= lease.epoch %></td>
                    <td><%= duration(lease.age_ms) %></td><td><%= duration(lease.heartbeat_lag_ms) %></td><td><%= inspect(lease.expires_at) %></td>
                    <td>
                      <%= if lease.expired do %>
                        <button phx-click="repair" phx-value-action="release_expired_lease" phx-value-target={lease.run_id} phx-value-lease-owner={lease.owner} phx-value-lease-token={lease.epoch}>Preview release</button>
                      <% else %>-<% end %>
                    </td>
                  </tr>
                <% end %>
              </tbody>
            </table>
          </section>

          <section>
            <h2>Activities and signals</h2>
            <div class="co-grid">
              <div><span>Pending activities</span><%= @report.activities.pending_count %></div>
              <div><span>Leased activities</span><%= @report.activities.leased_count %></div>
              <div><span>Oldest task</span><%= duration(@report.activities.oldest_task_age_ms) %></div>
              <div><span>Retry distribution</span><code><%= inspect(@report.activities.retry_distribution) %></code></div>
              <div><span>Signal backlog</span><%= @report.signals.pending_count %></div>
              <div><span>Catch-up lag</span><%= duration(@report.signals.catch_up_lag_ms) %></div>
            </div>
            <%= for task <- @report.activities.expired_leases do %>
              <.finding finding={task} label={"Task #{task.task_id} lease expired #{duration(task.overdue_ms)} ago"}>
                <button phx-click="repair" phx-value-action="requeue_activity" phx-value-target={task.task_id} phx-value-lease-owner={task.lease_owner} phx-value-attempt={task.attempt}>Preview requeue</button>
              </.finding>
            <% end %>
            <%= for task <- @report.activities.dead_letter_candidates do %>
              <.finding finding={task} label={"Dead-letter candidate #{task.task_id}: #{inspect(task.error)}"} />
            <% end %>
          </section>
        <% else %>
          <p class="co-empty">Health report unavailable.</p>
        <% end %>
      </main>
      """
    end

    attr(:finding, :map, required: true)
    attr(:label, :string, required: true)
    slot(:inner_block)

    defp finding(assigns) do
      ~H"""
      <div class={"co-health-finding #{if @finding.reviewed, do: "co-reviewed", else: ""}"}>
        <span><%= @label %><%= if @finding.reviewed, do: " (reviewed)" %></span>
        <div>
          <%= render_slot(@inner_block) %>
          <button :if={!@finding.reviewed} phx-click="repair" phx-value-action="mark_reviewed" phx-value-target={@finding.subject_id} phx-value-finding-type={@finding.finding_type} phx-value-fingerprint={@finding.fingerprint}>Preview review</button>
        </div>
      </div>
      """
    end

    defp load_report(socket) do
      case Continuum.Observer.health(instance: socket.assigns.instance) do
        {:ok, report} ->
          assign(socket, :report, report)

        {:error, reason} ->
          socket
          |> assign(:report, nil)
          |> put_flash(:error, "Health query failed: #{inspect(reason)}")
      end
    end

    defp run_repair(socket, repair, execute?) do
      opts =
        repair.opts ++
          [instance: socket.assigns.instance, execute: execute?, reviewed_by: "observer"]

      Continuum.Observer.repair_health(repair.action, repair.target, opts)
    end

    defp repair_params(params) do
      opts =
        []
        |> maybe_put_integer(:lease_token, params["lease-token"])
        |> maybe_put_integer(:attempt, params["attempt"])
        |> maybe_put(:lease_owner, params["lease-owner"])
        |> maybe_put(:finding_type, params["finding-type"])
        |> maybe_put(:fingerprint, params["fingerprint"])

      %{action: params["action"], target: params["target"], opts: opts}
    end

    defp maybe_put(opts, _key, nil), do: opts
    defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

    defp maybe_put_integer(opts, _key, nil), do: opts

    defp maybe_put_integer(opts, key, value) do
      case Integer.parse(value) do
        {integer, ""} -> Keyword.put(opts, key, integer)
        _ -> opts
      end
    end

    defp blank_dash(""), do: "-"
    defp blank_dash(value), do: value
    defp duration(nil), do: "-"
    defp duration(ms) when ms < 1_000, do: "#{ms}ms"
    defp duration(ms) when ms < 60_000, do: "#{div(ms, 1_000)}s"
    defp duration(ms), do: "#{div(ms, 60_000)}m"
  end
else
  defmodule Continuum.Observer.HealthLive do
    @moduledoc false

    def mount(_params, _session, _socket),
      do: raise("Continuum.Observer requires phoenix_live_view and phoenix_html")

    def handle_event(_event, _params, socket), do: {:noreply, socket}
  end
end
