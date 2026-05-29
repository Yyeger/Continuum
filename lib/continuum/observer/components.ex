if Code.ensure_loaded?(Phoenix.Component) do
  defmodule Continuum.Observer.Components do
    @moduledoc false

    use Phoenix.Component

    attr(:state, :atom, required: true)

    def state_badge(assigns) do
      ~H"""
      <span class={"co-badge co-badge-#{@state}"}><%= @state %></span>
      """
    end

    attr(:value, :any, default: nil)

    def timestamp(assigns) do
      ~H"""
      <span class="co-timestamp"><%= format_time(@value) %></span>
      """
    end

    attr(:payload, :any, required: true)

    def payload(assigns) do
      ~H"""
      <pre class="co-payload"><%= Continuum.Observer.pretty(@payload) %></pre>
      """
    end

    @doc "CSS class grouping an event type for timeline colouring."
    def event_class(type), do: "co-event co-event-#{Continuum.Observer.Components.event_kind(type)}"

    @doc false
    def event_kind(type) do
      case to_string(type) do
        "compensation_" <> _ -> "compensation"
        "child_" <> _ -> "child"
        "run_continued_as_new" -> "continued"
        "patched" -> "patched"
        "activity_" <> _ -> "activity"
        "signal_" <> _ -> "signal"
        "timer_" <> _ -> "timer"
        _ -> "default"
      end
    end

    defp format_time(nil), do: "-"
    defp format_time(%DateTime{} = value), do: DateTime.to_iso8601(value)
    defp format_time(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
    defp format_time(value), do: to_string(value)
  end
else
  defmodule Continuum.Observer.Components do
    @moduledoc false

    def state_badge(assigns), do: assigns
    def timestamp(assigns), do: assigns
    def payload(assigns), do: assigns
    def event_class(_type), do: ""
    def event_kind(_type), do: "default"
  end
end
