defmodule OpenTelemetry.Span do
  @moduledoc """
  This module contains macros for Span operations that update the active current Span in the current process.
  An example of creating an Event and adding it to the current Span:

      require OpenTelemetry.Span
      ...
      event = "ecto.query"
      ecto_attributes = OpenTelemetry.event([{"query", query}, {"total_time", total_time}])
      OpenTelemetry.Span.add_event(event, ecto_attributes)
      ...

  A Span represents a single operation within a trace. Spans can be nested to form a trace tree.
  Each trace contains a root span, which typically describes the end-to-end latency and, optionally,
  one or more sub-spans for its sub-operations.

  Spans encapsulate:

  - The span name
  - An immutable SpanContext (`t:OpenTelemetry.span_ctx/0`) that uniquely identifies the Span
  - A parent Span in the form of a Span (`t:OpenTelemetry.span/0`), SpanContext (`t:OpenTelemetry.span_ctx/0`), or `undefined`
  - A start timestamp
  - An end timestamp
  - An ordered mapping of Attributes (`t:OpenTelemetry.attributes/0`)
  - A list of Links to other Spans (`t:OpenTelemetry.link/0`)
  - A list of timestamped Events (`t:OpenTelemetry.event/0`)
  - A Status (`t:OpenTelemetry.status/0`)
  """

  @doc """
  Get the SpanId of a Span.
  """
  @spec span_id(OpenTelemetry.span_ctx()) :: OpenTelemetry.span_id()
  defdelegate span_id(span), to: :otel_span

  @doc """
  Get the TraceId of a Span.
  """
  @spec trace_id(OpenTelemetry.span_ctx()) :: OpenTelemetry.trace_id()
  defdelegate trace_id(span), to: :otel_span

  @doc """
  Get the Tracestate of a Span.
  """
  @spec tracestate(OpenTelemetry.span_ctx()) :: OpenTelemetry.tracestate()
  defdelegate tracestate(span), to: :otel_span

  @doc """
  End the Span. Sets the end timestamp for the currently active Span. This has no effect on any
  child Spans that may exist of this Span.

  The Context is unchanged.
  """
  defdelegate end_span(span_ctx), to: :otel_span

  @doc """
  Set an attribute with key and value on the currently active Span.
  """
  @spec set_attribute(OpenTelemetry.span_ctx(), OpenTelemetry.attribute_key(), OpenTelemetry.attribute_value()) :: boolean()
  defdelegate set_attribute(span_ctx, key, value), to: :otel_span

  @doc """
  Add a list of attributes to the currently active Span.
  """
  @spec set_attributes(OpenTelemetry.span_ctx(), OpenTelemetry.attributes()) :: boolean()
  defdelegate set_attributes(span_ctx, attributes), to: :otel_span

  @doc """
  Add an event to the currently active Span.
  """
  @spec add_event(OpenTelemetry.span_ctx(), OpenTelemetry.event_name(), OpenTelemetry.attributes()) :: boolean()
  defdelegate add_event(span_ctx, event, attributes), to: :otel_span

  @doc """
  Add a list of events to the currently active Span.
  """
  @spec add_events(OpenTelemetry.span_ctx(), [OpenTelemetry.event()]) :: boolean()
  defdelegate add_events(span_ctx, events), to: :otel_span

  @doc """
  Sets the Status of the currently active Span.

  If used, this will override the default Span Status, which is `Ok`.
  """
  @spec set_status(OpenTelemetry.span_ctx(), OpenTelemetry.status()) :: boolean()
  defdelegate set_status(span_ctx, status), to: :otel_span

  @doc """
  Updates the Span name.

  It is highly discouraged to update the name of a Span after its creation. Span name is
  often used to group, filter and identify the logical groups of spans. And often, filtering
  logic will be implemented before the Span creation for performance reasons. Thus the name
  update may interfere with this logic.

  The function name is called UpdateName to differentiate this function from the regular
  property setter. It emphasizes that this operation signifies a major change for a Span
  and may lead to re-calculation of sampling or filtering decisions made previously
  depending on the implementation.
  """
  @spec update_name(OpenTelemetry.span_ctx(), String.t()) :: boolean()
  defdelegate update_name(span_ctx, name), to: :otel_span
end
