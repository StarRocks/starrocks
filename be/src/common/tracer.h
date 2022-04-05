// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#pragma once

#include "opentelemetry/trace/provider.h"
#include "opentelemetry/sdk/trace/simple_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"

namespace starrocks {

using Span = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>;
using SpanContext = opentelemetry::trace::SpanContext;

class Tracer {
private:
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer;

public:
    Tracer() = default;
    Tracer(opentelemetry::nostd::string_view service_name);

    // Init the tracer.
    void init(opentelemetry::nostd::string_view service_name);
    // Shutdown the tracer.
    void shutdown();

    bool is_enabled() const;

    // Creates and returns a new span with `trace_name`
    // this span represents a trace, since it has no parent.
    Span start_trace(opentelemetry::nostd::string_view trace_name);

    // Creates and returns a new span with `span_name` which parent span is `parent_span'.
    Span add_span(opentelemetry::nostd::string_view span_name, const Span& parent_span);

    // Creates and return a new span with `span_name`
    // the span is added to the trace which it's context is `parent_ctx`.
    // parent_ctx contains the required information of the trace.
    Span add_span(opentelemetry::nostd::string_view span_name, const SpanContext& parent_ctx);
};

}

