// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#pragma once

#include "opentelemetry/sdk/trace/simple_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/trace/provider.h"

namespace starrocks {

using Span = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>;
using SpanContext = opentelemetry::trace::SpanContext;

// The tracer options.
struct TracerOptions {
    std::string jaeger_endpoint;
    int jaeger_server_port;
};

/*
 * Support trace and export to jaeger.
 * Use example:
 *    std::unique_ptr<Tracer> tracer = std::make_unique<Tracer>("BasicTest");
 *    auto f1_span = tracer->start_trace("span1");
 *    auto f2_span = tracer->add_span("span2", f1_span);
 *    f2_span->End();
 *
 */
class Tracer {
public:
    Tracer() = default;
    Tracer(const std::string& service_name, const TracerOptions& tracer_opts = {"localhost", 6381});

    // Init the tracer.
    void init(const std::string& service_name);

    // Shutdown the tracer.
    void shutdown();

    // Creates and returns a new span with `trace_name`
    // this span represents a trace, since it has no parent.
    Span start_trace(const std::string& trace_name);

    // Creates and returns a new span with `span_name` which parent span is `parent_span'.
    Span add_span(const std::string& span_name, const Span& parent_span);

    // Creates and return a new span with `span_name`
    // the span is added to the trace which it's context is `parent_ctx`.
    // parent_ctx contains the required information of the trace.
    Span add_span(const std::string& span_name, const SpanContext& parent_ctx);

private:
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> _tracer;
    TracerOptions _tracer_options;
};

} // namespace starrocks
