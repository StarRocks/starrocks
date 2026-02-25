// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_context.h>

#include "common/tracer_fwd.h"

namespace starrocks {

/**
 * Handles span creation and provides a compatible interface to `opentelemetry::trace::Tracer`.
 *
 * Spans are organized in a hierarchy. Once a new span is created, through calling `start_trace()`,
 * it will be added as a child to the active span, and replaces its parent as the new active span.
 * When there is no active span, the newly created span is considered as the root span.
 *
 * Here is an example on how to create spans and retrieve traces:
 * ```
 * const Tracer& tracer = Tracer::Instance();
 *
 * void f1(const Tracer& tracer) {
 *     auto root = tracer.start_trace("root");
 *     sleepFor(Milliseconds(1));
 *     {
 *         auto child = tracer.add_span("child", root);
 *         sleepFor(Milliseconds(2));
 *     }
 * }
 * ```
 *
 */
class Tracer {
public:
    ~Tracer();

    // Get the global tracer instance.
    static Tracer& Instance();

    static void release_instance();

    // Return true if trace is enabled.
    bool is_enabled() const;

    // Creates and returns a new span with `trace_name`
    // this span represents a trace, since it has no parent.
    Span start_trace(const std::string& trace_name);

    Span start_trace_txn(const std::string& trace_name, int64_t txn_id);

    Span start_trace_tablet(const std::string& trace_name, int64_t tablet_id);

    Span start_trace_txn_tablet(const std::string& trace_name, int64_t txn_id, int64_t tablet_id);

    // Creates and returns a new span with `span_name` which parent span is `parent_span'.
    Span add_span(const std::string& span_name, const Span& parent_span);

    // Creates and return a new span with `span_name`
    // the span is added to the trace which it's context is `parent_ctx`.
    // parent_ctx contains the required information of the trace.
    Span add_span(const std::string& span_name, const SpanContext& parent_ctx);

    // If trace_parent is empty, create a new trace, else add a span
    Span start_trace_or_add_span(const std::string& name, const std::string& trace_parent);

    // Construct a SpanContext from Traceparent header
    static SpanContext from_trace_parent(const std::string& trace_parent);

    // Construct a Traceparent header from SpanContext
    static std::string to_trace_parent(const SpanContext& context);

private:
    // Init the tracer.
    void init(const std::string& service_name);
    // Shutdown the tracer.
    void shutdown();

    // The global tracer.
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> _tracer;
};

void shutdown_tracer();

} // namespace starrocks
