// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// macOS shim: no-op Tracer to avoid linking opentelemetry
#include "common/tracer.h"

// Include the real OpenTelemetry headers to get the interface definitions.
#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/common/key_value_iterable.h>
#include <opentelemetry/common/timestamp.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_context.h>
#include <opentelemetry/trace/span_metadata.h>

#include <memory>

namespace starrocks {

// A no-op implementation of the opentelemetry::trace::Span interface.
// This class implements all pure virtual methods with empty bodies,
// so we can create instances of it without linking the opentelemetry library.
class NoOpSpan final : public opentelemetry::trace::Span {
public:
    void SetAttribute(opentelemetry::nostd::string_view /*key*/,
                      const opentelemetry::common::AttributeValue& /*value*/) noexcept override {}

    void AddEvent(opentelemetry::nostd::string_view /*name*/) noexcept override {}

    void AddEvent(opentelemetry::nostd::string_view /*name*/,
                  opentelemetry::common::SystemTimestamp /*timestamp*/) noexcept override {}

    void AddEvent(opentelemetry::nostd::string_view /*name*/,
                  const opentelemetry::common::KeyValueIterable& /*attributes*/) noexcept override {}

    void AddEvent(opentelemetry::nostd::string_view /*name*/, opentelemetry::common::SystemTimestamp /*timestamp*/,
                  const opentelemetry::common::KeyValueIterable& /*attributes*/) noexcept override {}

    void SetStatus(opentelemetry::trace::StatusCode /*code*/,
                   opentelemetry::nostd::string_view /*description*/) noexcept override {}

    void UpdateName(opentelemetry::nostd::string_view /*name*/) noexcept override {}

    void End(const opentelemetry::trace::EndSpanOptions& /*options*/ = {}) noexcept override {}

    bool IsRecording() const noexcept override { return false; }

    opentelemetry::trace::SpanContext GetContext() const noexcept override {
        return opentelemetry::trace::SpanContext::GetInvalid();
    }
};

// Helper to create a shared_ptr to our NoOpSpan.
// This ensures that we always return a valid, non-null pointer.
static Span make_noop_span() {
    std::shared_ptr<opentelemetry::trace::Span> noop_span_ptr = std::make_shared<NoOpSpan>();
    return Span(noop_span_ptr);
}

// --- Implementation of starrocks::Tracer ---

Tracer::~Tracer() = default;

Tracer& Tracer::Instance() {
    static Tracer t;
    return t;
}

void Tracer::release_instance() {}

void Tracer::init(const std::string&) {}

void Tracer::shutdown() {}

bool Tracer::is_enabled() const { return false; }

Span Tracer::start_trace(const std::string&) { return make_noop_span(); }

Span Tracer::start_trace_txn(const std::string&, int64_t) { return make_noop_span(); }

Span Tracer::start_trace_tablet(const std::string&, int64_t) { return make_noop_span(); }

Span Tracer::start_trace_txn_tablet(const std::string&, int64_t, int64_t) {
    return make_noop_span();
}

Span Tracer::add_span(const std::string&, const Span&) { return make_noop_span(); }

Span Tracer::add_span(const std::string&, const SpanContext&) { return make_noop_span(); }

Span Tracer::start_trace_or_add_span(const std::string&, const std::string&) {
    return make_noop_span();
}

SpanContext Tracer::from_trace_parent(const std::string&) {
    return SpanContext::GetInvalid();
}

std::string Tracer::to_trace_parent(const SpanContext&) { return {}; }

void shutdown_tracer() {}

} // namespace starrocks
