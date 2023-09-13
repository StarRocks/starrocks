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

#include "common/tracer.h"

#include <opentelemetry/exporters/jaeger/jaeger_exporter.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/propagation/detail/hex.h>
#include <opentelemetry/trace/provider.h>

#include "common/config.h"
#include "gutil/strings/split.h"

namespace starrocks {

Tracer::~Tracer() {
    shutdown();
}

Tracer& Tracer::Instance() {
    static Tracer global_tracer;
    static std::once_flag oc;
    std::call_once(oc, [&]() { global_tracer.init("starrocks-be"); });
    return global_tracer;
}

void Tracer::release_instance() {
    Instance().shutdown();
}

void Tracer::init(const std::string& service_name) {
    if (!config::jaeger_endpoint.empty()) {
        opentelemetry::exporter::jaeger::JaegerExporterOptions opts;
        vector<string> host_port = strings::Split(config::jaeger_endpoint, ":");
        if (host_port.size() != 2) {
            LOG(WARNING) << "bad jaeger_endpoint " << config::jaeger_endpoint;
            return;
        }
        opts.endpoint = host_port[0];
        long port = strtol(host_port[1].c_str(), nullptr, 10);
        if (port > 0 && port <= USHRT_MAX) {
            opts.server_port = (uint16_t)port;
        }
        auto jaeger_exporter = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(
                new opentelemetry::exporter::jaeger::JaegerExporter(opts));
        auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(
                new opentelemetry::sdk::trace::SimpleSpanProcessor(std::move(jaeger_exporter)));
        const auto jaeger_resource = opentelemetry::sdk::resource::Resource::Create(
                opentelemetry::sdk::resource::ResourceAttributes{{"service.name", service_name}});
        const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
                new opentelemetry::sdk::trace::TracerProvider(std::move(processor), jaeger_resource));
        _tracer = provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
    } else {
        _tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("no-op", OPENTELEMETRY_SDK_VERSION);
    }
}

void Tracer::shutdown() {
    if (_tracer) {
        _tracer->CloseWithMicroseconds(1);
        _tracer = nullptr;
    }
}

void shutdown_tracer() {
    Tracer::release_instance();
}

bool Tracer::is_enabled() const {
    return !config::jaeger_endpoint.empty();
}

Span Tracer::start_trace(const std::string& trace_name) {
    return _tracer->StartSpan(trace_name);
}

Span Tracer::start_trace_txn(const std::string& trace_name, int64_t txn_id) {
    auto ret = _tracer->StartSpan(trace_name);
    ret->SetAttribute("txn_id", txn_id);
    return ret;
}

Span Tracer::start_trace_tablet(const std::string& trace_name, int64_t tablet_id) {
    auto ret = _tracer->StartSpan(trace_name);
    ret->SetAttribute("tablet_id", tablet_id);
    return ret;
}

Span Tracer::start_trace_txn_tablet(const std::string& trace_name, int64_t txn_id, int64_t tablet_id) {
    auto ret = _tracer->StartSpan(trace_name);
    ret->SetAttribute("txn_id", txn_id);
    ret->SetAttribute("tablet_id", tablet_id);
    return ret;
}

Span Tracer::add_span(const std::string& span_name, const Span& parent_span) {
    const auto parent_ctx = parent_span->GetContext();
    return add_span(span_name, parent_ctx);
}

Span Tracer::add_span(const std::string& span_name, const SpanContext& parent_ctx) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_ctx;
    return _tracer->StartSpan(span_name, span_opts);
}

Span Tracer::start_trace_or_add_span(const std::string& name, const std::string& trace_parent) {
    if (trace_parent.empty()) {
        return _tracer->StartSpan(name);
    } else {
        auto ctx = from_trace_parent(trace_parent);
        return add_span(name, ctx);
    }
}

// Traceparent header format
// length: 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55
// Value = 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00
//  base16(version) = 00
//  base16(trace-id) = 4bf92f3577b34da6a3ce929d0e0e4736
//  base16(parent-id) = 00f067aa0ba902b7
//  base16(trace-flags) = 00  // not sampled

constexpr size_t trace_parent_header_length = 55;

SpanContext Tracer::from_trace_parent(const std::string& trace_parent) {
    if (trace_parent.size() != trace_parent_header_length) {
        return SpanContext::GetInvalid();
    }
    vector<string> fields = strings::Split(trace_parent, "-");
    if (fields.size() != 4) {
        return SpanContext::GetInvalid();
    }
    if (fields[0].size() != 2 || fields[1].size() != 32 || fields[2].size() != 16 || fields[3].size() != 2) {
        return SpanContext::GetInvalid();
    }
    using trace::propagation::detail::IsValidHex;
    if (!IsValidHex(fields[0]) || !IsValidHex(fields[1]) || !IsValidHex(fields[2]) || !IsValidHex(fields[3])) {
        return SpanContext::GetInvalid();
    }
    using trace::propagation::detail::HexToBinary;
    uint8_t version = 0;
    HexToBinary(fields[0], &version, 1);
    if (version != 0) {
        return SpanContext::GetInvalid();
    }
    uint8_t trace_id_buf[16];
    HexToBinary(fields[1], trace_id_buf, 16);
    uint8_t span_id_buf[8];
    HexToBinary(fields[2], span_id_buf, 8);
    uint8_t flags;
    HexToBinary(fields[3], &flags, 1);
    return {trace::TraceId(trace_id_buf), trace::SpanId(span_id_buf), trace::TraceFlags(flags), true};
}

std::string Tracer::to_trace_parent(const SpanContext& context) {
    std::string ret;
    ret.resize(trace_parent_header_length);
    size_t cur = 0;
    ret[cur++] = '0';
    ret[cur++] = '0';
    ret[cur++] = '-';
    context.trace_id().ToLowerBase16(opentelemetry::nostd::span<char, 32>(&ret[cur], 32));
    cur += 32;
    ret[cur++] = '-';
    context.span_id().ToLowerBase16(opentelemetry::nostd::span<char, 16>(&ret[cur], 16));
    cur += 16;
    ret[cur++] = '-';
    context.trace_flags().ToLowerBase16(opentelemetry::nostd::span<char, 2>(&ret[cur], 2));
    cur += 2;
    DCHECK_EQ(cur, trace_parent_header_length);
    return ret;
}

} // namespace starrocks
