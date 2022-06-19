// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#include "common/tracer.h"

#include <opentelemetry/exporters/jaeger/jaeger_exporter.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
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
    std::call_once(oc, [&]() { global_tracer.init("starrocks"); });
    return global_tracer;
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
        long port = strtol(host_port[1].c_str(), NULL, 10);
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
    _tracer->CloseWithMicroseconds(1);
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

} // namespace starrocks
