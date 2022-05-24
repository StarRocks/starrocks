// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#include "common/tracer.h"

#include <opentelemetry/exporters/jaeger/jaeger_exporter.h>

#include "common/config.h"

namespace starrocks {

namespace sdktrace = opentelemetry::sdk::trace;
namespace trace = opentelemetry::trace;

Tracer::~Tracer() {
    shutdown();
}

Tracer& Tracer::Instance() {
    static Tracer global_tracer;
    static std::once_flag oc;
    std::call_once(oc, [&]() { global_tracer.init("STARROCKS-BE"); });
    return global_tracer;
}

void Tracer::init(const std::string& service_name) {
    if (!config::jaeger_endpoint.empty()) {
        opentelemetry::exporter::jaeger::JaegerExporterOptions opts;
        opts.endpoint = config::jaeger_endpoint;
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
