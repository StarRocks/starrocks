// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#include "common/tracer.h"

#include <opentelemetry/exporters/jaeger/jaeger_exporter.h>

namespace starrocks {

namespace sdktrace = opentelemetry::sdk::trace;
namespace trace = opentelemetry::trace;

Tracer::Tracer(const std::string& service_name, const TracerOptions& tracer_opts) : _tracer_options(tracer_opts) {
    init(service_name);
}

void Tracer::init(const std::string& service_name) {
    if (_tracer == nullptr) {
        opentelemetry::exporter::jaeger::JaegerExporterOptions opts;
        opts.endpoint = _tracer_options.jaeger_endpoint;
        opts.server_port = _tracer_options.jaeger_server_port;
        auto jaeger_exporter = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(
                new opentelemetry::exporter::jaeger::JaegerExporter(opts));
        auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(
                new opentelemetry::sdk::trace::SimpleSpanProcessor(std::move(jaeger_exporter)));
        const auto jaeger_resource = opentelemetry::sdk::resource::Resource::Create(
                std::move(opentelemetry::sdk::resource::ResourceAttributes{{"service.name", service_name}}));
        const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
                new opentelemetry::sdk::trace::TracerProvider(std::move(processor), jaeger_resource));
        _tracer = provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
    }
}

void Tracer::shutdown() {
    if (_tracer != nullptr) {
        _tracer->CloseWithMicroseconds(1);
    }
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
