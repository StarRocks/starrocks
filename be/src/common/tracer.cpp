// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#include "tracer.h"

#include "opentelemetry/exporters/ostream/span_exporter.h"

namespace starrocks {

namespace sdktrace = opentelemetry::sdk::trace;
namespace trace = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;

Tracer::Tracer(opentelemetry::nostd::string_view service_name) {
    init(service_name);
}

void Tracer::init(opentelemetry::nostd::string_view service_name) {
    if (!tracer) {
        auto exporter =
                std::unique_ptr<sdktrace::SpanExporter>(new opentelemetry::exporter::trace::OStreamSpanExporter);
        auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(
                new opentelemetry::sdk::trace::SimpleSpanProcessor(std::move(exporter)));
        const auto jaeger_resource = opentelemetry::sdk::resource::Resource::Create(
                std::move(opentelemetry::sdk::resource::ResourceAttributes{{"service.name", service_name}}));
        const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
                new opentelemetry::sdk::trace::TracerProvider(std::move(processor), jaeger_resource));
        tracer = provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
    }
}

void Tracer::shutdown() {
    if (tracer) {
        tracer->CloseWithMicroseconds(1);
    }
}

Span Tracer::start_trace(opentelemetry::nostd::string_view trace_name) {
    return tracer->StartSpan(trace_name);
}

Span Tracer::add_span(opentelemetry::nostd::string_view span_name, const Span& parent_span) {
    if (is_enabled() && parent_span) {
        const auto parent_ctx = parent_span->GetContext();
        return add_span(span_name, parent_ctx);
    }
}

Span Tracer::add_span(opentelemetry::nostd::string_view span_name, const SpanContext& parent_ctx) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_ctx;

    return tracer->StartSpan(span_name, span_opts);
}

bool Tracer::is_enabled() const {
    return true;
}

} // namespace starrocks