// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.common;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

public class TraceManager {
    private static final String SERVICE_NAME = "starrocks";
    private static Tracer instance = null;

    public static Tracer getTracer() {
        if (instance == null) {
            synchronized (TraceManager.class) {
                if (instance == null) {
                    OpenTelemetrySdkBuilder builder = OpenTelemetrySdk.builder();
                    if (!Config.jaeger_grpc_endpoint.isEmpty()) {
                        SpanProcessor processor = BatchSpanProcessor.builder(
                                JaegerGrpcSpanExporter.builder().setEndpoint(Config.jaeger_grpc_endpoint)
                                        .build()).build();
                        Resource resource = Resource.builder().put("service.name", SERVICE_NAME).build();
                        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                                .addSpanProcessor(processor)
                                .setResource(resource)
                                .build();
                        builder.setTracerProvider(sdkTracerProvider);
                    }
                    OpenTelemetry openTelemetry = builder.buildAndRegisterGlobal();
                    instance = openTelemetry.getTracer(SERVICE_NAME);
                }
            }
        }
        return instance;
    }

    public static Span startSpan(String name, Span parent) {
        return getTracer().spanBuilder(name)
                .setParent(Context.current().with(parent)).startSpan();
    }

    public static Span startSpan(String name) {
        return getTracer().spanBuilder(name).startSpan();
    }
}
