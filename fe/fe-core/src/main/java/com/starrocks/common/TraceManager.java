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

package com.starrocks.common;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.internal.TemporaryBuffers;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
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
    private static final String SERVICE_NAME = "starrocks-fe";
    private static volatile Tracer instance = null;

    public static Tracer getTracer() {
        if (instance == null) {
            synchronized (TraceManager.class) {
                if (instance == null) {
                    if (!Config.jaeger_grpc_endpoint.isEmpty()) {
                        OpenTelemetrySdkBuilder builder = OpenTelemetrySdk.builder();
                        SpanProcessor processor = BatchSpanProcessor.builder(
                                JaegerGrpcSpanExporter.builder().setEndpoint(Config.jaeger_grpc_endpoint)
                                        .build()).build();
                        Resource resource = Resource.builder().put("service.name", SERVICE_NAME).build();
                        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                                .addSpanProcessor(processor)
                                .setResource(resource)
                                .build();
                        builder.setTracerProvider(sdkTracerProvider);
                        OpenTelemetry openTelemetry = builder.buildAndRegisterGlobal();
                        instance = openTelemetry.getTracer(SERVICE_NAME);
                    } else {
                        instance = GlobalOpenTelemetry.get().getTracer(SERVICE_NAME);
                    }
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

    /**
     * Start a fake noop span, there are many code paths run in replay process, these events are duplicates in most cases,
     * so we should disable tracing for these cases, using this noop span in those cases, just to avoid null checks in code.
     * For example:
     * <pre>
     *  Span span;
     *  if (not in replay) {
     *      span = startSpan();
     *  }
     *  if (not in replay) {
     *      span.setAttribute("key", "value");
     *  }
     *  ...
     *  if (not in replay) {
     *      span.end();
     *  }
     *
     *  become:
     *  Span span;
     *  if (not in replay) {
     *      span = startSpan();
     *  } else {
     *      span = startNoopSpan();
     *  }
     *
     *  span.setAttribute("key", "value");
     *  ...
     *  span.end();
     *  </pre>
     */
    public static Span startNoopSpan() {
        return new NoopSpan();
    }

    public static String toTraceParent(SpanContext spanContext) {
        if (!spanContext.isValid()) {
            return null;
        }
        char[] chars = TemporaryBuffers.chars(55);
        chars[0] = "00".charAt(0);
        chars[1] = "00".charAt(1);
        chars[2] = '-';
        String traceId = spanContext.getTraceId();
        traceId.getChars(0, traceId.length(), chars, 3);
        chars[35] = '-';
        String spanId = spanContext.getSpanId();
        spanId.getChars(0, spanId.length(), chars, 36);
        chars[52] = '-';
        String traceFlagsHex = spanContext.getTraceFlags().asHex();
        chars[53] = traceFlagsHex.charAt(0);
        chars[54] = traceFlagsHex.charAt(1);
        return new String(chars, 0, 55);
    }
}
