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

package com.starrocks.common;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.trace.SpanProcessor;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TraceManagerTest {

    private static final String SERVICE_NAME = "starrocks-fe";

    @Before
    public void resetConfig() {
        Config.jaeger_grpc_endpoint = "";
        Config.otlp_exporter_grpc_endpoint = "";
    }

    @Before
    public void resetTracer() {
        TraceManager.setTracer(null);
    }

    @Test
    public void shouldReturnNoOpTracerWhenTracingConfigIsEmpty() {
        Tracer tracer = TraceManager.getTracer();

        assertEquals(OpenTelemetry.noop().getTracer(SERVICE_NAME), tracer);
    }

    @Test
    public void shouldConstructTracer() {
        Config.jaeger_grpc_endpoint = "http://localhost:14250";
        Config.otlp_exporter_grpc_endpoint = "http://localhost:4317";

        Tracer tracer = TraceManager.getTracer();
        tracer.spanBuilder("test-span").startSpan().end();
    }

    @Test
    public void shouldReturnEmptyListWhenTracingConfigIsEmpty() {
        List<SpanProcessor> processors = TraceManager.getSpanProcessors();

        assertEquals(0, processors.size());
    }

    @Test
    public void shouldReturnSingleProcessorWhenJaegerExportEnabled() {
        Config.jaeger_grpc_endpoint = "http://localhost:14250";

        List<SpanProcessor> processors = TraceManager.getSpanProcessors();

        assertEquals(1, processors.size());
    }

    @Test
    public void shouldReturnSingleProcessorWhenOtlpExportEnabled() {
        Config.otlp_exporter_grpc_endpoint = "http://localhost:4317";

        List<SpanProcessor> processors = TraceManager.getSpanProcessors();

        assertEquals(1, processors.size());
    }

    @Test
    public void shouldReturnMultipleProcessorsWhenMultipleSpanExportEnabled() {
        Config.jaeger_grpc_endpoint = "http://localhost:14250";
        Config.otlp_exporter_grpc_endpoint = "http://localhost:4317";

        List<SpanProcessor> processors = TraceManager.getSpanProcessors();

        assertEquals(2, processors.size());
    }
}
