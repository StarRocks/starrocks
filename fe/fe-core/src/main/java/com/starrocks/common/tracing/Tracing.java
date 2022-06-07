// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/AuditLog.java

package com.starrocks.common.tracing;

import com.starrocks.common.Config;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.util.concurrent.TimeUnit;

public class Tracing {
    private static String SERVICE_NAME = "STARROCKS_FE";
    private static OpenTelemetry openTelemetry = OpenTelemetry.noop();

    public static void initTracing() {
        if (Config.jaeger_server_endpoint.isEmpty()) {
            return;
        }
        // Export traces to Jaeger server.
        JaegerGrpcSpanExporter jaegerExporter =
                JaegerGrpcSpanExporter.builder()
                        .setTimeout(30, TimeUnit.SECONDS)
                        .setEndpoint(Config.jaeger_server_endpoint)
                        .build();
        Resource serviceNameResource =
                Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), SERVICE_NAME));
        // Send a batch of spans if ScheduleDelay time or MaxExportBatchSize is reached.
        BatchSpanProcessor spanProcessor =
                BatchSpanProcessor.builder(jaegerExporter)
                        .setScheduleDelay(Config.tracing_schedule_delay_time_ms, TimeUnit.MILLISECONDS)
                        .setMaxExportBatchSize(Config.tracing_max_export_batch_size)
                        .build();
        // Set to process the spans by the Jaeger Exporter.
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder().addSpanProcessor(spanProcessor)
                .setResource(Resource.getDefault().merge(serviceNameResource)).build();
        openTelemetry =
                OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
        // It's always a good idea to shut down the SDK cleanly at JVM exit.
        Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::shutdown));
    }

    public static OpenTelemetry getTracer() {
        return openTelemetry;
    }
}
