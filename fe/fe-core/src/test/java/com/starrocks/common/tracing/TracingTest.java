// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/util/DebugUtilTest.java

package com.starrocks.common.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import org.junit.Assert;
import org.junit.Test;

public class TracingTest {

    @Test
    public void basicTest() {
        try {
        Tracing.initTracing();
        OpenTelemetry openTelemetry = Tracing.getTracer();
        Tracer tracer = openTelemetry.getTracer("TracerTest");
        Span rootSpan = tracer.spanBuilder("root").startSpan();
        rootSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "PUT");
        rootSpan.setAttribute("method", "doWork");
        Thread.sleep(2000);
        rootSpan.end();
        Assert.assertFalse( rootSpan.isRecording());
        Assert.assertFalse(rootSpan.getSpanContext().isSampled());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
