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

package com.starrocks.http;

import com.codahale.metrics.Histogram;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;

import static com.starrocks.http.HttpMetricRegistry.HTTP_CONNECTIONS_NUM;
import static com.starrocks.http.HttpMetricRegistry.HTTP_HANDLING_REQUESTS_NUM;
import static com.starrocks.http.HttpMetricRegistry.HTTP_REQUEST_HANDLE_LATENCY_MS;

/** Metrics for {@link HttpServerHandler}. */
public class HttpServerHandlerMetrics {

    /** An instance shared by all HttpServerHandlers. */
    private static final HttpServerHandlerMetrics INSTANCE = new HttpServerHandlerMetrics();

    public final LongCounterMetric httpConnectionsNum;
    public final LongCounterMetric handlingRequestsNum;
    public final Histogram requestHandleLatencyMs;

    private HttpServerHandlerMetrics() {
        HttpMetricRegistry httpMetricRegistry = HttpMetricRegistry.getInstance();
        this.httpConnectionsNum = new LongCounterMetric(HTTP_CONNECTIONS_NUM,
                Metric.MetricUnit.NOUNIT, "the number of established http connections currently");
        httpMetricRegistry.registerCounter(httpConnectionsNum);
        this.handlingRequestsNum = new LongCounterMetric(HTTP_HANDLING_REQUESTS_NUM, Metric.MetricUnit.NOUNIT,
                "the number of http requests that is being handled");
        httpMetricRegistry.registerCounter(handlingRequestsNum);
        this.requestHandleLatencyMs = httpMetricRegistry.registerHistogram(HTTP_REQUEST_HANDLE_LATENCY_MS);
    }

    public static HttpServerHandlerMetrics getInstance() {
        return INSTANCE;
    }
}
