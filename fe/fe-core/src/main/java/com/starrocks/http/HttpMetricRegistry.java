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
import com.codahale.metrics.MetricRegistry;
import com.starrocks.metric.CounterMetric;
import com.starrocks.metric.GaugeMetric;
import com.starrocks.metric.MetricVisitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HttpMetricRegistry {

    private static final HttpMetricRegistry INSTANCE = new HttpMetricRegistry();

    // ================= HTTP Metrics =================

    /** A gauge metric. The number of http workers which determines the concurrency of http requests. */
    public static final String HTTP_WORKERS_NUM = "http_workers_num";

    /**
     * A gauge metric. The number of tasks that are pending for processing in the queues of
     * http workers. Use Netty's NioEventLoop#pendingTasks to get the number, but as the API
     * said, it can be expensive as it depends on the internal implementation. So this metric
     * only be available when <code>Config#enable_http_detail_metrics</code> is enabled.
     */
    public static final String HTTP_WORKER_PENDING_TASKS_NUM = "http_worker_pending_tasks_num";

    /** A counter metric. The number of http connections. */
    public static final String HTTP_CONNECTIONS_NUM = "http_connections_num";

    /** A counter metric. The number of http requests that is being handled by {@link BaseAction#handleRequest}. */
    public static final String HTTP_HANDLING_REQUESTS_NUM = "http_handling_requests_num";

    /** A histogram metric. The latency in milliseconds to handle http requests by {@link BaseAction#handleRequest}. */
    public static final String HTTP_REQUEST_HANDLE_LATENCY_MS = "http_request_handle_latency_ms";

    // ================= Transaction Stream Load Metrics =================

    /** A counter metric. The number of begin operations that are being handled. */
    public static final String TXN_STREAM_LOAD_BEGIN_NUM = "txn_stream_load_begin_num";

    /** A histogram metric. The latency in milliseconds to handle begin operations. */
    public static final String TXN_STREAM_LOAD_BEGIN_LATENCY_MS = "txn_stream_load_begin_latency_ms";

    /** A counter metric. The number of load operations that are being handled. */
    public static final String TXN_STREAM_LOAD_LOAD_NUM = "txn_stream_load_load_num";

    /** A histogram metric. The latency in milliseconds to handle load operations. */
    public static final String TXN_STREAM_LOAD_LOAD_LATENCY_MS = "txn_stream_load_load_latency_ms";

    /** A counter metric. The number of prepare operations that are being handled. */
    public static final String TXN_STREAM_LOAD_PREPARE_NUM = "txn_stream_load_prepare_num";

    /** A histogram metric. The latency in milliseconds to handle prepare operations. */
    public static final String TXN_STREAM_LOAD_PREPARE_LATENCY_MS = "txn_stream_load_prepare_latency_ms";

    /** A counter metric. The number of commit operations that are being handled. */
    public static final String TXN_STREAM_LOAD_COMMIT_NUM = "txn_stream_load_commit_num";

    /** A histogram metric. The latency in milliseconds to handle commit operations. */
    public static final String TXN_STREAM_LOAD_COMMIT_LATENCY_MS = "txn_stream_load_commit_latency_ms";

    /** A counter metric. The number of rollback operations that are being handled. */
    public static final String TXN_STREAM_LOAD_ROLLBACK_NUM = "txn_stream_load_rollback_num";

    /** A histogram metric. The latency in milliseconds to handle rollback operations. */
    public static final String TXN_STREAM_LOAD_ROLLBACK_LATENCY_MS = "txn_stream_load_rollback_latency_ms";

    // ================= Metric maps =================

    private final Map<String, CounterMetric<?>> counterMetrics = new ConcurrentHashMap<>();
    private final Map<String, GaugeMetric<?>> gaugeMetrics = new ConcurrentHashMap<>();
    private final MetricRegistry histoMetricRegistry = new MetricRegistry();

    public static HttpMetricRegistry getInstance() {
        return INSTANCE;
    }

    private HttpMetricRegistry() {
    }

    public synchronized void registerCounter(CounterMetric<?> counterMetric) {
        counterMetrics.put(counterMetric.getName(), counterMetric);
    }

    public synchronized void registerGauge(GaugeMetric<?> gaugeMetric) {
        gaugeMetrics.put(gaugeMetric.getName(), gaugeMetric);
    }

    public synchronized Histogram registerHistogram(String name) {
        return histoMetricRegistry.histogram(name);
    }

    public void visit(MetricVisitor visitor) {
        for (CounterMetric<?> metric : counterMetrics.values()) {
            visitor.visit(metric);
        }

        for (GaugeMetric<?> metric : gaugeMetrics.values()) {
            visitor.visit(metric);
        }

        for (Map.Entry<String, Histogram> entry : histoMetricRegistry.getHistograms().entrySet()) {
            visitor.visitHistogram(entry.getKey(), entry.getValue());
        }
    }
}
