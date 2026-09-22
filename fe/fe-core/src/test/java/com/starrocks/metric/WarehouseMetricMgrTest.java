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

package com.starrocks.metric;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class WarehouseMetricMgrTest {
    private static final long WH2_ID = 1001L;
    private static final String WH2_NAME = "wh2";
    private static final String DEFAULT = WarehouseManager.DEFAULT_WAREHOUSE_NAME;

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    private static String label(Metric<?> metric, String key) {
        return metric.getLabels().stream()
                .filter(l -> l.getKey().equals(key))
                .map(MetricLabel::getValue)
                .findFirst()
                .orElse(null);
    }

    /** Counters of the series without a CN group (cngroup_name = ""), keyed by warehouse name. */
    private static Map<String, Long> counterByWarehouse(String metricName) {
        List<Metric> metrics = MetricRepo.getMetricsByName(metricName);
        return metrics.stream()
                .filter(m -> label(m, "cngroup_name") == null || "".equals(label(m, "cngroup_name")))
                .collect(Collectors.toMap(
                        m -> label(m, "warehouse_name"),
                        m -> ((LongCounterMetric) m).getValue()));
    }

    private static Long counter(String metricName, String warehouseName, String cngroupName) {
        return MetricRepo.getMetricsByName(metricName).stream()
                .filter(m -> warehouseName.equals(label(m, "warehouse_name")) && cngroupName.equals(label(m, "cngroup_name")))
                .map(m -> ((LongCounterMetric) m).getValue())
                .findFirst()
                .orElse(null);
    }

    private static double gauge(String metricName, String warehouseName, String type) {
        return MetricRepo.getMetricsByName(metricName).stream()
                .filter(m -> warehouseName.equals(label(m, "warehouse_name"))
                        && (label(m, "cngroup_name") == null || "".equals(label(m, "cngroup_name")))
                        && (type == null || type.equals(label(m, "type"))))
                .map(m -> ((GaugeMetricImpl<Double>) m).getValue())
                .findFirst()
                .orElseThrow();
    }

    private static double latency(String warehouseName, String type) {
        return gauge(WarehouseMetricMgr.WAREHOUSE_QUERY_LATENCY, warehouseName, type);
    }

    /** Mirror the real flow: the session selects the warehouse and StmtExecutor copies it into the audit builder. */
    private static void useWarehouse(ConnectContext ctx, String warehouseName) {
        ctx.getSessionVariable().setWarehouseName(warehouseName);
        ctx.getAuditEventBuilder().setWarehouse(warehouseName);
    }

    @Test
    public void testWarehouseQueryMetrics() {
        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouseAllowNull(String warehouseName) {
                return WH2_NAME.equals(warehouseName) ? new DefaultWarehouse(WH2_ID, WH2_NAME) : null;
            }
        };
        WarehouseManager warehouseManager = new WarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouseManager;
            }
        };

        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());

        // default warehouse: 3 requests, 2 queries (1 failed with timeout), latencies 10ms and 30ms
        useWarehouse(ctx, DEFAULT);
        for (int i = 0; i < 3; i++) {
            WarehouseMetricMgr.increaseRequest(ctx, 1L);
        }
        WarehouseMetricMgr.increaseQuery(ctx, 1L);
        WarehouseMetricMgr.increaseQuery(ctx, 1L);
        WarehouseMetricMgr.increaseQueryErr(ctx, 1L);
        WarehouseMetricMgr.increaseQueryTimeout(ctx, 1L);
        WarehouseMetricMgr.increaseQueryAnalysisErr(ctx, 2L);
        WarehouseMetricMgr.increaseQueryInternalErr(ctx, 3L);
        WarehouseMetricMgr.increaseSlowQuery(ctx, 4L);
        WarehouseMetricMgr.updateQueryLatency(ctx, 10L);
        WarehouseMetricMgr.updateQueryLatency(ctx, 30L);

        // wh2: 5 requests, 5 queries, none failed, latencies 100..104ms
        useWarehouse(ctx, WH2_NAME);
        WarehouseMetricMgr.increaseRequest(ctx, 5L);
        WarehouseMetricMgr.increaseQuery(ctx, 5L);
        for (int i = 0; i < 5; i++) {
            WarehouseMetricMgr.updateQueryLatency(ctx, 100L + i);
        }

        // unknown warehouse: silently skipped, no metric created
        useWarehouse(ctx, "not_exist");
        WarehouseMetricMgr.increaseRequest(ctx, 1L);
        WarehouseMetricMgr.increaseQuery(ctx, 1L);
        WarehouseMetricMgr.increaseQueryErr(ctx, 1L);
        WarehouseMetricMgr.updateQueryLatency(ctx, 1L);

        // disabled by config: skipped
        boolean old = Config.enable_per_warehouse_query_metrics;
        Config.enable_per_warehouse_query_metrics = false;
        try {
            useWarehouse(ctx, WH2_NAME);
            WarehouseMetricMgr.increaseRequest(ctx, 100L);
            WarehouseMetricMgr.increaseQuery(ctx, 100L);
            WarehouseMetricMgr.increaseQueryErr(ctx, 100L);
            WarehouseMetricMgr.increaseQueryTimeout(ctx, 100L);
            WarehouseMetricMgr.updateQueryLatency(ctx, 9999L);
        } finally {
            Config.enable_per_warehouse_query_metrics = old;
        }

        // counters: every counter of a warehouse exists as soon as the warehouse handled one request
        Map<String, Long> request = counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_REQUEST_TOTAL);
        Map<String, Long> total = counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL);
        Map<String, Long> err = counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_ERR);
        Map<String, Long> timeout = counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_TIMEOUT);
        Assertions.assertEquals(2, request.size());
        Assertions.assertEquals(2, total.size());
        Assertions.assertEquals(2, err.size());
        Assertions.assertEquals(2, timeout.size());
        Assertions.assertEquals(3L, request.get(DEFAULT));
        Assertions.assertEquals(2L, total.get(DEFAULT));
        Assertions.assertEquals(1L, err.get(DEFAULT));
        Assertions.assertEquals(1L, timeout.get(DEFAULT));
        Assertions.assertEquals(5L, request.get(WH2_NAME));
        Assertions.assertEquals(5L, total.get(WH2_NAME));
        Assertions.assertEquals(0L, err.get(WH2_NAME));
        Assertions.assertEquals(0L, timeout.get(WH2_NAME));
        Assertions.assertEquals(2L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_ANALYSIS_ERR).get(DEFAULT));
        Assertions.assertEquals(3L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_INTERNAL_ERR).get(DEFAULT));
        Assertions.assertEquals(4L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_SLOW_QUERY).get(DEFAULT));
        Assertions.assertEquals(0L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_SLOW_QUERY).get(WH2_NAME));

        // labels carry both id and name
        for (Metric m : MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL)) {
            String expectedId = WH2_NAME.equals(label(m, "warehouse_name"))
                    ? String.valueOf(WH2_ID) : String.valueOf(WarehouseManager.DEFAULT_WAREHOUSE_ID);
            Assertions.assertEquals(expectedId, label(m, "warehouse_id"));
        }

        // latency gauges: one per type (same set as the cluster-wide query_latency) per warehouse,
        // populated only by the calculator tick
        List<Metric> latencyMetrics = MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_QUERY_LATENCY);
        Assertions.assertEquals(2 * WarehouseMetricMgr.QUERY_LATENCY_TYPES.length, latencyMetrics.size());
        Assertions.assertEquals(0.0, latency(WH2_NAME, "95_quantile"));
        Assertions.assertEquals(0.0, gauge(WarehouseMetricMgr.WAREHOUSE_QPS, WH2_NAME, null));

        WarehouseMetricMgr.updateCalculatedQueryMetrics(10L);

        // rates over a 10s interval
        Assertions.assertEquals(0.2, gauge(WarehouseMetricMgr.WAREHOUSE_QPS, DEFAULT, null), 1e-9);
        Assertions.assertEquals(0.3, gauge(WarehouseMetricMgr.WAREHOUSE_RPS, DEFAULT, null), 1e-9);
        Assertions.assertEquals(0.5, gauge(WarehouseMetricMgr.WAREHOUSE_QPS, WH2_NAME, null), 1e-9);
        Assertions.assertEquals(0.5, gauge(WarehouseMetricMgr.WAREHOUSE_RPS, WH2_NAME, null), 1e-9);

        // quantiles use the same nearest-rank algorithm as the cluster-wide query_latency gauges
        Assertions.assertEquals(20.0, latency(DEFAULT, "mean"));
        Assertions.assertEquals(30.0, latency(DEFAULT, "50_quantile"));
        Assertions.assertEquals(30.0, latency(DEFAULT, "95_quantile"));
        Assertions.assertEquals(102.0, latency(WH2_NAME, "mean"));
        Assertions.assertEquals(102.0, latency(WH2_NAME, "50_quantile"));
        Assertions.assertEquals(103.0, latency(WH2_NAME, "75_quantile"));
        Assertions.assertEquals(104.0, latency(WH2_NAME, "90_quantile"));
        Assertions.assertEquals(104.0, latency(WH2_NAME, "99_quantile"));
        Assertions.assertEquals(104.0, latency(WH2_NAME, "999_quantile"));

        // the next tick without traffic reports 0 rates and 0 latency (window semantics of the global gauges)
        WarehouseMetricMgr.updateCalculatedQueryMetrics(10L);
        Assertions.assertEquals(0.0, gauge(WarehouseMetricMgr.WAREHOUSE_QPS, WH2_NAME, null));
        Assertions.assertEquals(0.0, latency(WH2_NAME, "95_quantile"));
        Assertions.assertEquals(0.0, latency(DEFAULT, "mean"));
        // ...while the cumulative counters keep their values
        Assertions.assertEquals(5L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL).get(WH2_NAME));

        // a query-scope warehouse hint: the session still points at the default warehouse when the query is
        // accounted, but the audit event builder carries the execution warehouse -> attributed to wh2
        ctx.getSessionVariable().setWarehouseName(DEFAULT);
        ctx.getAuditEventBuilder().setWarehouse(WH2_NAME);
        WarehouseMetricMgr.increaseQuery(ctx, 1L);
        WarehouseMetricMgr.increaseQueryErr(ctx, 1L);
        WarehouseMetricMgr.updateQueryLatency(ctx, 7L);
        // requests are accounted before parsing, so they stay with the session warehouse
        WarehouseMetricMgr.increaseRequest(ctx, 1L);
        Assertions.assertEquals(6L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL).get(WH2_NAME));
        Assertions.assertEquals(1L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_ERR).get(WH2_NAME));
        Assertions.assertEquals(2L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL).get(DEFAULT));
        Assertions.assertEquals(4L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_REQUEST_TOTAL).get(DEFAULT));
        ctx.getAuditEventBuilder().reset();

        // a statement executed in a named CN group gets its own series, labelled with cngroup_name
        useWarehouse(ctx, WH2_NAME);
        ctx.getAuditEventBuilder().setCNGroup("cg1");
        WarehouseMetricMgr.increaseQuery(ctx, 3L);
        WarehouseMetricMgr.increaseQueryErr(ctx, 1L);
        ctx.getAuditEventBuilder().setCNGroup("cg2");
        WarehouseMetricMgr.increaseQuery(ctx, 4L);
        ctx.getAuditEventBuilder().setCNGroup("");
        Assertions.assertEquals(3L, counter(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL, WH2_NAME, "cg1"));
        Assertions.assertEquals(1L, counter(WarehouseMetricMgr.WAREHOUSE_QUERY_ERR, WH2_NAME, "cg1"));
        Assertions.assertEquals(4L, counter(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL, WH2_NAME, "cg2"));
        Assertions.assertEquals(6L, counter(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL, WH2_NAME, ""));
        for (Metric m : MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL)) {
            Assertions.assertNotNull(label(m, "cngroup_name"));
        }
        // requests are counted before parsing, so request_total / rps stay per warehouse without a CN group label
        for (Metric m : MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_REQUEST_TOTAL)) {
            Assertions.assertNull(label(m, "cngroup_name"));
        }
        Assertions.assertEquals(1, MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_REQUEST_TOTAL).stream()
                .filter(m -> WH2_NAME.equals(label(m, "warehouse_name"))).count());
        // dropping cg1 removes only cg1's series; cg2 and the CN-group-less series stay
        WarehouseMetricMgr.onDropCNGroup(WH2_ID, Set.of("cg2"));
        Assertions.assertNull(counter(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL, WH2_NAME, "cg1"));
        Assertions.assertEquals(4L, counter(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL, WH2_NAME, "cg2"));
        Assertions.assertEquals(6L, counter(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL, WH2_NAME, ""));

        // dropping a warehouse removes every one of its series, whatever the CN group
        WarehouseMetricMgr.onDropWarehouse(WH2_ID);
        Assertions.assertNull(counter(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL, WH2_NAME, "cg2"));
        Assertions.assertEquals(1, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL).size());
        Assertions.assertEquals(1, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_SLOW_QUERY).size());
        Assertions.assertEquals(1, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_REQUEST_TOTAL).size());
        Assertions.assertEquals(WarehouseMetricMgr.QUERY_LATENCY_TYPES.length,
                MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_QUERY_LATENCY).size());
        Assertions.assertEquals(1, MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_QPS).size());
        // and the next query on it recreates them from scratch
        useWarehouse(ctx, WH2_NAME);
        WarehouseMetricMgr.increaseQuery(ctx, 1L);
        Assertions.assertEquals(1L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL).get(WH2_NAME));

        // turning the mutable flag off removes every exported series on the next calculator tick...
        Config.enable_per_warehouse_query_metrics = false;
        try {
            WarehouseMetricMgr.updateCalculatedQueryMetrics(10L);
            Assertions.assertEquals(0, MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL).size());
            Assertions.assertEquals(0, MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_QUERY_LATENCY).size());
            Assertions.assertEquals(0, MetricRepo.getMetricsByName(WarehouseMetricMgr.WAREHOUSE_QPS).size());
        } finally {
            Config.enable_per_warehouse_query_metrics = old;
        }
        // ...and turning it back on starts over with the next query
        useWarehouse(ctx, DEFAULT);
        WarehouseMetricMgr.increaseQuery(ctx, 1L);
        Assertions.assertEquals(1L, counterByWarehouse(WarehouseMetricMgr.WAREHOUSE_QUERY_TOTAL).get(DEFAULT));
    }

    @Test
    public void testHistogramMetricExactSum() {
        // the reservoir keeps at most 1028 samples, so count * mean drifts; the sum must stay exact
        HistogramMetric histogram = new HistogramMetric("h");
        long expectedSum = 0;
        for (int i = 1; i <= 5000; i++) {
            histogram.update(i);
            expectedSum += i;
        }
        Assertions.assertEquals(5000, histogram.getCount());
        Assertions.assertEquals(expectedSum, histogram.getSum());

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("starrocks_fe");
        histogram.addLabel(new MetricLabel("warehouse_name", "wh"));
        visitor.visitHistogram(histogram);
        // exported as a double to keep the existing output format
        Assertions.assertTrue(visitor.build().contains("starrocks_fe_h_sum{warehouse_name=\"wh\"} " + (double) expectedSum),
                visitor.build());
    }
}
