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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WarehouseMetricMgr {
    private static final Logger LOG = LogManager.getLogger(WarehouseMetricMgr.class);

    private static final String UNFINISHED_QUERY = "unfinished_query";
    private static final String UNFINISHED_BACKUP_JOB = "unfinished_backup_job";
    private static final String UNFINISHED_RESTORE_JOB = "unfinished_restore_job";
    private static final String LAST_FINISHED_JOB_TIMESTAMP = "last_finished_job_timestamp";

    private static final String QUERY_WAREHOUSE = "query_warehouse";
    private static final String QUERY_WAREHOUSE_ERR = "query_warehouse_err";
    private static final String QUERY_WAREHOUSE_LATENCY = "query_warehouse_latency";

    private static final String WAREHOUSE_QUERY_QUEUE_TOTAL = "warehouse_query_queue_total";
    private static final String WAREHOUSE_QUERY_QUEUE_PENDING = "warehouse_query_queue_pending";
    private static final String WAREHOUSE_QUERY_QUEUE_TIMEOUT = "warehouse_query_queue_timeout";

    private WarehouseMetricMgr() {
    }

    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_QUERY_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_BACKUP_JOB_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_RESTORE_JOB_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, GaugeMetricImpl<Long>> LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, LongCounterMetric> WAREHOUSE_QUERY_COUNTER_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> WAREHOUSE_QUERY_ERR_COUNTER_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, QueryWarehouseLatencyMetrics> WAREHOUSE_QUERY_LATENCY_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> WAREHOUSE_QUERY_QUEUE_TOTAL_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> WAREHOUSE_QUERY_QUEUE_PENDING_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> WAREHOUSE_QUERY_QUEUE_TIMEOUT_MAP
            = new ConcurrentHashMap<>();

    public static Map<Long, Long> getUnfinishedQueries() {
        return metricMapToValueMap(UNFINISHED_QUERY_COUNTER_MAP);
    }

    public static Map<Long, Long> getUnfinishedBackupJobs() {
        return metricMapToValueMap(UNFINISHED_BACKUP_JOB_COUNTER_MAP);
    }

    public static Map<Long, Long> getUnfinishedRestoreJobs() {
        return metricMapToValueMap(UNFINISHED_RESTORE_JOB_COUNTER_MAP);
    }

    public static Map<Long, Long> getLastFinishedJobTimestampMs() {
        return metricMapToValueMap(LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP);
    }

    public static void increaseUnfinishedQueries(Long warehouseId, Long delta) {
        if (delta < 0) {
            setLastFinishedJobTimestamp(warehouseId, System.currentTimeMillis());
        }

        LongCounterMetric metric = getOrCreateMetric(
                warehouseId,
                UNFINISHED_QUERY_COUNTER_MAP,
                UNFINISHED_QUERY,
                () -> new LongCounterMetric(UNFINISHED_QUERY, Metric.MetricUnit.REQUESTS,
                        "current unfinished queries of the warehouse")
        );
        metric.increase(delta);
    }

    public static void increaseUnfinishedBackupJobs(Long warehouseId, Long delta) {
        if (delta < 0) {
            setLastFinishedJobTimestamp(warehouseId, System.currentTimeMillis());
        }

        LongCounterMetric metric = getOrCreateMetric(
                warehouseId,
                UNFINISHED_BACKUP_JOB_COUNTER_MAP,
                UNFINISHED_BACKUP_JOB,
                () -> new LongCounterMetric(UNFINISHED_BACKUP_JOB, Metric.MetricUnit.REQUESTS,
                        "current unfinished backup jobs of the warehouse")
        );
        metric.increase(delta);
    }

    public static void increaseUnfinishedRestoreJobs(Long warehouseId, Long delta) {
        if (delta < 0) {
            setLastFinishedJobTimestamp(warehouseId, System.currentTimeMillis());
        }

        LongCounterMetric metric = getOrCreateMetric(
                warehouseId,
                UNFINISHED_RESTORE_JOB_COUNTER_MAP,
                UNFINISHED_RESTORE_JOB,
                () -> new LongCounterMetric(UNFINISHED_RESTORE_JOB, Metric.MetricUnit.REQUESTS,
                        "current unfinished queries of the warehouse")
        );
        metric.increase(delta);
    }

    private static void setLastFinishedJobTimestamp(Long warehouseId, Long timestamp) {
        GaugeMetricImpl<Long> metric = getOrCreateMetric(
                warehouseId,
                LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP,
                LAST_FINISHED_JOB_TIMESTAMP,
                () -> new GaugeMetricImpl<>(LAST_FINISHED_JOB_TIMESTAMP, Metric.MetricUnit.MICROSECONDS,
                        "the timestamp of last finished job")
        );
        metric.setValue(timestamp);
    }

    private static <T extends Metric<Long>> T getOrCreateMetric(Long warehouseId, Map<Long, T> metricMap,
                                                                String metricName, Supplier<T> metricSupplier) {
        if (!metricMap.containsKey(warehouseId)) {
            synchronized (WarehouseMetricMgr.class) {
                if (!metricMap.containsKey(warehouseId)) {
                    Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
                    String whName = wh == null ? "unknown" : wh.getName();

                    T metric = metricSupplier.get();
                    metric.addLabel(new MetricLabel("warehouse_id", warehouseId.toString()));
                    metric.addLabel(new MetricLabel("warehouse_name", whName));

                    metricMap.put(warehouseId, metric);
                    MetricRepo.addMetric(metric);

                    LOG.info("Add {} metric, warehouse is {}", metricName, warehouseId);
                }
            }
        }
        return metricMap.get(warehouseId);
    }

    private static <T extends Metric<Long>> Map<Long, Long> metricMapToValueMap(Map<Long, T> metricMap) {
        return metricMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
    }

    /**
     * For the metric {@code starrocks_fe_query_warehouse}.
     */
    public static void increaseQuery(ConnectContext ctx, long delta) {
        LongCounterMetric metrics = createQueryWarehouseMetrics(
                WAREHOUSE_QUERY_COUNTER_MAP, QUERY_WAREHOUSE, "query warehouse", ctx);
        metrics.increase(delta);
    }
    /**
     * For the metric {@code starrocks_fe_query_warehouse_err}.
     */
    public static void increaseQueryErr(ConnectContext ctx, long delta) {
        LongCounterMetric metrics = createQueryWarehouseMetrics(
                WAREHOUSE_QUERY_ERR_COUNTER_MAP, QUERY_WAREHOUSE_ERR, "query err warehouse", ctx);
        metrics.increase(delta);
    }
    public static void visitQueryLatency() {
        for (String warehouseName : WAREHOUSE_QUERY_LATENCY_MAP.keySet()) {
            QueryWarehouseLatencyMetrics metrics = WAREHOUSE_QUERY_LATENCY_MAP.get(warehouseName);
            metrics.update();
        }
    }
    /**
     * For the metric {@code starrocks_fe_query_warehouse_latency}.
     */
    public static void updateQueryLatency(ConnectContext ctx, Long elapseMs) {
        QueryWarehouseLatencyMetrics metrics = createQueryWarehouseLatencyMetrics(ctx);
        metrics.histogram.update(elapseMs);
    }
    private static LongCounterMetric createQueryWarehouseMetrics(Map<String, LongCounterMetric> cacheMap, String metricsName,
                                                                 String metricsMsg, ConnectContext ctx) {
        String warehouseName = ctx.getCurrentWarehouseName();
        if (!cacheMap.containsKey(warehouseName)) {
            synchronized (WarehouseMetricMgr.class) {
                if (!cacheMap.containsKey(warehouseName)) {
                    LongCounterMetric metric = new LongCounterMetric(metricsName, Metric.MetricUnit.REQUESTS, metricsMsg);
                    metric.addLabel(new MetricLabel("name", warehouseName));
                    cacheMap.put(warehouseName, metric);
                    MetricRepo.addMetric(metric);
                    LOG.info("Add {} metric, warehouse name is {}", metricsName, warehouseName);
                }
            }
        }
        return cacheMap.get(warehouseName);
    }
    private static QueryWarehouseLatencyMetrics createQueryWarehouseLatencyMetrics(ConnectContext ctx) {
        String warehouseName = ctx.getCurrentWarehouseName();
        return WAREHOUSE_QUERY_LATENCY_MAP.computeIfAbsent(warehouseName,
                currWarehouseName -> new QueryWarehouseLatencyMetrics(QUERY_WAREHOUSE_LATENCY, currWarehouseName));
    }
    private static final class QueryWarehouseLatencyMetrics {
        private static final String[] QUERY_LATENCY_LABELS =
                {"mean", "75_quantile", "95_quantile", "98_quantile", "99_quantile", "999_quantile"};
        private final MetricRegistry metricRegistry;
        private Histogram histogram;
        private final List<GaugeMetricImpl<Double>> metricsList;
        private final String metricName;
        private QueryWarehouseLatencyMetrics(String metricName, String warehouseName) {
            this.metricName = metricName;
            this.metricRegistry = new MetricRegistry();
            initHistogram(metricName);
            this.metricsList = new ArrayList<>();
            for (String label : QUERY_LATENCY_LABELS) {
                GaugeMetricImpl<Double> metrics = new GaugeMetricImpl<>(
                        metricName, Metric.MetricUnit.MILLISECONDS, label + " of warehouse query latency");
                metrics.addLabel(new MetricLabel("type", label));
                metrics.addLabel(new MetricLabel("name", warehouseName));
                metrics.setValue(0.0);
                MetricRepo.addMetric(metrics);
                LOG.info("Add {} metric, warehouse name is {}", QUERY_WAREHOUSE_LATENCY, warehouseName);
                this.metricsList.add(metrics);
            }
        }
        private void initHistogram(String metricsName) {
            this.histogram = this.metricRegistry.histogram(metricsName);
        }
        private void update() {
            Histogram oldHistogram = this.histogram;
            this.metricRegistry.remove(this.metricName);
            initHistogram(metricName);
            Snapshot snapshot = oldHistogram.getSnapshot();
            metricsList.get(0).setValue(snapshot.getMedian());
            metricsList.get(1).setValue(snapshot.get75thPercentile());
            metricsList.get(2).setValue(snapshot.get95thPercentile());
            metricsList.get(3).setValue(snapshot.get98thPercentile());
            metricsList.get(4).setValue(snapshot.get99thPercentile());
            metricsList.get(5).setValue(snapshot.get999thPercentile());
        }
    }

    public static void increaseQueuedQuery(ConnectContext ctx, long delta) {
        if (delta > 0) {
            LongCounterMetric metrics = createQueryWarehouseMetrics(
                    WAREHOUSE_QUERY_QUEUE_TOTAL_MAP, WAREHOUSE_QUERY_QUEUE_TOTAL,
                    "the number of total history queued queries of this warehouse", ctx);
            metrics.increase(delta);
        }

        LongCounterMetric metrics = createQueryWarehouseMetrics(
                WAREHOUSE_QUERY_QUEUE_PENDING_MAP, WAREHOUSE_QUERY_QUEUE_PENDING,
                "the number of pending queries of this warehouse", ctx);
        metrics.increase(delta);
    }

    public static void increaseTimeoutQueuedQuery(ConnectContext ctx, long delta) {
        LongCounterMetric metrics = createQueryWarehouseMetrics(
                WAREHOUSE_QUERY_QUEUE_TIMEOUT_MAP, WAREHOUSE_QUERY_QUEUE_TIMEOUT,
                "the number of pending timeout queries of this warehouse", ctx);
        metrics.increase(delta);
    }
}