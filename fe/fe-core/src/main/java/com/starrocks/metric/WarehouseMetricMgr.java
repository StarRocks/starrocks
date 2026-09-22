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

import com.codahale.metrics.MetricRegistry;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WarehouseMetricMgr {
    private static final Logger LOG = LogManager.getLogger(WarehouseMetricMgr.class);

    private static final String UNFINISHED_QUERY = "unfinished_query";
    private static final String UNFINISHED_BACKUP_JOB = "unfinished_backup_job";
    private static final String UNFINISHED_RESTORE_JOB = "unfinished_restore_job";
    private static final String LAST_FINISHED_JOB_TIMESTAMP = "last_finished_job_timestamp";

    // Per-warehouse counterparts of the cluster-wide FE query metrics
    // (request_total / query_total / query_err / query_timeout / query_analysis_err / query_internal_err /
    // slow_query / qps / rps / query_latency / query_latency_ms).
    public static final String WAREHOUSE_REQUEST_TOTAL = "warehouse_request_total";
    public static final String WAREHOUSE_QUERY_TOTAL = "warehouse_query_total";
    public static final String WAREHOUSE_QUERY_ERR = "warehouse_query_err";
    public static final String WAREHOUSE_QUERY_TIMEOUT = "warehouse_query_timeout";
    public static final String WAREHOUSE_QUERY_ANALYSIS_ERR = "warehouse_query_analysis_err";
    public static final String WAREHOUSE_QUERY_INTERNAL_ERR = "warehouse_query_internal_err";
    public static final String WAREHOUSE_SLOW_QUERY = "warehouse_slow_query";
    public static final String WAREHOUSE_QPS = "warehouse_qps";
    public static final String WAREHOUSE_RPS = "warehouse_rps";
    public static final String WAREHOUSE_QUERY_LATENCY = "warehouse_query_latency";
    public static final String WAREHOUSE_QUERY_LATENCY_MS = "warehouse_query_latency_ms";

    /**
     * Same {@code type} label values as the cluster-wide {@code query_latency} gauges, so dashboards can slice
     * the same statistic by warehouse.
     */
    static final String[] QUERY_LATENCY_TYPES =
            {"mean", "50_quantile", "75_quantile", "90_quantile", "95_quantile", "99_quantile", "999_quantile"};
    private static final double[] QUERY_LATENCY_QUANTILES = {0.5, 0.75, 0.90, 0.95, 0.99, 0.999};

    /**
     * Upper bound on the latency samples buffered per warehouse between two MetricCalculator ticks. Beyond it the
     * quantiles are computed from the first MAX_LATENCY_SAMPLES_PER_INTERVAL samples of the interval while mean
     * still covers every query.
     */
    static final int MAX_LATENCY_SAMPLES_PER_INTERVAL = 100_000;

    private WarehouseMetricMgr() {
    }

    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_QUERY_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_BACKUP_JOB_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_RESTORE_JOB_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, GaugeMetricImpl<Long>> LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP =
            new ConcurrentHashMap<>();

    // one entity per (warehouse, CN group) that has handled a request; cngroup is "" when the statement ran
    // without a CN group (shared-nothing mode, or metadata-only statements before a compute resource is acquired)
    private static final ConcurrentMap<QueryMetricsKey, WarehouseQueryMetrics> QUERY_METRICS_MAP = new ConcurrentHashMap<>();
    // requests are counted before the statement is parsed, i.e. before any CN group is known: one entity per warehouse
    private static final ConcurrentMap<Long, WarehouseRequestMetrics> REQUEST_METRICS_MAP = new ConcurrentHashMap<>();

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

    // ---------------------------------------------------------------------------------------------------------
    // Per-warehouse query metrics, fed from the same ConnectProcessor call sites as the cluster-wide ones.
    // ---------------------------------------------------------------------------------------------------------

    /**
     * {@code starrocks_fe_warehouse_request_total}: every request handled on behalf of the warehouse. Attributed to
     * the session's warehouse and carries no CN group label, since the request has not been parsed yet at this point.
     */
    public static void increaseRequest(ConnectContext ctx, long delta) {
        if (!Config.enable_per_warehouse_query_metrics || ctx == null) {
            return;
        }
        String warehouseName = ctx.getCurrentWarehouseName();
        Long warehouseId = resolveWarehouseId(warehouseName);
        if (warehouseId == null) {
            return;
        }
        WarehouseRequestMetrics metrics = REQUEST_METRICS_MAP.get(warehouseId);
        if (metrics == null) {
            metrics = REQUEST_METRICS_MAP.computeIfAbsent(warehouseId, id -> new WarehouseRequestMetrics(id, warehouseName));
        }
        metrics.requestTotal.increase(delta);
    }

    /** {@code starrocks_fe_warehouse_query_total}: finished queries (successful or failed) of the warehouse. */
    public static void increaseQuery(ConnectContext ctx, long delta) {
        WarehouseQueryMetrics metrics = getQueryMetrics(ctx);
        if (metrics != null) {
            metrics.queryTotal.increase(delta);
        }
    }

    /** {@code starrocks_fe_warehouse_query_err}: failed queries of the warehouse. */
    public static void increaseQueryErr(ConnectContext ctx, long delta) {
        WarehouseQueryMetrics metrics = getQueryMetrics(ctx);
        if (metrics != null) {
            metrics.queryErr.increase(delta);
        }
    }

    /** {@code starrocks_fe_warehouse_query_timeout}: queries of the warehouse that failed with a timeout. */
    public static void increaseQueryTimeout(ConnectContext ctx, long delta) {
        WarehouseQueryMetrics metrics = getQueryMetrics(ctx);
        if (metrics != null) {
            metrics.queryTimeout.increase(delta);
        }
    }

    /** {@code starrocks_fe_warehouse_query_analysis_err}: queries of the warehouse that failed during analysis. */
    public static void increaseQueryAnalysisErr(ConnectContext ctx, long delta) {
        WarehouseQueryMetrics metrics = getQueryMetrics(ctx);
        if (metrics != null) {
            metrics.queryAnalysisErr.increase(delta);
        }
    }

    /** {@code starrocks_fe_warehouse_query_internal_err}: queries of the warehouse that failed with an internal error. */
    public static void increaseQueryInternalErr(ConnectContext ctx, long delta) {
        WarehouseQueryMetrics metrics = getQueryMetrics(ctx);
        if (metrics != null) {
            metrics.queryInternalErr.increase(delta);
        }
    }

    /** {@code starrocks_fe_warehouse_slow_query}: successful queries of the warehouse slower than qe_slow_log_ms. */
    public static void increaseSlowQuery(ConnectContext ctx, long delta) {
        WarehouseQueryMetrics metrics = getQueryMetrics(ctx);
        if (metrics != null) {
            metrics.slowQuery.increase(delta);
        }
    }

    /**
     * Record the latency of a successful query. Feeds both {@code starrocks_fe_warehouse_query_latency_ms}
     * (a summary over all queries so far) and the sample buffer behind the {@code warehouse_query_latency}
     * quantile gauges of the current MetricCalculator interval.
     */
    public static void updateQueryLatency(ConnectContext ctx, long elapseMs) {
        WarehouseQueryMetrics metrics = getQueryMetrics(ctx);
        if (metrics != null) {
            metrics.recordLatency(elapseMs);
        }
    }

    /**
     * Called by {@link MetricCalculator} on every tick: turns the counter deltas of the interval into
     * {@code warehouse_qps} / {@code warehouse_rps} and the buffered latencies into the
     * {@code warehouse_query_latency} quantile gauges, using the same algorithm and window as the cluster-wide
     * {@code qps} / {@code rps} / {@code query_latency} metrics.
     */
    public static void updateCalculatedQueryMetrics(long intervalSeconds) {
        if (!Config.enable_per_warehouse_query_metrics) {
            // The flag is mutable: once it is turned off, stop exporting the existing series instead of
            // leaving them frozen in the registry. They are recreated on the next query if it is turned on again.
            if (!QUERY_METRICS_MAP.isEmpty() || !REQUEST_METRICS_MAP.isEmpty()) {
                removeQueryMetrics(key -> true);
                removeRequestMetrics(id -> true);
                LOG.info("Removed per-warehouse query metrics because enable_per_warehouse_query_metrics is off");
            }
            return;
        }
        long interval = Math.max(intervalSeconds, 1L);
        for (WarehouseRequestMetrics metrics : REQUEST_METRICS_MAP.values()) {
            try {
                metrics.calculate(interval);
            } catch (Exception e) {
                LOG.warn("failed to calculate request metrics of warehouse {}: {}", metrics.warehouseId, e.getMessage());
            }
        }
        for (WarehouseQueryMetrics metrics : QUERY_METRICS_MAP.values()) {
            try {
                metrics.calculate(interval);
            } catch (Exception e) {
                LOG.warn("failed to calculate query metrics of warehouse {}: {}", metrics.warehouseId, e.getMessage());
            }
        }
    }

    /**
     * Drop every metric of the warehouse once it is dropped, so that stale series do not linger until the FE
     * restarts and the metric cardinality stays bounded by the number of live warehouses.
     */
    public static void onDropWarehouse(long warehouseId) {
        removeQueryMetrics(key -> key.warehouseId == warehouseId);
        removeRequestMetrics(id -> id == warehouseId);
        List<Map<Long, ? extends Metric<?>>> legacyMaps = List.of(UNFINISHED_QUERY_COUNTER_MAP,
                UNFINISHED_BACKUP_JOB_COUNTER_MAP, UNFINISHED_RESTORE_JOB_COUNTER_MAP, LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP);
        for (Map<Long, ? extends Metric<?>> map : legacyMaps) {
            Metric<?> metric = map.remove(warehouseId);
            if (metric != null) {
                MetricRepo.removeMetric(metric);
            }
        }
        LOG.info("Removed metrics of dropped warehouse {}", warehouseId);
    }

    /**
     * Drop the metrics of the CN groups of a warehouse that no longer exist. Called after a CN group was dropped,
     * with the names of the CN groups that remain; series without a CN group ("") are kept.
     */
    public static void onDropCNGroup(long warehouseId, Set<String> remainingCNGroupNames) {
        removeQueryMetrics(key -> key.warehouseId == warehouseId && !key.cngroupName.isEmpty()
                && !remainingCNGroupNames.contains(key.cngroupName));
    }

    private static void removeRequestMetrics(Predicate<Long> filter) {
        for (Long warehouseId : List.copyOf(REQUEST_METRICS_MAP.keySet())) {
            if (filter.test(warehouseId)) {
                WarehouseRequestMetrics metrics = REQUEST_METRICS_MAP.remove(warehouseId);
                if (metrics != null) {
                    metrics.unregister();
                    LOG.info("Removed request metrics of warehouse {}", warehouseId);
                }
            }
        }
    }

    private static void removeQueryMetrics(Predicate<QueryMetricsKey> filter) {
        for (QueryMetricsKey key : List.copyOf(QUERY_METRICS_MAP.keySet())) {
            if (filter.test(key)) {
                WarehouseQueryMetrics metrics = QUERY_METRICS_MAP.remove(key);
                if (metrics != null) {
                    metrics.unregister();
                    LOG.info("Removed query metrics of warehouse {} cngroup '{}'", key.warehouseId, key.cngroupName);
                }
            }
        }
    }

    /**
     * Metrics of the warehouse that executed the current statement. A query-scope {@code SET_VAR(warehouse=...)}
     * hint is applied and reverted inside StmtExecutor, so by the time ConnectProcessor accounts the query the
     * session already points at its original warehouse again; the audit event builder, however, records the
     * execution warehouse, so that is what is used here.
     */
    private static WarehouseQueryMetrics getQueryMetrics(ConnectContext ctx) {
        if (!Config.enable_per_warehouse_query_metrics || ctx == null) {
            return null;
        }
        String warehouseName = ctx.getAuditEventBuilder() == null ? null : ctx.getAuditEventBuilder().getWarehouse();
        String cngroupName = ctx.getAuditEventBuilder() == null ? null : ctx.getAuditEventBuilder().getCNGroup();
        if (warehouseName == null || warehouseName.isEmpty()) {
            warehouseName = ctx.getCurrentWarehouseName();
            cngroupName = ctx.getCurrentComputeResourceName();
        }
        return getQueryMetrics(warehouseName, cngroupName);
    }

    private static WarehouseQueryMetrics getQueryMetrics(String warehouseName, String cngroupName) {
        Long warehouseId = resolveWarehouseId(warehouseName);
        if (warehouseId == null) {
            return null;
        }
        QueryMetricsKey key = new QueryMetricsKey(warehouseId, cngroupName == null ? "" : cngroupName);
        WarehouseQueryMetrics metrics = QUERY_METRICS_MAP.get(key);
        if (metrics == null) {
            metrics = QUERY_METRICS_MAP.computeIfAbsent(key,
                    k -> new WarehouseQueryMetrics(k.warehouseId, warehouseName, k.cngroupName));
        }
        return metrics;
    }

    static final class QueryMetricsKey {
        final long warehouseId;
        final String cngroupName;

        QueryMetricsKey(long warehouseId, String cngroupName) {
            this.warehouseId = warehouseId;
            this.cngroupName = cngroupName;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof QueryMetricsKey)) {
                return false;
            }
            QueryMetricsKey other = (QueryMetricsKey) o;
            return warehouseId == other.warehouseId && cngroupName.equals(other.cngroupName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(warehouseId, cngroupName);
        }
    }

    private static Long resolveWarehouseId(String warehouseName) {
        if (warehouseName == null || warehouseName.isEmpty()) {
            return null;
        }
        if (WarehouseManager.isDefaultWarehouse(warehouseName)) {
            return WarehouseManager.DEFAULT_WAREHOUSE_ID;
        }
        try {
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
            return warehouse == null ? null : warehouse.getId();
        } catch (Exception e) {
            LOG.debug("failed to resolve warehouse {} for query metrics: {}", warehouseName, e.getMessage());
            return null;
        }
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
     * Query metrics of one (warehouse, CN group) pair. Every metric is created (and thus exported, as 0) the first time the
     * warehouse handles a request, so ratios such as {@code err / total} never hit an absent series.
     */
    static final class WarehouseQueryMetrics {
        private final long warehouseId;
        private final String cngroupName;

        final LongCounterMetric queryTotal;
        final LongCounterMetric queryErr;
        final LongCounterMetric queryTimeout;
        final LongCounterMetric queryAnalysisErr;
        final LongCounterMetric queryInternalErr;
        final LongCounterMetric slowQuery;
        final GaugeMetricImpl<Double> qps;
        final HistogramMetric latencyHistogram;
        final List<GaugeMetricImpl<Double>> latencyGauges = new ArrayList<>(QUERY_LATENCY_TYPES.length);

        private long lastQueryTotal = 0;

        // latency samples of the current interval; guarded by `this`
        private List<Long> latencySamples = new ArrayList<>();
        private long latencyCount = 0;
        private double latencySum = 0;

        private WarehouseQueryMetrics(long warehouseId, String warehouseName, String cngroupName) {
            this.warehouseId = warehouseId;
            this.cngroupName = cngroupName;
            String id = String.valueOf(warehouseId);

            queryTotal = counter(WAREHOUSE_QUERY_TOTAL, "total number of queries of the warehouse", id, warehouseName);
            queryErr = counter(WAREHOUSE_QUERY_ERR, "total number of failed queries of the warehouse", id, warehouseName);
            queryTimeout = counter(WAREHOUSE_QUERY_TIMEOUT, "total number of timed-out queries of the warehouse", id,
                    warehouseName);
            queryAnalysisErr = counter(WAREHOUSE_QUERY_ANALYSIS_ERR,
                    "total number of queries of the warehouse that failed during analysis", id, warehouseName);
            queryInternalErr = counter(WAREHOUSE_QUERY_INTERNAL_ERR,
                    "total number of queries of the warehouse that failed with an internal error", id, warehouseName);
            slowQuery = counter(WAREHOUSE_SLOW_QUERY,
                    "total number of successful queries of the warehouse slower than qe_slow_log_ms", id, warehouseName);
            qps = gauge(WAREHOUSE_QPS, Metric.MetricUnit.NOUNIT, "queries per second of the warehouse", null, id,
                    warehouseName);
            for (String type : QUERY_LATENCY_TYPES) {
                latencyGauges.add(gauge(WAREHOUSE_QUERY_LATENCY, Metric.MetricUnit.MILLISECONDS,
                        type + " of query latency of the warehouse", type, id, warehouseName));
            }

            latencyHistogram = new HistogramMetric(WAREHOUSE_QUERY_LATENCY_MS);
            latencyHistogram.addLabel(new MetricLabel("warehouse_id", id));
            latencyHistogram.addLabel(new MetricLabel("warehouse_name", warehouseName));
            latencyHistogram.addLabel(new MetricLabel("cngroup_name", cngroupName));
            MetricRepo.addHistogram(histogramRegistryName(), latencyHistogram);

            LOG.info("Add per-warehouse query metrics, warehouse is {} ({}), cngroup '{}'", warehouseId, warehouseName,
                    cngroupName);
        }

        private String histogramRegistryName() {
            return MetricRegistry.name("warehouse_query", "latency", "ms", String.valueOf(warehouseId), cngroupName);
        }

        private LongCounterMetric counter(String name, String desc, String id, String warehouseName) {
            LongCounterMetric metric = new LongCounterMetric(name, Metric.MetricUnit.REQUESTS, desc);
            addLabels(metric, id, warehouseName);
            MetricRepo.addMetric(metric);
            return metric;
        }

        private void addLabels(Metric<?> metric, String id, String warehouseName) {
            metric.addLabel(new MetricLabel("warehouse_id", id));
            metric.addLabel(new MetricLabel("warehouse_name", warehouseName));
            metric.addLabel(new MetricLabel("cngroup_name", cngroupName));
        }

        private GaugeMetricImpl<Double> gauge(String name, Metric.MetricUnit unit, String desc, String type,
                                             String id, String warehouseName) {
            GaugeMetricImpl<Double> metric = new GaugeMetricImpl<>(name, unit, desc);
            if (type != null) {
                metric.addLabel(new MetricLabel("type", type));
            }
            addLabels(metric, id, warehouseName);
            metric.setValue(0.0);
            MetricRepo.addMetric(metric);
            return metric;
        }

        private void unregister() {
            MetricRepo.removeMetric(queryTotal);
            MetricRepo.removeMetric(queryErr);
            MetricRepo.removeMetric(queryTimeout);
            MetricRepo.removeMetric(queryAnalysisErr);
            MetricRepo.removeMetric(queryInternalErr);
            MetricRepo.removeMetric(slowQuery);
            MetricRepo.removeMetric(qps);
            for (GaugeMetricImpl<Double> g : latencyGauges) {
                MetricRepo.removeMetric(g);
            }
            MetricRepo.removeHistogram(histogramRegistryName());
        }

        private void recordLatency(long elapseMs) {
            latencyHistogram.update(elapseMs);
            synchronized (this) {
                latencyCount++;
                latencySum += elapseMs;
                if (latencySamples.size() < MAX_LATENCY_SAMPLES_PER_INTERVAL) {
                    latencySamples.add(elapseMs);
                }
            }
        }

        private void calculate(long intervalSeconds) {
            long currentQueryTotal = queryTotal.getValue();
            double q = (double) (currentQueryTotal - lastQueryTotal) / intervalSeconds;
            qps.setValue(q < 0 ? 0.0 : q);
            lastQueryTotal = currentQueryTotal;

            List<Long> samples;
            long count;
            double sum;
            synchronized (this) {
                samples = latencySamples;
                count = latencyCount;
                sum = latencySum;
                latencySamples = new ArrayList<>();
                latencyCount = 0;
                latencySum = 0;
            }
            if (samples.isEmpty()) {
                for (GaugeMetricImpl<Double> g : latencyGauges) {
                    g.setValue(0.0);
                }
                return;
            }
            Collections.sort(samples);
            latencyGauges.get(0).setValue(sum / count);
            for (int i = 0; i < QUERY_LATENCY_QUANTILES.length; i++) {
                int index = (int) Math.round((samples.size() - 1) * QUERY_LATENCY_QUANTILES[i]);
                latencyGauges.get(i + 1).setValue((double) samples.get(index));
            }
        }
    }

    /** Request count and RPS of one warehouse; created on the warehouse's first request, removed when it is dropped. */
    static final class WarehouseRequestMetrics {
        private final long warehouseId;
        final LongCounterMetric requestTotal;
        final GaugeMetricImpl<Double> rps;
        private long lastRequestTotal = 0;

        private WarehouseRequestMetrics(long warehouseId, String warehouseName) {
            this.warehouseId = warehouseId;
            String id = String.valueOf(warehouseId);
            requestTotal = new LongCounterMetric(WAREHOUSE_REQUEST_TOTAL, Metric.MetricUnit.REQUESTS,
                    "total number of requests of the warehouse");
            requestTotal.addLabel(new MetricLabel("warehouse_id", id));
            requestTotal.addLabel(new MetricLabel("warehouse_name", warehouseName));
            MetricRepo.addMetric(requestTotal);
            rps = new GaugeMetricImpl<>(WAREHOUSE_RPS, Metric.MetricUnit.NOUNIT, "requests per second of the warehouse");
            rps.addLabel(new MetricLabel("warehouse_id", id));
            rps.addLabel(new MetricLabel("warehouse_name", warehouseName));
            rps.setValue(0.0);
            MetricRepo.addMetric(rps);
            LOG.info("Add per-warehouse request metrics, warehouse is {} ({})", warehouseId, warehouseName);
        }

        private void calculate(long intervalSeconds) {
            long current = requestTotal.getValue();
            double r = (double) (current - lastRequestTotal) / intervalSeconds;
            rps.setValue(r < 0 ? 0.0 : r);
            lastRequestTotal = current;
        }

        private void unregister() {
            MetricRepo.removeMetric(requestTotal);
            MetricRepo.removeMetric(rps);
        }
    }
}
