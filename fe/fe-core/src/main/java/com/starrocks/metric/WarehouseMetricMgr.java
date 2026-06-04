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

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final String PENDING_SUM_RAW_SLOTS = "query_queue_pending_sum_raw_slots";
    private static final String PRE_SCALE_WAIT_TOTAL = "query_queue_pre_scale_wait_total";
    private static final String PRE_SCALE_WAIT_MS = "query_queue_pre_scale_wait_ms";

    private WarehouseMetricMgr() {
    }

    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_QUERY_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_BACKUP_JOB_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_RESTORE_JOB_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, GaugeMetricImpl<Long>> LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, GaugeMetricImpl<Long>> PENDING_SUM_RAW_SLOTS_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, LongCounterMetric> PRE_SCALE_WAIT_TOTAL_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, HistogramMetric> PRE_SCALE_WAIT_MS_MAP = new ConcurrentHashMap<>();

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

    public static GaugeMetricImpl<Long> getPendingSumRawSlotsGauge(Long warehouseId) {
        return getOrCreateMetric(
                warehouseId,
                PENDING_SUM_RAW_SLOTS_MAP,
                PENDING_SUM_RAW_SLOTS,
                () -> new GaugeMetricImpl<>(PENDING_SUM_RAW_SLOTS, Metric.MetricUnit.NOUNIT,
                        "sum of raw (un-clamped) estimated slots across pending queries per warehouse", 0L));
    }

    public static LongCounterMetric getPreScaleWaitCounter(Long warehouseId) {
        return getOrCreateMetric(
                warehouseId,
                PRE_SCALE_WAIT_TOTAL_MAP,
                PRE_SCALE_WAIT_TOTAL,
                () -> new LongCounterMetric(PRE_SCALE_WAIT_TOTAL, Metric.MetricUnit.REQUESTS,
                        "cumulative count of pre-scale wait gate firings per warehouse"));
    }

    public static HistogramMetric getPreScaleWaitHistogram(Long warehouseId) {
        HistogramMetric existing = PRE_SCALE_WAIT_MS_MAP.get(warehouseId);
        if (existing != null) {
            return existing;
        }
        synchronized (WarehouseMetricMgr.class) {
            existing = PRE_SCALE_WAIT_MS_MAP.get(warehouseId);
            if (existing != null) {
                return existing;
            }
            String whName = resolveWarehouseName(warehouseId);
            HistogramMetric h = new HistogramMetric(PRE_SCALE_WAIT_MS);
            h.addLabel(new MetricLabel("warehouse_id", warehouseId.toString()));
            h.addLabel(new MetricLabel("warehouse_name", whName));
            PRE_SCALE_WAIT_MS_MAP.put(warehouseId, h);
            // HistogramMetric extends com.codahale.metrics.Histogram (not Metric<?>), so it is
            // surfaced via MetricVisitor#visitHistogram rather than MetricRepo.addMetric.
            LOG.info("Add {} metric, warehouse is {}", PRE_SCALE_WAIT_MS, warehouseId);
            return h;
        }
    }

    private static <T extends Metric<Long>> T getOrCreateMetric(Long warehouseId, Map<Long, T> metricMap,
                                                                String metricName, Supplier<T> metricSupplier) {
        if (!metricMap.containsKey(warehouseId)) {
            synchronized (WarehouseMetricMgr.class) {
                if (!metricMap.containsKey(warehouseId)) {
                    String whName = resolveWarehouseName(warehouseId);

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

    private static String resolveWarehouseName(Long warehouseId) {
        try {
            Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
            return wh == null ? "unknown" : wh.getName();
        } catch (Exception e) {
            // getWarehouse(long) can throw ErrorReportException when the warehouse registry isn't ready
            // (typical in unit tests and right after FE startup). Fall back so metric registration never blocks.
            return "unknown";
        }
    }

    private static <T extends Metric<Long>> Map<Long, Long> metricMapToValueMap(Map<Long, T> metricMap) {
        return metricMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
    }
}
