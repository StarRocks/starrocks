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

    private WarehouseMetricMgr() {
    }

    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_QUERY_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_BACKUP_JOB_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, LongCounterMetric> UNFINISHED_RESTORE_JOB_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, GaugeMetricImpl<Long>> LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP =
            new ConcurrentHashMap<>();

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
}
