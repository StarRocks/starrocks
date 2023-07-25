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

    private WarehouseMetricMgr() {}

    private static final ConcurrentMap<String, LongCounterMetric> UNFINISHED_QUERY_COUNTER_MAP =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, LongCounterMetric> UNFINISHED_BACKUP_JOB_COUNTER_MAP =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, LongCounterMetric> UNFINISHED_RESTORE_JOB_COUNTER_MAP =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, GaugeMetricImpl<Long>> LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP =
            new ConcurrentHashMap<>();

    public static Map<String, Long> getUnfinishedQueries() {
        return metricMapToValueMap(UNFINISHED_QUERY_COUNTER_MAP);
    }

    public static Map<String, Long> getUnfinishedBackupJobs() {
        return metricMapToValueMap(UNFINISHED_BACKUP_JOB_COUNTER_MAP);
    }

    public static Map<String, Long> getUnfinishedRestoreJobs() {
        return metricMapToValueMap(UNFINISHED_RESTORE_JOB_COUNTER_MAP);
    }

    public static Map<String, Long> getLastFinishedJobTimestampMs() {
        return metricMapToValueMap(LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP);
    }

    public static void increaseUnfinishedQueries(String warehouse, Long delta) {
        if (delta < 0) {
            setLastFinishedJobTimestamp(warehouse, System.currentTimeMillis());
        }

        LongCounterMetric metric = getOrCreateMetric(
                warehouse,
                UNFINISHED_QUERY_COUNTER_MAP,
                UNFINISHED_QUERY,
                () -> new LongCounterMetric(UNFINISHED_QUERY, Metric.MetricUnit.REQUESTS,
                        "current unfinished queries of the warehouse")
        );
        metric.increase(delta);
    }

    public static void increaseUnfinishedBackupJobs(String warehouse, Long delta) {
        if (delta < 0) {
            setLastFinishedJobTimestamp(warehouse, System.currentTimeMillis());
        }

        LongCounterMetric metric = getOrCreateMetric(
                warehouse,
                UNFINISHED_BACKUP_JOB_COUNTER_MAP,
                UNFINISHED_BACKUP_JOB,
                () -> new LongCounterMetric(UNFINISHED_BACKUP_JOB, Metric.MetricUnit.REQUESTS,
                        "current unfinished backup jobs of the warehouse")
        );
        metric.increase(delta);
    }

    public static void increaseUnfinishedRestoreJobs(String warehouse, Long delta) {
        if (delta < 0) {
            setLastFinishedJobTimestamp(warehouse, System.currentTimeMillis());
        }

        LongCounterMetric metric = getOrCreateMetric(
                warehouse,
                UNFINISHED_RESTORE_JOB_COUNTER_MAP,
                UNFINISHED_RESTORE_JOB,
                () -> new LongCounterMetric(UNFINISHED_RESTORE_JOB, Metric.MetricUnit.REQUESTS,
                        "current unfinished queries of the warehouse")
        );
        metric.increase(delta);
    }

    private static void setLastFinishedJobTimestamp(String warehouse, Long timestamp) {
        GaugeMetricImpl<Long> metric = getOrCreateMetric(
                warehouse,
                LAST_FINISHED_JOB_TIMESTAMP_COUNTER_MAP,
                LAST_FINISHED_JOB_TIMESTAMP,
                () -> new GaugeMetricImpl<>(LAST_FINISHED_JOB_TIMESTAMP, Metric.MetricUnit.MICROSECONDS,
                        "the timestamp of last finished job")
        );
        metric.setValue(timestamp);
    }

    private static <T extends Metric<Long>> T getOrCreateMetric(String warehouse, Map<String, T> metricMap,
                                                                String metricName, Supplier<T> metricSupplier) {
        if (!metricMap.containsKey(warehouse)) {
            synchronized (WarehouseMetricMgr.class) {
                if (!metricMap.containsKey(warehouse)) {
                    T metric = metricSupplier.get();
                    metric.addLabel(new MetricLabel("warehouse", warehouse));

                    metricMap.put(warehouse, metric);
                    MetricRepo.addMetric(metric);

                    LOG.info("Add {} metric, warehouse is {}", metricName, warehouse);
                }
            }
        }
        return metricMap.get(warehouse);
    }

    private static <T extends Metric<Long>> Map<String, Long> metricMapToValueMap(Map<String, T> metricMap) {
        return metricMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
    }
}
