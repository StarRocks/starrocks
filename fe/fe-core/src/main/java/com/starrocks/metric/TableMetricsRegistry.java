// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.metric;

import com.google.common.collect.Maps;
import com.starrocks.common.ThreadPoolManager;

import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class TableMetricsRegistry {

    private final Map<Long, TableMetricsEntity> idToTableMetrics;
    private final ScheduledThreadPoolExecutor timer;
    private static final TableMetricsRegistry INSTANCE = new TableMetricsRegistry();

    private TableMetricsRegistry() {
        idToTableMetrics = Maps.newConcurrentMap();
        // clear all metrics everyday
        timer = ThreadPoolManager.newDaemonScheduledThreadPool(1, "Table-Metrics-Cleaner", true);
        timer.scheduleAtFixedRate(new MetricsCleaner(), 0, 1L, TimeUnit.DAYS);
    }

    public static TableMetricsRegistry getInstance() {
        return INSTANCE;
    }

    public TableMetricsEntity getMetricsEntity(long tableId) {
        return idToTableMetrics.computeIfAbsent(tableId, k -> new TableMetricsEntity());
    }

    private class MetricsCleaner extends TimerTask {
        @Override
        public void run() {
            synchronized (TableMetricsRegistry.this) {
                idToTableMetrics.clear();
            }
        }
    }
}

