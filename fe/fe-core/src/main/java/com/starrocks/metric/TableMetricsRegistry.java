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

import com.google.common.collect.Maps;
import com.starrocks.common.concurrent.ThreadPoolManager;

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

