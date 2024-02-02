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
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MaterializedViewMetricsRegistry {

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final Map<MvId, MaterializedViewMetricsEntity> idToMVMetrics;
    private final ScheduledThreadPoolExecutor timer;
    private static final MaterializedViewMetricsRegistry INSTANCE = new MaterializedViewMetricsRegistry();

    private MaterializedViewMetricsRegistry() {
        idToMVMetrics = Maps.newHashMap();
        // clear all metrics everyday
        timer = ThreadPoolManager.newDaemonScheduledThreadPool(1, "MaterializedView-Metrics-Cleaner", true);
        timer.scheduleAtFixedRate(new MaterializedViewMetricsRegistry.MetricsCleaner(), 0, 1L, TimeUnit.DAYS);
    }

    public static MaterializedViewMetricsRegistry getInstance() {
        return INSTANCE;
    }

    public synchronized MaterializedViewMetricsEntity getMetricsEntity(MvId mvId) {
        return idToMVMetrics.computeIfAbsent(mvId, k -> new MaterializedViewMetricsEntity(metricRegistry, mvId));
    }

    private class MetricsCleaner extends TimerTask {
        @Override
        public void run() {
            synchronized (MaterializedViewMetricsRegistry.this) {
                idToMVMetrics.clear();
            }
        }
    }

    // collect materialized-view-level metrics
    public static void collectMaterializedViewMetrics(MetricVisitor visitor, boolean minifyMetrics) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<String> dbNames = globalStateMgr.getLocalMetastore().listDbNames();
        for (String dbName : dbNames) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (null == db) {
                continue;
            }
            for (MaterializedView mv : db.getMaterializedViews()) {
                MaterializedViewMetricsEntity mvEntity =
                        MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());

                for (Metric m : mvEntity.getMetrics()) {
                    // minify metrics if needed
                    if (minifyMetrics) {
                        if (null == m.getValue()) {
                            continue;
                        }
                        if (Metric.MetricType.COUNTER == m.type && ((Long) m.getValue()).longValue() == 0L) {
                            continue;
                        }
                    }
                    m.addLabel(new MetricLabel("db_name", dbName))
                            .addLabel(new MetricLabel("mv_name", mv.getName()))
                            .addLabel(new MetricLabel("mv_id", String.valueOf(mv.getId())));
                    visitor.visit(m);
                }
            }
        }

        // Histogram metrics should only output once
        for (Map.Entry<String, Histogram> e : MaterializedViewMetricsRegistry.getInstance()
                .metricRegistry.getHistograms().entrySet()) {
            if (minifyMetrics) {
                if (e.getValue().getCount() == 0) {
                    continue;
                }
            }
            visitor.visitHistogram(e.getKey(), e.getValue());
        }
    }
}
