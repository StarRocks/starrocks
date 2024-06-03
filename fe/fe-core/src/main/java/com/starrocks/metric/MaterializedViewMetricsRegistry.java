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
import com.starrocks.catalog.MvId;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.DebugUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MaterializedViewMetricsRegistry {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewMetricsRegistry.class);

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final Map<MvId, MaterializedViewMetricsEntity> idToMVMetrics;
    private final ScheduledThreadPoolExecutor timer;
    private static final MaterializedViewMetricsRegistry INSTANCE = new MaterializedViewMetricsRegistry();

    private MaterializedViewMetricsRegistry() {
        idToMVMetrics = Maps.newHashMap();
        // clear all metrics everyday
        timer = ThreadPoolManager.newDaemonScheduledThreadPool(1, "MaterializedView-Metrics-Cleaner", true);
        // add initial delay to avoid all metrics are cleared at the same time
        timer.scheduleAtFixedRate(new MaterializedViewMetricsRegistry.MetricsCleaner(), 1L, 1L, TimeUnit.DAYS);
    }

    public static MaterializedViewMetricsRegistry getInstance() {
        return INSTANCE;
    }

    public synchronized IMaterializedViewMetricsEntity getMetricsEntity(MvId mvId) {
        if (!Config.enable_materialized_view_metrics_collect) {
            return new MaterializedViewMetricsBlackHoleEntity();
        }
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

    private static void doCollectMetrics(MvId mvId, MaterializedViewMetricsEntity entity,
                                       MetricVisitor visitor, boolean minifyMetrics) {
        if (!entity.initDbAndTableName()) {
            LOG.debug("Invalid materialized view metrics entity, mvId: {}", mvId);
            return;
        }

        for (Metric m : entity.getMetrics()) {
            // minify metrics if needed
            if (minifyMetrics) {
                if (null == m.getValue()) {
                    continue;
                }
                // ignore gauge metrics since it will try db lock and visit more metadata
                if (Metric.MetricType.GAUGE == m.type) {
                    continue;
                }
                // ignore counter metrics with 0 value
                if (Metric.MetricType.COUNTER == m.type && ((Long) m.getValue()).longValue() == 0L) {
                    continue;
                }
            }
            m.addLabel(new MetricLabel("db_name", entity.dbNameOpt.get()))
                    .addLabel(new MetricLabel("mv_name", entity.mvNameOpt.get()))
                    .addLabel(new MetricLabel("mv_id", String.valueOf(mvId.getId())));
            visitor.visit(m);
        }

        // Histogram metrics should only output once
        if (!minifyMetrics) {
            for (Map.Entry<String, Histogram> e : MaterializedViewMetricsRegistry.getInstance()
                    .metricRegistry.getHistograms().entrySet()) {
                visitor.visitHistogram(e.getKey(), e.getValue());
            }
        }
    }

    // collect materialized-view-level metrics
    public static void collectMaterializedViewMetrics(MetricVisitor visitor, boolean minifyMetrics) {
        MaterializedViewMetricsRegistry instance = MaterializedViewMetricsRegistry.getInstance();
        for (Map.Entry<MvId, MaterializedViewMetricsEntity> entry : instance.idToMVMetrics.entrySet()) {
            IMaterializedViewMetricsEntity mvEntity = entry.getValue();
            if (mvEntity == null || mvEntity instanceof MaterializedViewMetricsBlackHoleEntity) {
                continue;
            }
            try {
                MvId mvId = entry.getKey();
                MaterializedViewMetricsEntity entity = (MaterializedViewMetricsEntity) mvEntity;
                doCollectMetrics(mvId, entity, visitor, minifyMetrics);
            } catch (Exception e) {
                LOG.warn("Failed to collect materialized view metrics for mvId: {}", entry.getKey(),
                        DebugUtil.getStackTrace(e));
            }
        }
    }
}
