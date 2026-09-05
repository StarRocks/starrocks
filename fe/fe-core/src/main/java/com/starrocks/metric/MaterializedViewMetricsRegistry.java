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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MaterializedViewMetricsRegistry {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewMetricsRegistry.class);

    private final MetricRegistry metricRegistry = new MetricRegistry();
    // Global refresh-duration histograms live in their own registry so they scrape unconditionally, instead of
    // sharing metricRegistry whose only scrape path (collectMaterializedViewMetrics) sits behind the per-MV gate.
    private final MetricRegistry globalHistogramRegistry = new MetricRegistry();
    private final Map<MvId, IMaterializedViewMetricsEntity> idToMVMetrics;
    private final ScheduledThreadPoolExecutor timer;
    private static final MaterializedViewMetricsRegistry INSTANCE = new MaterializedViewMetricsRegistry();
    private static final IMaterializedViewMetricsEntity BLACK_HOLE_ENTITY = new MaterializedViewMetricsBlackHoleEntity();

    // mv_global_query_mv_usage_total is keyed by (usage_type, refresh_mode); MetricWithLabelGroup only
    // supports a single label, so the two-dimensional counters are tracked here.
    private static final Map<String, LongCounterMetric> MV_USAGE_COUNTERS = Maps.newConcurrentMap();

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

    public synchronized void remove(MvId mvId) {
        LOG.info("Removing materialized view metrics for mvId: {}", mvId);
        idToMVMetrics.remove(mvId);
    }
    private IMaterializedViewMetricsEntity initMaterializedViewMetricsEntity(MvId mvId) {
        if (!Config.enable_materialized_view_metrics_collect) {
            return BLACK_HOLE_ENTITY;
        } else {
            return new MaterializedViewMetricsEntity(metricRegistry, mvId);
        }
    }

    public synchronized void registerMetricsEntity(MvId mvId) {
        idToMVMetrics.put(mvId, initMaterializedViewMetricsEntity(mvId));
    }

    public synchronized IMaterializedViewMetricsEntity getMetricsEntity(MvId mvId) {
        return idToMVMetrics.computeIfAbsent(mvId, k -> initMaterializedViewMetricsEntity(k));
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
            LOG.warn("Invalid materialized view metrics entity, mvId: {}", mvId);
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
                    .addLabel(new MetricLabel("mv_id", String.valueOf(mvId.getId())))
                    .addLabel(new MetricLabel("warehouse_name", entity.warehouseNameOpt.orElse("")));
            visitor.visit(m);
        }
    }

    // collect materialized-view-level metrics
    public static void collectMaterializedViewMetrics(MetricVisitor visitor, boolean minifyMetrics) {
        MaterializedViewMetricsRegistry instance = MaterializedViewMetricsRegistry.getInstance();
        for (Map.Entry<MvId, IMaterializedViewMetricsEntity> entry : instance.idToMVMetrics.entrySet()) {
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

        // Histogram metrics should only output once
        if (!minifyMetrics) {
            for (Map.Entry<String, Histogram> e : MaterializedViewMetricsRegistry.getInstance()
                    .metricRegistry.getHistograms().entrySet()) {
                visitor.visitHistogram(e.getKey(), e.getValue());
            }
        }
    }

    // Enumerate all async materialized views at scrape time, bucketed by refresh mode x active status.
    // Lock-free like MetricRepo.collectTableMetrics (reads only immutable-ish fields); db.getMaterializedViews()
    // covers async MVs only (not sync/rollup MVs).
    public static void collectGlobalMvCount(MetricVisitor visitor) {
        Map<String, Long> countByModeStatus = Maps.newHashMap();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        for (String dbName : globalStateMgr.getLocalMetastore().listDbNames(new ConnectContext())) {
            Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
            if (db == null) {
                continue;
            }
            for (MaterializedView mv : db.getMaterializedViews()) {
                String mode = mv.getRefreshMode() == null ? "UNKNOWN" : mv.getRefreshMode().name();
                String status = mv.isActive() ? "ACTIVE" : "INACTIVE";
                countByModeStatus.merge(mode + "|" + status, 1L, Long::sum);
            }
        }
        for (Map.Entry<String, Long> entry : countByModeStatus.entrySet()) {
            String[] parts = entry.getKey().split("\\|", 2);
            GaugeMetricImpl<Long> gauge = new GaugeMetricImpl<>("mv_global_count", Metric.MetricUnit.NOUNIT,
                    "current number of materialized views by refresh mode and active status");
            gauge.addLabel(new MetricLabel("refresh_mode", parts[0]));
            gauge.addLabel(new MetricLabel("status", parts[1]));
            gauge.setValue(entry.getValue());
            visitor.visit(gauge);
        }
    }

    // Fleet-level aggregate of the per-MV refresh counters, bumped at the same job-terminal hook so it equals
    // the per-MV sum. MERGED is a scheduling artifact (no real job), so it is excluded — matching
    // information_schema.materialized_view_refresh_jobs.
    public static void increaseGlobalRefreshJobStatus(Constants.TaskRunState status, String warehouse) {
        if (status == null || !status.isFinishState() || status == Constants.TaskRunState.MERGED) {
            return;
        }
        String wh = Strings.isNullOrEmpty(warehouse) ? "" : warehouse;
        MetricRepo.COUNTER_MV_GLOBAL_REFRESH_JOBS.getMetric(wh).increase(1L);
        if (status == Constants.TaskRunState.SUCCESS) {
            MetricRepo.COUNTER_MV_GLOBAL_REFRESH_SUCCESS_JOBS.getMetric(wh).increase(1L);
        } else if (status == Constants.TaskRunState.FAILED) {
            MetricRepo.COUNTER_MV_GLOBAL_REFRESH_FAILED_JOBS.getMetric(wh).increase(1L);
        }
    }

    public static Histogram getGlobalDurationHistogram(String warehouse) {
        String wh = Strings.isNullOrEmpty(warehouse) ? "" : warehouse;
        HistogramMetric h = new HistogramMetric("mv_global_refresh_duration");
        h.addLabel(new MetricLabel("warehouse_name", wh));
        return getInstance().globalHistogramRegistry.histogram(h.getHistogramName(), () -> h);
    }

    public static void updateGlobalRefreshDuration(long wallMs, String warehouse) {
        getGlobalDurationHistogram(warehouse).update(wallMs);
    }

    public static void collectGlobalDurationHistograms(MetricVisitor visitor) {
        for (Map.Entry<String, Histogram> e : getInstance().globalHistogramRegistry.getHistograms().entrySet()) {
            visitor.visitHistogram(e.getKey(), e.getValue());
        }
    }

    // pending/running are scrape-time gauges aggregated by warehouse. Enumerate all async MVs (authoritative,
    // like collectGlobalMvCount) rather than the idToMVMetrics cache that MetricsCleaner wipes daily.
    public static void collectGlobalGauges(MetricVisitor visitor) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        TaskManager taskManager = globalStateMgr.getTaskManager();
        if (taskManager == null) {
            return;
        }
        TaskRunScheduler scheduler = taskManager.getTaskRunScheduler();
        Map<String, Long> pendingByWarehouse = Maps.newHashMap();
        Map<String, Long> runningByWarehouse = Maps.newHashMap();
        for (String dbName : globalStateMgr.getLocalMetastore().listDbNames(new ConnectContext())) {
            Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
            if (db == null) {
                continue;
            }
            for (MaterializedView mv : db.getMaterializedViews()) {
                Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
                if (task == null) {
                    continue;
                }
                // Key the running gauge by the run's status warehouse (where it actually executes, immune to a
                // mid-refresh ALTER) and the pending gauge by the MV's current warehouse (a queued run re-resolves
                // it at start). Seed both maps per warehouse so a quiet one reports 0 instead of a missing series.
                String mvWarehouse = mv.getWarehouseName() == null ? "" : mv.getWarehouseName();
                runningByWarehouse.putIfAbsent(mvWarehouse, 0L);
                pendingByWarehouse.merge(mvWarehouse, scheduler.getTaskIdPendingTaskRunCount(task.getId()), Long::sum);
                TaskRun running = scheduler.getRunningTaskRun(task.getId());
                if (running != null) {
                    TaskRunStatus status = running.getStatus();
                    String runWarehouse = status == null ? mvWarehouse : status.getWarehouseName();
                    pendingByWarehouse.putIfAbsent(runWarehouse, 0L);
                    runningByWarehouse.merge(runWarehouse, 1L, Long::sum);
                }
            }
        }
        emitGauge(visitor, "mv_global_refresh_pending_jobs",
                "current pending materialized view refresh jobs by warehouse", pendingByWarehouse);
        emitGauge(visitor, "mv_global_refresh_running_jobs",
                "current running materialized view refresh jobs by warehouse", runningByWarehouse);
    }

    private static void emitGauge(MetricVisitor visitor, String name, String description,
                                  Map<String, Long> byWarehouse) {
        for (Map.Entry<String, Long> entry : byWarehouse.entrySet()) {
            GaugeMetricImpl<Long> gauge = new GaugeMetricImpl<>(name, Metric.MetricUnit.NOUNIT, description);
            gauge.addLabel(new MetricLabel("warehouse_name", entry.getKey()));
            gauge.setValue(entry.getValue());
            visitor.visit(gauge);
        }
    }

    public static void increaseMvUsage(String usageType, String refreshMode) {
        String mode = refreshMode == null ? "UNKNOWN" : refreshMode;
        MV_USAGE_COUNTERS.computeIfAbsent(usageType + "|" + mode, key -> {
            LongCounterMetric metric = new LongCounterMetric("mv_global_query_mv_usage_total",
                    Metric.MetricUnit.REQUESTS, "materialized view usage by access type and refresh mode");
            metric.addLabel(new MetricLabel("usage_type", usageType));
            metric.addLabel(new MetricLabel("refresh_mode", mode));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(1L);
    }

    @VisibleForTesting
    public static long getMvUsageCount(String usageType, String refreshMode) {
        LongCounterMetric metric = MV_USAGE_COUNTERS.get(usageType + "|" + refreshMode);
        return metric == null ? 0L : metric.getValue();
    }

    public static void increaseGlobalQueryRewrite(String state) {
        MetricRepo.COUNTER_MV_GLOBAL_QUERY_REWRITE.getMetric(state).increase(1L);
    }
}
