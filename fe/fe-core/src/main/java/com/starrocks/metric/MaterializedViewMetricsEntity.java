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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.PartitionBasedMvRefreshProcessor;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public final class MaterializedViewMetricsEntity {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewMetricsEntity.class);


    private final MvId mvId;
    private final MetricRegistry metricRegistry;
    private final List<Metric> metrics = Lists.newArrayList();

    // refresh
    public LongCounterMetric counterRefreshJobTotal;
    public LongCounterMetric counterRefreshJobSuccessTotal;
    public LongCounterMetric counterRefreshJobFailedTotal;
    public LongCounterMetric counterRefreshJobEmptyTotal;
    public LongCounterMetric counterRefreshJobRetryCheckChangedTotal;

    // query
    // only record when the materialized view is not contained before rewriting
    public LongCounterMetric counterQueryHitTotal;
    public LongCounterMetric counterQueryConsideredTotal;
    public LongCounterMetric counterQueryMatchedTotal;
    // record once the materialized view is used in the final plan
    public LongCounterMetric counterQueryMaterializedViewTotal;

    // gauge
    public GaugeMetric<Long> counterRefreshPendingJobs;
    public GaugeMetric<Long> counterRefreshRunningJobs;
    public GaugeMetric<Long> counterRowNums;
    public GaugeMetric<Long> counterStorageSize;

    // histogram
    public Histogram histRefreshJobDuration;

    public MaterializedViewMetricsEntity(MetricRegistry metricRegistry, MvId mvId) {
        this.metricRegistry = metricRegistry;
        this.mvId = mvId;

        initMaterializedViewMetrics();
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    protected void initMaterializedViewMetrics() {
        // refresh metrics
        counterRefreshJobTotal = new LongCounterMetric("mv_refresh_jobs", MetricUnit.REQUESTS,
                "total materialized view's refresh jobs");
        metrics.add(counterRefreshJobTotal);
        counterRefreshJobSuccessTotal = new LongCounterMetric("mv_refresh_total_success_jobs", MetricUnit.REQUESTS,
                "total materialized view's refresh success jobs");
        metrics.add(counterRefreshJobSuccessTotal);
        counterRefreshJobFailedTotal = new LongCounterMetric("mv_refresh_total_failed_jobs", MetricUnit.REQUESTS,
                "total materialized view's refresh failed jobs");
        metrics.add(counterRefreshJobFailedTotal);
        counterRefreshJobEmptyTotal = new LongCounterMetric("mv_refresh_total_empty_jobs", MetricUnit.REQUESTS,
                "total materialized view's refresh empty jobs");
        metrics.add(counterRefreshJobEmptyTotal);
        counterRefreshJobRetryCheckChangedTotal = new LongCounterMetric("mv_refresh_total_retry_meta_count", MetricUnit.REQUESTS,
                "total materialized view's retry to check table change count");
        metrics.add(counterRefreshJobRetryCheckChangedTotal);

        // query metrics
        counterQueryMaterializedViewTotal = new LongCounterMetric("mv_query_total_count", MetricUnit.REQUESTS,
                "total materialized view's query count");
        metrics.add(counterQueryMaterializedViewTotal);
        counterQueryHitTotal = new LongCounterMetric("mv_query_total_hit_count", MetricUnit.REQUESTS,
                "total hit materialized view's query count");
        metrics.add(counterQueryHitTotal);
        counterQueryConsideredTotal = new LongCounterMetric("mv_query_total_considered_count", MetricUnit.REQUESTS,
                "total considered materialized view's query count");
        metrics.add(counterQueryConsideredTotal);
        counterQueryMatchedTotal = new LongCounterMetric("mv_query_total_matched_count", MetricUnit.REQUESTS,
                "total matched materialized view's query count");
        metrics.add(counterQueryMatchedTotal);

        // histogram metrics
        try {
            Database db = GlobalStateMgr.getCurrentState().getDb(mvId.getDbId());
            MaterializedView mv = (MaterializedView) db.getTable(mvId.getId());
            histRefreshJobDuration = metricRegistry.histogram(MetricRegistry.name("mv_refresh_duration",
                    db.getFullName(), mv.getName()));
        } catch (Exception e) {
            LOG.warn("Ignore histogram metrics for materialized view: {}", mvId);
        }

        // gauge metrics
        counterRefreshPendingJobs = new GaugeMetric<Long>("mv_refresh_pending_jobs", MetricUnit.NOUNIT,
                "current materialized view pending refresh jobs number") {
            @Override
            public Long getValue() {
                String mvTaskName = TaskBuilder.getMvTaskName(mvId.getId());
                TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
                if (taskManager == null) {
                    return 0L;
                }
                if (!taskManager.containTask(mvTaskName)) {
                    return 0L;
                }
                Long taskId = taskManager.getTask(mvTaskName).getId();
                return taskManager.getTaskRunManager().getPendingTaskRunCount(taskId);
            }
        };
        metrics.add(counterRefreshPendingJobs);

        counterRefreshRunningJobs = new GaugeMetric<Long>("mv_refresh_running_jobs", MetricUnit.NOUNIT,
                "current materialized view running refresh jobs number") {
            @Override
            public Long getValue() {
                String mvTaskName = TaskBuilder.getMvTaskName(mvId.getId());
                TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
                if (taskManager == null) {
                    return 0L;
                }
                if (!taskManager.containTask(mvTaskName)) {
                    return 0L;
                }
                Long taskId = taskManager.getTask(mvTaskName).getId();
                if (taskManager.getTaskRunManager().containsTaskInRunningTaskRunMap(taskId)) {
                    return 1L;
                } else {
                    return 0L;
                }
            }
        };
        metrics.add(counterRefreshRunningJobs);

        counterRowNums = new GaugeMetric<Long>("mv_row_count", MetricUnit.NOUNIT,
                "current materialized view's row count") {
            @Override
            public Long getValue() {
                Database db = GlobalStateMgr.getCurrentState().getDb(mvId.getDbId());
                if (db == null) {
                    return 0L;
                }
                Table table = db.getTable(mvId.getId());
                if (!table.isMaterializedView()) {
                    return 0L;
                }

                db.readLock();
                try {
                    MaterializedView mv = (MaterializedView) table;
                    return mv.getRowCount();
                } catch (Exception e) {
                    return 0L;
                } finally {
                    db.readUnlock();
                }
            }
        };
        metrics.add(counterRowNums);

        counterStorageSize = new GaugeMetric<Long>("mv_storage_size", MetricUnit.NOUNIT,
                "current materialized view's storage size") {
            @Override
            public Long getValue() {
                Database db = GlobalStateMgr.getCurrentState().getDb(mvId.getDbId());
                if (db == null) {
                    return 0L;
                }
                Table table = db.getTable(mvId.getId());
                if (!table.isMaterializedView()) {
                    return 0L;
                }

                db.readLock();
                try {
                    MaterializedView mv = (MaterializedView) table;
                    return mv.getDataSize();
                } catch (Exception e) {
                    return 0L;
                } finally {
                    db.readUnlock();
                }
            }
        };
        metrics.add(counterStorageSize);
    }

    public static boolean isUpdateMaterializedViewMetrics(ConnectContext connectContext) {
        if (connectContext == null) {
            return false;
        }
        // ignore: explain queries
        if (connectContext.getExplainLevel() != null) {
            return false;
        }
        // ignore: queries that are not using materialized view rewrite(eg: stats jobs)
        if (!connectContext.getSessionVariable().isEnableMaterializedViewRewrite() ||
                !Config.enable_materialized_view) {
            return false;
        }
        return true;
    }

    public void increaseQueryConsideredCount(long count) {
        this.counterQueryConsideredTotal.increase(count);
    }

    public void increaseQueryMatchedCount(long count) {
        this.counterQueryMatchedTotal.increase(count);
    }

    public void increaseQueryHitCount(long count) {
        this.counterQueryHitTotal.increase(count);
    }

    public void increaseQueryMaterializedViewCount(long count) {
        this.counterQueryMaterializedViewTotal.increase(count);
    }

    public void increaseRefreshJobStatus(PartitionBasedMvRefreshProcessor.RefreshJobStatus status) {
        switch (status) {
            case EMPTY:
                this.counterRefreshJobEmptyTotal.increase(1L);
                break;
            case SUCCESS:
                this.counterRefreshJobSuccessTotal.increase(1L);
                break;
            case FAILED:
                this.counterRefreshJobFailedTotal.increase(1L);
                break;
            case TOTAL:
                this.counterRefreshJobTotal.increase(1L);
                break;
        }
    }

    public void increaseRefreshRetryMetaCount(Long retryNum) {
        this.counterRefreshJobRetryCheckChangedTotal.increase(retryNum);
    }

    public void updateRefreshDuration(long duration) {
        this.histRefreshJobDuration.update(duration);
    }
}

