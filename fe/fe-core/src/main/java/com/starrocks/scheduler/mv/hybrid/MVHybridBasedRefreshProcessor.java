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

package com.starrocks.scheduler.mv.hybrid;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.BaseMVRefreshProcessor;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshExecutor;
import com.starrocks.scheduler.mv.MVRefreshParams;
import com.starrocks.scheduler.mv.ivm.MVIVMBasedRefreshProcessor;
import com.starrocks.scheduler.mv.ivm.TvrTableSnapshotInfo;
import com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor;
import com.starrocks.sql.plan.ExecPlan;

import java.util.Map;
import java.util.Set;

public final class MVHybridBasedRefreshProcessor extends BaseMVRefreshProcessor {
    private final MVPCTBasedRefreshProcessor pctProcessor;
    private final MVIVMBasedRefreshProcessor ivmProcessor;

    // This map is used to store the temporary tvr version range for each base table
    private final Map<BaseTableInfo, TvrVersionRange> tempMvTvrVersionRangeMap = Maps.newConcurrentMap();

    public MVHybridBasedRefreshProcessor(Database db,
                                         MaterializedView mv,
                                         MvTaskRunContext mvContext,
                                         IMaterializedViewMetricsEntity mvEntity,
                                         MaterializedView.RefreshMode refreshMode) {
        super(db, mv, mvContext, mvEntity, refreshMode, MVHybridBasedRefreshProcessor.class);
        this.ivmProcessor = new MVIVMBasedRefreshProcessor(db, mv, mvContext, mvEntity,
                MaterializedView.RefreshMode.INCREMENTAL);
        this.pctProcessor = new MVPCTBasedRefreshProcessor(db, mv, mvContext, mvEntity,
                MaterializedView.RefreshMode.AUTO);
    }

    @Override
    public ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) throws Exception {
        if (isIVMRefreshEnabled(mvRefreshParams)) {
            // if ivm refresh is enabled, try ivm first
            return switchToIVMRefresh(taskRunContext);
        } else {
            return switchToPCTRefresh(taskRunContext);
        }
    }

    private boolean isIVMRefreshEnabled(MVRefreshParams mvRefreshParams) {
        // if this is not a complete refresh and is a partial refresh, use pct refresh instead.
        if (!mvRefreshParams.isCompleteRefresh()) {
            return false;
        }
        return true;
    }

    private ProcessExecPlan switchToIVMRefresh(TaskRunContext taskRunContext) throws Exception {
        // try ivm first, and if failed, transfer to pct
        try {
            this.currentRefreshMode = MaterializedView.RefreshMode.INCREMENTAL;
            logger.info("Try to do ivm refresh for mv: {}, current refresh mode: {}",
                    mv.getName(), this.currentRefreshMode);
            updateTaskRunStatus(status -> {
                status.getMvTaskRunExtraMessage().setRefreshMode(currentRefreshMode.name());
            });
            return ivmProcessor.getProcessExecPlan(taskRunContext);
        } catch (Exception e) {
            logger.warn("Failed to do ivm refresh for mv: {}, try pct refresh. error: {}",
                    mv.getName(), e);
            return switchToPCTRefresh(taskRunContext);
        }
    }

    private ProcessExecPlan switchToPCTRefresh(TaskRunContext taskRunContext) throws Exception {
        this.currentRefreshMode = MaterializedView.RefreshMode.AUTO;
        // update the task run status to pct refresh
        updateTaskRunStatus(status -> {
            status.getMvTaskRunExtraMessage().setRefreshMode(MaterializedView.RefreshMode.PCT.name());
        });
        // reset the task run id for pct
        this.mvContext.getCtx().setQueryId(UUIDUtil.genUUID());
        // If the refresh is transferred to pct, we need to refresh the changed partitions once
        // and cannot generate multi-task-runs.
        pctProcessor.getMvRefreshParams().setCanGenerateNextTaskRun(false);

        // try get the tvr version range map from ivm processor
        final Map<BaseTableInfo, TvrVersionRange> mvTvrVersionRangeMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
        for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            TvrVersionRange changedVersionRange = ivmProcessor.getBaseTableChangedVersionRange(snapshotInfo,
                    mvTvrVersionRangeMap, this.currentRefreshMode);
            logger.info("Base table: {}, changed version range: {}",
                    snapshotInfo.getBaseTableInfo().getTableName(), changedVersionRange);
            // collect changed version range
            TvrTableSnapshotInfo tvrTableSnapshotInfo = new TvrTableSnapshotInfo(snapshotInfo.getBaseTableInfo(),
                    snapshotInfo.getBaseTable());
            tempMvTvrVersionRangeMap.put(snapshotInfo.getBaseTableInfo(), changedVersionRange);
            // update the snapshot info with the changed version range
            tvrTableSnapshotInfo.setTvrSnapshot(changedVersionRange);
        }
        return pctProcessor.getProcessExecPlan(taskRunContext);
    }

    @VisibleForTesting
    public BaseMVRefreshProcessor getCurrentProcessor() {
        if (this.currentRefreshMode == MaterializedView.RefreshMode.AUTO) {
            return pctProcessor;
        } else {
            return ivmProcessor;
        }
    }

    @Override
    public Constants.TaskRunState execProcessExecPlan(TaskRunContext taskRunContext,
                                                      ProcessExecPlan processExecPlan,
                                                      MVRefreshExecutor executor) throws Exception {
        return getCurrentProcessor().execProcessExecPlan(taskRunContext, processExecPlan, executor);
    }

    @Override
    public BaseTableSnapshotInfo buildBaseTableSnapshotInfo(BaseTableInfo baseTableInfo, Table table) {
        return getCurrentProcessor().buildBaseTableSnapshotInfo(baseTableInfo, table);
    }

    @Override
    public void generateNextTaskRunIfNeeded() {
        getCurrentProcessor().generateNextTaskRunIfNeeded();
    }

    @Override
    public void updateVersionMeta(ExecPlan execPlan,
                                  Set<String> mvRefreshedPartitions,
                                  Map<BaseTableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        if (this.currentRefreshMode == MaterializedView.RefreshMode.INCREMENTAL) {
            ivmProcessor.updateVersionMeta(execPlan, mvRefreshedPartitions, refTableAndPartitionNames);
        } else {
            if (mvContext.hasNextBatchPartition()) {
                updatePCTMeta(execPlan, pctMVToRefreshedPartitions, pctRefTableRefreshPartitions, Maps.newHashMap());
            } else {
                // if this is the last task run for a refresh job, update tempMvTvrVersionRangeMap instead.
                updatePCTMeta(execPlan, pctMVToRefreshedPartitions, pctRefTableRefreshPartitions, tempMvTvrVersionRangeMap);
            }
        }
    }
}
