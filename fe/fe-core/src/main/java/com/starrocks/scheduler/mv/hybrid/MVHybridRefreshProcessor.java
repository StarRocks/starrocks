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
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshExecutor;
import com.starrocks.scheduler.mv.MVRefreshParams;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.ivm.MVIVMRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;

import java.util.Map;

public final class MVHybridRefreshProcessor extends MVRefreshProcessor {
    private final MVPCTRefreshProcessor pctProcessor;
    private final MVIVMRefreshProcessor ivmProcessor;

    public MVHybridRefreshProcessor(Database db,
                                    MaterializedView mv,
                                    MvTaskRunContext mvContext,
                                    IMaterializedViewMetricsEntity mvEntity,
                                    MaterializedView.RefreshMode refreshMode) {
        super(db, mv, mvContext, mvEntity, refreshMode, MVHybridRefreshProcessor.class);
        this.ivmProcessor = new MVIVMRefreshProcessor(db, mv, mvContext, mvEntity,
                MaterializedView.RefreshMode.INCREMENTAL);
        this.pctProcessor = new MVPCTRefreshProcessor(db, mv, mvContext, mvEntity,
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
        // if force refresh is requested, bypass IVM and use PCT directly, which correctly handles
        // force semantics (clears visibleVersionMap, drops partitions, forces full re-materialization).
        if (mvRefreshParams.isNonTentativeForce()) {
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

        // First-batch setup: drop stale state from any prior attempt and install the freeze hook.
        // Subsequent batches reuse the persisted owner and do not enter this branch.
        if (mvRefreshParams.isCompleteRefresh()) {
            mv.getRefreshScheme().getAsyncRefreshContext().clearTempBaseTableInfoTvrDeltaState();
            pctProcessor.setAfterSyncHook(() -> {
                MaterializedView.AsyncRefreshContext refreshContext =
                        mv.getRefreshScheme().getAsyncRefreshContext();
                final Map<BaseTableInfo, TvrVersionRange> committedMap =
                        refreshContext.getBaseTableInfoTvrVersionRangeMap();
                final Map<BaseTableInfo, TvrVersionRange> frozen = Maps.newHashMap();
                for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
                    TvrVersionRange changedVersionRange = ivmProcessor.getBaseTableMaxChangedDelta(
                            snapshotInfo, committedMap);
                    logger.info("Base table: {}, changed version range: {}",
                            snapshotInfo.getBaseTableInfo().getTableName(), changedVersionRange);
                    frozen.put(snapshotInfo.getBaseTableInfo(), changedVersionRange);
                }
                refreshContext.replaceTempBaseTableInfoTvrDeltaMap(getStartTaskRunId(), frozen);
            });
        }

        return pctProcessor.getProcessExecPlan(taskRunContext);
    }

    @VisibleForTesting
    public MVRefreshProcessor getCurrentProcessor() {
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

}
