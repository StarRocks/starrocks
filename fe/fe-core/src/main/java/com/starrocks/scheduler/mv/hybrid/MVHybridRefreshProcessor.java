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
import com.starrocks.common.MaterializedViewExceptions;
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
import java.util.Optional;

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
                MaterializedView.RefreshMode.PCT);
    }

    @Override
    public ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) throws Exception {
        if (!isPctOnly(mvRefreshParams)) {
            return switchToIVMRefresh(taskRunContext);
        }
        // A partial request carries no reason: the analyzer rejects an explicit one, so the only run that
        // reaches here partial is a later batch of a pct job, which inherits the lead run's decision.
        if (mvRefreshParams.isNonTentativeForce() && isBatchLeadRun()) {
            recordRefreshModeReason(MaterializedView.RefreshModeReason.FORCE_REFRESH, null);
        }
        return switchToPCTRefresh(taskRunContext);
    }

    /**
     * Whether only pct can serve this request. Force semantics -- clearing visibleVersionMap, dropping
     * partitions, re-materializing in full -- are pct's, and a named partition range is not a delta.
     */
    @VisibleForTesting
    static boolean isPctOnly(MVRefreshParams mvRefreshParams) {
        return !mvRefreshParams.isCompleteRefresh() || mvRefreshParams.isNonTentativeForce();
    }

    private ProcessExecPlan switchToIVMRefresh(TaskRunContext taskRunContext) throws Exception {
        // try ivm first, and if failed, transfer to pct
        try {
            this.runRefreshMode = MaterializedView.RefreshMode.INCREMENTAL;
            logger.info("Try to do ivm refresh for mv: {}, run refresh mode: {}",
                    mv.getName(), this.runRefreshMode);
            updateTaskRunStatus(status -> {
                status.getMvTaskRunExtraMessage().setRefreshMode(runRefreshMode.name());
            });
            return ivmProcessor.getProcessExecPlan(taskRunContext);
        } catch (Exception e) {
            logger.warn("Failed to do ivm refresh for mv: {}, try pct refresh. error: {}",
                    mv.getName(), e);
            recordRefreshModeReasonIfAbsent(MaterializedView.RefreshModeReason.UNKNOWN);
            return switchToPCTRefresh(taskRunContext);
        }
    }

    private ProcessExecPlan switchToPCTRefresh(TaskRunContext taskRunContext) throws Exception {
        this.runRefreshMode = MaterializedView.RefreshMode.PCT;
        updateTaskRunStatus(status -> {
            status.getMvTaskRunExtraMessage().setRefreshMode(runRefreshMode.name());
        });
        // reset the task run id for pct
        this.mvContext.getCtx().setQueryId(UUIDUtil.genUUID());

        // First-batch setup: drop stale state from any prior attempt and install the freeze hook, which
        // the sync path fires before it aligns partitions against the frozen snapshot.
        // Subsequent batches reuse the persisted owner and do not enter this branch.
        if (mvRefreshParams.isCompleteRefresh()) {
            mv.getRefreshScheme().getAsyncRefreshContext().clearTempBaseTableInfoTvrDeltaState();
            pctProcessor.setBeforePartitionAlignHook(() -> {
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
        return runRefreshMode.isIncremental() ? ivmProcessor : pctProcessor;
    }

    @Override
    public Constants.TaskRunState execProcessExecPlan(TaskRunContext taskRunContext,
                                                      ProcessExecPlan processExecPlan,
                                                      MVRefreshExecutor executor) throws Exception {
        try {
            return getCurrentProcessor().execProcessExecPlan(taskRunContext, processExecPlan, executor);
        } catch (Exception e) {
            Optional<MaterializedView.RefreshModeReason> reason =
                    fallbackReasonOnExecutionFailure(runRefreshMode, mv.getCurrentRefreshMode(), e);
            if (reason.isEmpty()) {
                throw e;
            }
            logger.warn("Incremental refresh for mv {} was rejected by the backend ({}), falling back to pct " +
                    "for this run", mv.getName(), reason.get(), e);
            // The backend reports a tablet, not a table, so no single base table is attributable.
            recordRefreshModeReason(reason.get(), null);
            try {
                ProcessExecPlan pctPlan = switchToPCTRefresh(taskRunContext);
                if (pctPlan.state() == Constants.TaskRunState.SKIPPED) {
                    return Constants.TaskRunState.SKIPPED;
                }
                return pctProcessor.execProcessExecPlan(taskRunContext, pctPlan, executor);
            } catch (Exception pctFailure) {
                // Carry the rejection that sent this run to pct, or the reported failure looks like a plain
                // pct error and the incremental attempt that preceded it is only visible in the log.
                pctFailure.addSuppressed(e);
                throw pctFailure;
            }
        }
    }

    /**
     * Some rejections only surface once the backend reads the changes, past the plan-time fallback: a row
     * delete on a duplicate-key or aggregate base table, a base version no longer reachable, a version
     * whose changes were never captured. Only an AUTO view may recover from them here -- an INCREMENTAL one
     * also reaches this processor when a base table needs its TVR baseline rebuilt, and switching that run
     * to pct would silently give it the approximate semantics it declined.
     */
    @VisibleForTesting
    static Optional<MaterializedView.RefreshModeReason> fallbackReasonOnExecutionFailure(
            MaterializedView.RefreshMode runRefreshMode,
            MaterializedView.RefreshMode settledMode,
            Throwable e) {
        if (!runRefreshMode.isIncremental() || settledMode != MaterializedView.RefreshMode.AUTO
                || !MaterializedViewExceptions.isChangeNotTrackableFailure(e)) {
            return Optional.empty();
        }
        // One error code covers four conditions. Two of them an operator caused and can act on; the
        // rest -- a base version the ancestor chain cannot reach, a capture that recorded a failure --
        // mean an invariant broke, and naming them would send the operator after a setting to change.
        if (MaterializedViewExceptions.isRowDeleteRejectionFailure(e)) {
            return Optional.of(MaterializedView.RefreshModeReason.NON_APPEND_ONLY_CHANGE);
        }
        if (MaterializedViewExceptions.isCaptureDisabledRejectionFailure(e)) {
            return Optional.of(MaterializedView.RefreshModeReason.CHANGE_CAPTURE_DISABLED);
        }
        return Optional.of(MaterializedView.RefreshModeReason.UNKNOWN);
    }

    @Override
    public BaseTableSnapshotInfo buildBaseTableSnapshotInfo(BaseTableInfo baseTableInfo, Table table) {
        return getCurrentProcessor().buildBaseTableSnapshotInfo(baseTableInfo, table);
    }

    @Override
    public boolean generateNextTaskRunIfNeeded() {
        return getCurrentProcessor().generateNextTaskRunIfNeeded();
    }

    @Override
    public boolean hasNextBatchRun() {
        return getCurrentProcessor().hasNextBatchRun();
    }

}
