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
<<<<<<< HEAD
=======
import com.starrocks.common.CdcErrorUtils;
>>>>>>> 103084cf4fc... [Refactor][RB-390] Classify MV refresh failures by type instead of message text (#65255)
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
<<<<<<< HEAD
=======
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.common.ChangeNotTrackableException;
import org.apache.commons.lang3.exception.ExceptionUtils;
>>>>>>> 103084cf4fc... [Refactor][RB-390] Classify MV refresh failures by type instead of message text (#65255)

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
<<<<<<< HEAD
        return getCurrentProcessor().execProcessExecPlan(taskRunContext, processExecPlan, executor);
=======
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
            recordRefreshModeReason(reason.get(),
                    reason.get() == MaterializedView.RefreshModeReason.CHANGE_CAPTURE_DISABLED
                            ? soleCaptureCandidate(snapshotBaseTables.values()) : null);
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
        Optional<CdcErrorUtils.Parsed> rejection = ExceptionUtils.getThrowableList(e).stream()
                .filter(ChangeNotTrackableException.class::isInstance)
                .map(t -> ((ChangeNotTrackableException) t).getRejection())
                .findFirst();
        if (!runRefreshMode.isIncremental() || settledMode != MaterializedView.RefreshMode.AUTO
                || rejection.isEmpty()) {
            return Optional.empty();
        }
        // One error code covers four conditions. Two of them an operator caused and can act on; the
        // rest -- a base version the ancestor chain cannot reach, a capture that recorded a failure --
        // mean an invariant broke, and naming them would send the operator after a setting to change.
        if (CdcErrorUtils.isRowDeleteRejection(rejection.get())) {
            return Optional.of(MaterializedView.RefreshModeReason.NON_APPEND_ONLY_CHANGE);
        }
        if (CdcErrorUtils.isCaptureDisabledRejection(rejection.get())) {
            return Optional.of(MaterializedView.RefreshModeReason.CHANGE_CAPTURE_DISABLED);
        }
        return Optional.of(MaterializedView.RefreshModeReason.UNKNOWN);
    }

    /**
     * The base table to blame for a capture-disabled rejection, or null when more than one could be. The
     * backend's check only fires for a primary-key table whose changes this run had to read, so one such
     * base table is the only place it can have come from. The capture property is deliberately not
     * consulted: this rejection means the window spanned versions published while capture was off, which
     * the operator may well have turned back on before the refresh ran. Typed with instanceof rather than
     * cast because a snapshot of another shape should cost the label, not the refresh it is labelling.
     */
    @VisibleForTesting
    static String soleCaptureCandidate(Collection<BaseTableSnapshotInfo> snapshots) {
        String candidate = null;
        for (BaseTableSnapshotInfo snapshotInfo : snapshots) {
            if (!(snapshotInfo.getBaseTable() instanceof OlapTable)
                    || ((OlapTable) snapshotInfo.getBaseTable()).getKeysType() != KeysType.PRIMARY_KEYS) {
                continue;
            }
            if (!(snapshotInfo instanceof TvrTableSnapshotInfo)) {
                continue;
            }
            TvrVersionRange delta = ((TvrTableSnapshotInfo) snapshotInfo).getTvrSnapshot();
            if (delta == null || delta.isEmpty()) {
                continue;
            }
            if (candidate != null) {
                return null;
            }
            candidate = snapshotInfo.getBaseTableInfo().getReadableString();
        }
        return candidate;
>>>>>>> 103084cf4fc... [Refactor][RB-390] Classify MV refresh failures by type instead of message text (#65255)
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
