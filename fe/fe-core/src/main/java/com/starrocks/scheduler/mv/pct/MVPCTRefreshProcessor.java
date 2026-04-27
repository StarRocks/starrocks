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

package com.starrocks.scheduler.mv.pct;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshExecutor;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.hybrid.MVHybridRefreshProcessor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.parquet.Strings;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.scheduler.TaskRun.MV_ID;
import static com.starrocks.scheduler.TaskRun.MV_UNCOPYABLE_PROPERTIES;

/**
 * PCT(Partition Change Tracking) based materialized view refresh processor which is designed to refresh materialized views
 * based on partition changes in the base tables.
 * MVPCTRefreshProcessor is not thread safe for concurrent runs of the same materialized view
 */
public final class MVPCTRefreshProcessor extends MVRefreshProcessor {

    // One-shot callback invoked after syncAndCheckPCTPartitions and before plan building. Used by
    // Hybrid fallback to freeze TVR so the pinned state is visible to the plan builder and differ.
    private Runnable afterSyncHook;

    // True when this is a subsequent pinned batch whose pinning owner has been overwritten by
    // a newer job — we must SKIP to avoid touching the newer job's state.
    //
    // Gate on PINNED_REFRESH_JOB_ID to confirm the current run is actually a pinned batch.
    // Without this, an unrelated partial refresh running while a prior pinning job's owner
    // state has been left behind (e.g. failure/abort before final cleanup) would be falsely
    // SKIPPED and its requested partitions would never refresh.
    private boolean isStalePinnedBatch() {
        if (mvRefreshParams.isCompleteRefresh()) {
            return false;
        }
        if (mvContext.getProperties().get(TaskRun.PINNED_REFRESH_JOB_ID) == null) {
            return false;
        }
        String owner = mv.getRefreshScheme().getAsyncRefreshContext().getTempTvrOwnerStartTaskRunId();
        String startTaskRunId = getStartTaskRunId();
        if (owner != null && !owner.equals(startTaskRunId)) {
            logger.warn("Skip stale pinned batch: pinning owner={}, startTaskRunId={}", owner, startTaskRunId);
            return true;
        }
        return false;
    }

    public MVPCTRefreshProcessor(Database db, MaterializedView mv,
                                 MvTaskRunContext mvContext,
                                 IMaterializedViewMetricsEntity mvEntity,
                                 MaterializedView.RefreshMode refreshMode) {
        super(db, mv, mvContext, mvEntity, refreshMode, MVPCTRefreshProcessor.class);
    }

    public void setAfterSyncHook(Runnable afterSyncHook) {
        this.afterSyncHook = afterSyncHook;
    }

    @Override
    public int getRetryTimes(ConnectContext connectContext) {
        int maxRefreshMaterializedViewRetryNum = 1;
        if (connectContext != null && connectContext.getSessionVariable() != null) {
            maxRefreshMaterializedViewRetryNum =
                    connectContext.getSessionVariable().getQueryDebugOptions().getMaxRefreshMaterializedViewRetryNum();
            if (maxRefreshMaterializedViewRetryNum <= 0) {
                maxRefreshMaterializedViewRetryNum = 1;
            }
        }
        maxRefreshMaterializedViewRetryNum = Math.max(Config.max_mv_refresh_failure_retry_times,
                maxRefreshMaterializedViewRetryNum);
        return maxRefreshMaterializedViewRetryNum;
    }

    @Override
    public ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) throws Exception {
        if (isStalePinnedBatch()) {
            return new ProcessExecPlan(Constants.TaskRunState.SKIPPED, null, null);
        }

        // sync and check partitions of base tables
        mvPctRefreshSynchronizer.syncAndCheckPCTPartitions();

        // Clear-before-run: if the hook throws, the field is still nulled out on retry.
        final Runnable hook = this.afterSyncHook;
        this.afterSyncHook = null;
        if (hook != null) {
            hook.run();
        }

        setupPinnedRangesIfNeeded();

        // check to refresh partitions of mv and base tables
        try (Timer ignored = Tracers.watchScope("MVRefreshCheckMVToRefreshPartitions")) {
            mvPctRefreshSynchronizer.updatePCTToRefreshMetas(false);
            PCTRefreshScope refreshScope = mvContext.getRefreshScope();
            if (refreshScope == null || refreshScope.isEmpty()) {
                return new ProcessExecPlan(Constants.TaskRunState.SKIPPED, null, null);
            }
        }

        // execute the ExecPlan of insert stmt
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshPrepareRefreshPlan")) {
            PCTRefreshScope refreshScope = mvContext.getRefreshScope();
            insertStmt = prepareRefreshPlan(refreshScope.getMvPartitionsToRefresh(),
                    refreshScope.getRefTablePartitionNames());
        }
        return new ProcessExecPlan(Constants.TaskRunState.SUCCESS, mvContext.getExecPlan(), insertStmt);
    }

    @Override
    public Constants.TaskRunState execProcessExecPlan(TaskRunContext context,
                                                      ProcessExecPlan processExecPlan,
                                                      MVRefreshExecutor executor) throws Exception {
        ExecPlan mvExecPlan = processExecPlan.execPlan();
        try (Timer ignored = Tracers.watchScope("MVRefreshMaterializedView")) {
            InsertStmt insertStmt = processExecPlan.insertStmt();
            executor.executePlan(mvExecPlan, insertStmt);
        }
        // insert execute successfully, update the meta of mv according to ExecPlan
        try (Timer ignored = Tracers.watchScope("MVRefreshUpdateMeta")) {
            updateVersionMeta(mvExecPlan, pctMVToRefreshedPartitions, pctRefTableRefreshPartitions);
        }
        return Constants.TaskRunState.SUCCESS;
    }

    /**
     * Prepare the statement and plan for mv refreshing, considering the partitions of ref table
     */
    private InsertStmt prepareRefreshPlan(PCellSortedSet mvToRefreshedPartitions,
                                          PCellSetMapping refTablePartitionNames)
            throws AnalysisException, LockTimeoutException {
        // Prepare refresh connect context
        ConnectContext ctx = mvContext.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase())
                .setWarehouse(ctx.getCurrentWarehouseName())
                .setQuerySource(QueryDetail.QuerySource.MV.name())
                .setCNGroup(ctx.getCurrentComputeResourceName());

        // Prepare refresh variables
        final Set<Table> baseTables = snapshotBaseTables.values()
                .stream()
                .map(BaseTableSnapshotInfo::getBaseTable)
                .collect(Collectors.toSet());
        changeDefaultConnectContextIfNeeded(ctx, baseTables);

        // Generate AST of insert statement
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshParser")) {
            insertStmt = generateInsertAst(ctx, mvToRefreshedPartitions, mv.getTaskDefinition());
        }

        PlannerMetaLocker locker = new PlannerMetaLocker(ctx, insertStmt);
        ExecPlan execPlan = null;
        if (!locker.tryLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException(String.format("Materialized view %s.%s refresh failed: " +
                    "failed to acquire planner meta lock within %d ms when preparing refresh plan",
                    db.getFullName(), mv.getName(), Config.mv_refresh_try_lock_timeout_ms));
        }

        PCTPredicateBuilder predicateBuilder = new PCTPredicateBuilder(mvPctRefreshPartitioner);
        MVPCTRefreshPlanBuilder planBuilder = new MVPCTRefreshPlanBuilder(db, mv, mvContext, predicateBuilder);
        try {
            // Analyze and prepare a partition & Rebuild insert statement by
            // considering to-refresh partitions of ref tables/ mv
            try (Timer ignored = Tracers.watchScope("MVRefreshAnalyzer")) {
                insertStmt = planBuilder.analyzeAndBuildInsertPlan(insertStmt,
                        mvToRefreshedPartitions, refTablePartitionNames, ctx);
                // Must set execution id before StatementPlanner.plan
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
            }

            // Generate insert stmt's exec plan, make thread local ctx existed
            try (ConnectContext.ScopeGuard guard = ctx.bindScope(); Timer ignored = Tracers.watchScope("MVRefreshPlanner")) {
                ctx.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(false); //already refreshed before
                execPlan = StatementPlanner.planInsertStmt(locker, insertStmt, ctx);
            }
        } finally {
            locker.unlock();
        }

        final InsertStmt finalInsertStmt = insertStmt;
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage message = status.getMvTaskRunExtraMessage();
            if (message == null) {
                return;
            }

            // update plan builder message
            Map<String, String> planBuildMessage = planBuilder.getPlanBuilderMessage();
            if (planBuildMessage != null) {
                logger.info("MV Refresh PlanBuilderMessage: {}", planBuildMessage);
                message.setPlanBuilderMessage(planBuildMessage);
                // record the plan builder message
                Tracers.record("MVRefreshPlanBuilderInfo", planBuildMessage.toString());
            }

            final String refreshedSql = finalInsertStmt != null ? AstToSQLBuilder.buildSimple(finalInsertStmt) : "";
            // update mv refresh definition
            if (!Strings.isNullOrEmpty(refreshedSql)) {
                // Remove line separator and shrink to MAX_FIELD_VARCHAR_LENGTH-1 which is defined in the TaskRunsSystemTable.java
                String query = LogUtil.removeLineSeparator(refreshedSql);
                status.setDefinition(MvUtils.shrinkToSize(query, SystemTable.MAX_FIELD_VARCHAR_LENGTH - 1));
            }
        });

        QueryDebugOptions debugOptions = ctx.getSessionVariable().getQueryDebugOptions();
        // log the final mv refresh plan for each refresh for better trace and debug
        if (logger.isDebugEnabled() || debugOptions.isEnableQueryTraceLog()) {
            logger.info("MV Refresh Final Plan\nMV PartitionsToRefresh: {}\nBase PartitionsToScan: {}\n" +
                            "Insert Plan:\n{}",
                    mvToRefreshedPartitions, refTablePartitionNames,
                    execPlan != null ? execPlan.getExplainString(StatementBase.ExplainLevel.VERBOSE) : "");
        } else {
            logger.info("MV Refresh Final Plan, MV PartitionsToRefresh: {}, Base PartitionsToScan: {}",
                    mvToRefreshedPartitions, refTablePartitionNames);
        }

        mvContext.setExecPlan(execPlan);
        return insertStmt;
    }

    @Override
    public void generateNextTaskRunIfNeeded() {
        if (!mvContext.hasNextBatchPartition() || mvContext.getTaskRun().isKilled()) {
            return;
        }

        // Publish the next-batch cursor to the persisted TaskRunStatus. This is the single
        // source of truth that downstream continuation readers — both the async follow-up
        // dispatch below and the sync continuation loop in TaskManager.executeTaskSync — read
        // to reconstruct the next batch's ExecuteOption via buildNextBatchOption().
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setNextPartitionStart(mvContext.getNextPartitionStart());
            extraMessage.setNextPartitionEnd(mvContext.getNextPartitionEnd());
            extraMessage.setNextPartitionValues(mvContext.getNextPartitionValues());
        });

        // Construct the next-batch option through the shared builder. Using the same path for
        // sync and async ensures the two continuation flows never drift apart (e.g., someone
        // adding a new property for async forgetting to add it for sync).
        ExecuteOption nextOption = buildNextBatchOption(mvContext.getTaskRun());
        if (nextOption == null) {
            return;
        }

        // Dispatch asymmetrically (sync / test / async), but the option itself is identical
        // regardless of caller: the only thing that differs is who drives the follow-up.
        if (nextOption.getIsSync()) {
            // Sync: the TaskManager.executeTaskSync loop reads the cursor from extra message
            // (via buildNextBatchOption) and submits the next batch itself. We must NOT
            // auto-dispatch here or it would race the caller's loop.
            logger.info("[MV] Sync refresh defers next batch to TaskManager loop for MV {}-{}: " +
                            "start={}, end={}, values={}", mv.getName(), mv.getId(),
                    mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(),
                    mvContext.getNextPartitionValues());
            return;
        }

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        long mvId = Long.parseLong(mvContext.getProperties().get(MV_ID));
        String taskName = TaskBuilder.getMvTaskName(mvId);
        logger.info("[MV] Generate a task to refresh next batches of partitions for MV {}-{}, start={}, end={}, " +
                        "priority={}, properties={}", mv.getName(), mv.getId(),
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(),
                nextOption.getPriority(), nextOption.getTaskRunProperties());

        if ("true".equalsIgnoreCase(mvContext.getProperties().getOrDefault(TaskRun.IS_TEST, ""))) {
            // test-only hook: stash for the test driver to pick up
            nextTaskRun = TaskRunBuilder
                    .newBuilder(taskManager.getTask(taskName))
                    .properties(nextOption.getTaskRunProperties())
                    .setExecuteOption(nextOption)
                    .build();
        } else {
            taskManager.executeTask(taskName, nextOption);
        }
    }

    /**
     * Build the {@link ExecuteOption} for the next refresh batch given the previous batch's
     * {@link TaskRun}, which carries everything needed in one object:
     * <ul>
     *   <li>its merged runtime properties (source of FORCE, warehouse, etc. — including any
     *       properties mutated after the option was constructed),</li>
     *   <li>its original {@link ExecuteOption} (for priority / isSync / isManual inheritance),</li>
     *   <li>its persisted {@link TaskRunStatus} with the
     *       {@link MVTaskRunExtraMessage} next-partition cursor.</li>
     * </ul>
     *
     * <p>This is the <b>single</b> entry point used by both continuation paths:
     * <ul>
     *   <li>{@link #generateNextTaskRunIfNeeded} — async follow-up dispatch from within
     *       the worker thread that just finished batch N;</li>
     *   <li>{@link com.starrocks.scheduler.TaskManager#executeTaskSync} — sync continuation
     *       loop on the SQL thread that submitted the original REFRESH ... WITH SYNC MODE.</li>
     * </ul>
     * Centralising the option construction here is important: any future field that needs to
     * flow across batches (new property, new cursor type, new priority rule) must be added in
     * one place rather than kept in lockstep across parallel sync/async implementations.
     *
     * @return a new option for the next batch, or {@code null} if the previous TaskRun carries
     *         no continuation cursor — signalling "no more batches" to both drivers.
     */
    public static ExecuteOption buildNextBatchOption(TaskRun prevTaskRun) {
        if (prevTaskRun == null) {
            return null;
        }
        TaskRunStatus prevStatus = prevTaskRun.getStatus();
        ExecuteOption prevOption = prevTaskRun.getExecuteOption();
        if (prevStatus == null || prevOption == null) {
            return null;
        }
        MVTaskRunExtraMessage extra = prevStatus.getMvTaskRunExtraMessage();
        if (extra == null) {
            return null;
        }
        String nextStart = extra.getNextPartitionStart();
        String nextEnd = extra.getNextPartitionEnd();
        String nextValues = extra.getNextPartitionValues();
        boolean hasRangeCursor = !Strings.isNullOrEmpty(nextStart) && !Strings.isNullOrEmpty(nextEnd);
        boolean hasListCursor = !Strings.isNullOrEmpty(nextValues);
        if (!hasRangeCursor && !hasListCursor) {
            return null;
        }

        // Read from the TaskRun's merged runtime properties (not the option's snapshot) so we
        // catch any properties set on the TaskRun after its option was built — most notably
        // FORCE, which tests and external callers toggle directly on the TaskRun.
        Map<String, String> prevProps = prevTaskRun.getProperties();
        Map<String, String> nextProps = Maps.newHashMap();
        if (prevProps != null) {
            for (Map.Entry<String, String> entry : prevProps.entrySet()) {
                // Skip per-batch keys that must be set specifically for the next batch (the
                // current PARTITION_START / PARTITION_END / PARTITION_VALUES belong to batch N,
                // not batch N+1).
                if (entry.getKey() == null || entry.getValue() == null
                        || MV_UNCOPYABLE_PROPERTIES.contains(entry.getKey())) {
                    continue;
                }
                nextProps.put(entry.getKey(), entry.getValue());
            }
        }
        // Install the new partition cursor. Cursor shape (range vs list) comes from whichever
        // field the partitioner populated — we do not need the MV object here.
        if (hasListCursor) {
            nextProps.put(TaskRun.PARTITION_VALUES, nextValues);
        } else {
            nextProps.put(TaskRun.PARTITION_START, nextStart);
            nextProps.put(TaskRun.PARTITION_END, nextEnd);
        }
        // Preserve the originating task run id so every continuation batch is grouped in
        // history under the first batch's id.
        if (!Strings.isNullOrEmpty(prevStatus.getStartTaskRunId())) {
            nextProps.put(TaskRun.START_TASK_RUN_ID, prevStatus.getStartTaskRunId());
        }
        if (isPinnedRefreshTaskRun(prevTaskRun) && !Strings.isNullOrEmpty(prevStatus.getStartTaskRunId())) {
            nextProps.put(TaskRun.PINNED_REFRESH_JOB_ID, prevStatus.getStartTaskRunId());
        }

        // Bump priority to HIGHER for continuation batches so a long refresh does not starve
        // behind newly arriving lower-priority tasks (matches the pre-refactor async path).
        int priority = prevOption.getPriority() > Constants.TaskRunPriority.LOWEST.value()
                ? prevOption.getPriority()
                : Constants.TaskRunPriority.HIGHER.value();
        ExecuteOption nextOption = new ExecuteOption(priority, true, nextProps);
        nextOption.setSync(prevOption.getIsSync());
        nextOption.setManual(prevOption.isManual());
        return nextOption;
    }

    private static boolean isPinnedRefreshTaskRun(TaskRun prevTaskRun) {
        if (!(prevTaskRun.getProcessor() instanceof MVTaskRunProcessor)) {
            return false;
        }
        MVRefreshProcessor refreshProcessor =
                ((MVTaskRunProcessor) prevTaskRun.getProcessor()).getMVRefreshProcessor();
        return (refreshProcessor instanceof MVPCTRefreshProcessor || refreshProcessor instanceof MVHybridRefreshProcessor)
                && refreshProcessor.isPinnedMode();
    }

    @VisibleForTesting
    public Map<Long, BaseTableSnapshotInfo> getSnapshotBaseTables() {
        return snapshotBaseTables;
    }

    @Override
    public BaseTableSnapshotInfo buildBaseTableSnapshotInfo(BaseTableInfo baseTableInfo, Table table) {
        return new PCTTableSnapshotInfo(baseTableInfo, table);
    }

    public MVPCTRefreshPartitioner getMvPctRefreshPartitioner() {
        return mvPctRefreshPartitioner;
    }

    @Override
    public void updateVersionMeta(ExecPlan execPlan,
                                  PCellSortedSet mvRefreshedPartitions,
                                  Map<BaseTableSnapshotInfo, PCellSortedSet> refTableAndPartitionNames) {
        final String startTaskRunId = getStartTaskRunId();
        final boolean isLastBatch = !mvContext.hasNextBatchPartition();

        // Promote TVR only on the last batch, and only when we still own the pending state — a
        // newer job that overwrote the owner must not have its delta replaced by ours.
        Map<BaseTableInfo, TvrVersionRange> tvrMap = Maps.newHashMap();
        if (isLastBatch) {
            MaterializedView.AsyncRefreshContext ctxBefore = mv.getRefreshScheme().getAsyncRefreshContext();
            if (ownsPinningState(ctxBefore, startTaskRunId)) {
                tvrMap = ctxBefore.getTempBaseTableInfoTvrDeltaMap();
            } else {
                logger.warn("Skip TVR promotion: pinning owner={}, startTaskRunId={}",
                        ctxBefore.getTempTvrOwnerStartTaskRunId(), startTaskRunId);
            }
        }
        // updatePCTMeta swaps the refresh scheme on the MV via copy-on-write + editlog replay,
        // so re-read AsyncRefreshContext after this call — the previous reference is detached.
        updatePCTMeta(execPlan, pctMVToRefreshedPartitions, pctRefTableRefreshPartitions, tvrMap);

        // Clear only if we still own it (defence-in-depth behind the early-SKIP in
        // getProcessExecPlan); otherwise a stale batch would wipe a newer job's pending state.
        if (isLastBatch) {
            MaterializedView.AsyncRefreshContext ctxAfter = mv.getRefreshScheme().getAsyncRefreshContext();
            if (ownsPinningState(ctxAfter, startTaskRunId)) {
                ctxAfter.clearTempBaseTableInfoTvrDeltaState();
            } else {
                logger.warn("Skip clearTempBaseTableInfoTvrDeltaState: pinning owner={}, startTaskRunId={}",
                        ctxAfter.getTempTvrOwnerStartTaskRunId(), startTaskRunId);
            }
        }
    }

    // True when the pinning slot is either unowned or owned by the current job — safe to mutate.
    private static boolean ownsPinningState(MaterializedView.AsyncRefreshContext ctx, String startTaskRunId) {
        String owner = ctx.getTempTvrOwnerStartTaskRunId();
        return owner == null || owner.equals(startTaskRunId);
    }
}
