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
// limitations under the License

package com.starrocks.scheduler.mv.ivm;

import com.google.api.client.util.Sets;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.BaseMVRefreshProcessor;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.scheduler.TaskRun.MV_ID;
import static com.starrocks.scheduler.TaskRun.MV_UNCOPYABLE_PROPERTIES;

/**
 * Incremental View Materialization (IVM) based MV refresh processor:
 * - Collect base table changed snapshot infos.
 * - Build an incremental refresh plan based on the changed version ranges of base tables.
 * - Execute the refresh plan to update the materialized view.
 */
public class MVIVMBasedMVRefreshProcessor extends BaseMVRefreshProcessor {
    // This map is used to store the temporary tvr version range for each base table
    private final Map<BaseTableInfo, TvrVersionRange> tempMvTvrVersionRangeMap = Maps.newConcurrentMap();
    // whether the next task run is needed
    private boolean hasNextTaskRun = false;
    public MVIVMBasedMVRefreshProcessor(Database db, MaterializedView mv,
                                        MvTaskRunContext mvContext,
                                        IMaterializedViewMetricsEntity mvEntity) {
        super(db, mv, mvContext, mvEntity, MVIVMBasedMVRefreshProcessor.class);
    }

    public ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) throws Exception {
        syncAndCheckPCTPartitions(taskRunContext);

        // collect change snapshots
        try (Timer ignored = Tracers.watchScope("MVRefreshCheckChangedVersionRanges")) {
            final Map<BaseTableInfo, TvrVersionRange> mvTvrVersionRangeMap =
                    mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
            for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
                TvrVersionRange changedVersionRange =
                        getBaseTableChangedVersionRange(snapshotInfo, mvTvrVersionRangeMap);
                logger.info("Base table: {}, changed version range: {}",
                        snapshotInfo.getBaseTableInfo().getTableName(), changedVersionRange);
                // collect changed version range
                TvrTableSnapshotInfo tvrTableSnapshotInfo = (TvrTableSnapshotInfo) snapshotInfo;

                tvrTableSnapshotInfo.setTvrSnapshot(changedVersionRange);
                tempMvTvrVersionRangeMap.put(snapshotInfo.getBaseTableInfo(), changedVersionRange);
                // update the snapshot info with the changed version range
                tvrTableSnapshotInfo.setTvrSnapshot(changedVersionRange);
            }
        }
        boolean isTaskRunSkipped = snapshotBaseTables.values().stream()
                .map(snapshotInfo -> (TvrTableSnapshotInfo) snapshotInfo)
                .map(TvrTableSnapshotInfo::getTvrSnapshot)
                .allMatch(TvrVersionRange::isEmpty);
        if (isTaskRunSkipped) {
            logger.info("No base table has changed, skip the refresh for materialized view: {}",
                    mv.getName());
            return new ProcessExecPlan(Constants.TaskRunState.SKIPPED, null, null);
        }

        try (Timer ignored = Tracers.watchScope("MVRefreshCheckMVToRefreshPartitions")) {
            try {
                updatePCTToRefreshMetas(taskRunContext);
            } catch (Exception e) {
                // if the check failed, we should not throw exception here
                // because this check only affects mv refresh rather than mv refresh.
                logger.warn("Failed to check PCT partitions for materialized view: {}, error: {}",
                        mv.getName(), e.getMessage(), e);
            }
        }

        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshPrepareRefreshPlan")) {
            insertStmt = prepareRefreshPlan();
        }
        return new ProcessExecPlan(Constants.TaskRunState.SUCCESS, mvContext.getExecPlan(), insertStmt);
    }

    @Override
    public Constants.TaskRunState doProcessTaskRun(TaskRunContext taskRunContext,
                                                   MVRefreshExecutor executor) throws Exception {
        try {
            return refreshMaterializedView(executor);
        } catch (Exception e) {
            logger.warn("Failed to process task run for materialized view: {}, error: {}",
                    mv.getName(), e.getMessage(), e);
            throw e;
        }
    }

    private Constants.TaskRunState refreshMaterializedView(MVRefreshExecutor executor) throws Exception {
        final ProcessExecPlan processExecPlan = getProcessExecPlan(mvContext);
        if (processExecPlan.state() == Constants.TaskRunState.SKIPPED) {
            logger.info("Skip the refresh for materialized view: {}, no base table has changed",
                    mv.getName());
            return Constants.TaskRunState.SKIPPED;
        }

        final InsertStmt insertStmt = processExecPlan.insertStmt();
        final ExecPlan execPlan = processExecPlan.execPlan();
        try (Timer ignored = Tracers.watchScope("MVRefreshMaterializedView")) {
            MaterializedView.AsyncRefreshContext mvRefreshContext =
                    mv.getRefreshScheme().getAsyncRefreshContext();
            logger.info("temp tvr version range map: {}", tempMvTvrVersionRangeMap);
            mvRefreshContext.getTempBaseTableInfoTvrDeltaMap().putAll(tempMvTvrVersionRangeMap);
            executor.executePlan(execPlan, insertStmt);
        }

        try (Timer ignored = Tracers.watchScope("MVRefreshUpdateMeta")) {
            try {
                updatePCTMeta(execPlan, pctMVToRefreshedPartitions, pctRefTableRefreshPartitions);
            } catch (Exception e) {
                // if the update meta failed, we should not throw exception here
                // because this meta-update only affects pct-based refresh rather than ivm-based mv refresh, which means only
                // affect mv rewrite only.
                logger.warn("Failed to update meta for materialized view: {}, error: {}",
                        mv.getName(), e.getMessage(), e);
            }
        }

        return Constants.TaskRunState.SUCCESS;
    }

    private TvrTableDelta getBaseTableChangedVersionRange(BaseTableSnapshotInfo snapshotInfo,
                                                          Map<BaseTableInfo, TvrVersionRange> mvTvrVersionRangeMap) {
        final BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
        final Table snapshotTable = snapshotInfo.getBaseTable();

        Optional<Table> optTable = MvUtils.getTableWithIdentifier(baseTableInfo);
        if (optTable.isEmpty()) {
            throw new SemanticException("Base table %s.%s does not exist",
                    baseTableInfo.getDbName(), baseTableInfo.getTableName());
        }
        if (!snapshotTable.isIcebergTable()) {
            throw new SemanticException("Only support Iceberg table for MVIVMBasedMVRefreshProcessor, " +
                    "but got: " + snapshotTable.getType());
        }
        IcebergTable icebergTable = (IcebergTable) snapshotTable;

        final TvrTableDelta maxTvrDelta = getMaxBaseTableChangedDelta(baseTableInfo, icebergTable, mvTvrVersionRangeMap);
        // if no change, return empty
        if (maxTvrDelta.isEmpty()) {
            return maxTvrDelta;
        }

        // check the delta traits between the max delta
        List<TvrTableDeltaTrait> tableDeltaTraits = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .listTableDeltaTraits(baseTableInfo.getDbName(), icebergTable,
                        maxTvrDelta.fromSnapshot(), maxTvrDelta.toSnapshot());
        if (CollectionUtils.isEmpty(tableDeltaTraits)) {
            logger.warn("No tvr delta traits found for base table: {}, db: {}", baseTableInfo.getTableName(),
                    baseTableInfo.getDbName());
            throw new SemanticException("No tvr delta traits found for base table: %s.%s",
                    baseTableInfo.getDbName(), baseTableInfo.getTableName());
        }
        // check whether the last delta trait is equal to the max delta
        TvrTableDeltaTrait lastTvrDeltaTrait = tableDeltaTraits.get(tableDeltaTraits.size() - 1);
        TvrTableSnapshot lastTvrDeltaSnapshot = lastTvrDeltaTrait.getTvrDelta().toSnapshot();
        if (!lastTvrDeltaSnapshot.equals(maxTvrDelta.toSnapshot())) {
            logger.warn("The last tvr delta snapshot: {} is not equal to the max tvr delta snapshot: {}, " +
                    "use the max tvr delta instead", lastTvrDeltaSnapshot, maxTvrDelta.toSnapshot());
            throw new SemanticException("The last tvr delta snapshot: %s is not equal to the max tvr delta snapshot: %s",
                    lastTvrDeltaSnapshot, maxTvrDelta.toSnapshot());
        }
        for (TvrTableDeltaTrait deltaTrait : tableDeltaTraits) {
            // TODO: We may need to handle the case where the deltaTrait is not append-only.
            if (!deltaTrait.isAppendOnly()) {
                throw new SemanticException("TvrTableDeltaTrait is not append-only for base table: %s.%s, delta:%s",
                        baseTableInfo.getDbName(), baseTableInfo.getTableName(), deltaTrait);
            }
        }
        boolean isIVMRefreshAdaptive = isIVMRefreshAdaptive();
        if (isIVMRefreshAdaptive) {
            return getBaseTableChangedDeltaAdaptive(baseTableInfo, tableDeltaTraits, maxTvrDelta);
        } else {
            logger.info("Base table: {}, db: {}, use the max tvr delta: {} for sync refresh",
                    baseTableInfo.getTableName(), baseTableInfo.getDbName(), maxTvrDelta);
            return maxTvrDelta;
        }
    }

    private boolean isIVMRefreshAdaptive() {
        if (Config.mv_max_rows_per_refresh <= 0) {
            return false;
        }
        // for sync refresh, we always use the max changed delta
        ExecuteOption executeOption = mvContext.getExecuteOption();
        boolean isSyncRefresh = executeOption != null && executeOption.getIsSync();
        return !isSyncRefresh;
    }

    private TvrTableDelta getMaxBaseTableChangedDelta(BaseTableInfo baseTableInfo,
                                                      IcebergTable icebergTable,
                                                      Map<BaseTableInfo, TvrVersionRange> mvTvrVersionRangeMap) {
        // For now, we always refresh the latest snapshot from the last refresh.
        // current tvr snapshot
        TvrVersionRange currentTvrSnapshot = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getCurrentTvrSnapshot(baseTableInfo.getDbName(), icebergTable);
        if (currentTvrSnapshot == null || !(currentTvrSnapshot instanceof TvrTableSnapshot)) {
            logger.warn("Current tvr snapshot is null for base table: {}, db: {}",
                    baseTableInfo.getTableName(), baseTableInfo.getDbName());
            throw new SemanticException("Current tvr snapshot is null for base table: %s.%s",
                    baseTableInfo.getDbName(), baseTableInfo.getTableName());
        }
        if (currentTvrSnapshot.to.isMax()) {
            throw new SemanticException("Current tvr snapshot is max for base table: %s.%s, "
                            + "this means the table is empty or not ready for refresh",
                    baseTableInfo.getDbName(), baseTableInfo.getTableName());
        }
        TvrVersion currentVersion = currentTvrSnapshot.to;
        if (!mvTvrVersionRangeMap.containsKey(baseTableInfo)) {
            // mv has not refreshed yet, so we need to refresh it
            logger.info("Materialized view {} does not have a tvr version range for base table: {}, "
                            + "current tvr snapshot: {}, so we need to refresh it",
                    mv.getName(), baseTableInfo.getTableName(), currentTvrSnapshot);
            return TvrTableDelta.of(TvrVersion.MIN, currentVersion);
        }

        TvrVersionRange beforeTvrVersionRange = mvTvrVersionRangeMap.get(baseTableInfo);
        logger.info("Base table: {}, before tvr version range: {}, current tvr snapshot: {}",
                baseTableInfo.getTableName(), beforeTvrVersionRange, currentTvrSnapshot);
        if (beforeTvrVersionRange == null || !(beforeTvrVersionRange instanceof TvrTableSnapshot)) {
            throw new SemanticException("Materialized view " + mv.getName()
                    + " does not have a valid tvr version range for base table: " + baseTableInfo.getTableName());
        }
        TvrVersion beforeVersion = beforeTvrVersionRange.to;
        if (beforeVersion.equals(currentVersion)) {
            // no change, so we can skip the refresh
            logger.info("Base table {} has not changed, skip to refresh", baseTableInfo.getTableName());
            return TvrTableDelta.of(beforeVersion, currentVersion);
        }
        return TvrTableDelta.of(beforeVersion, currentVersion);
    }

    // TODO: We may introduce a smarter way to determine which incremental snapshot to refresh later.
    private TvrTableDelta getBaseTableChangedDeltaAdaptive(BaseTableInfo baseTableInfo,
                                                           List<TvrTableDeltaTrait> tableDeltaTraits,
                                                           TvrTableDelta maxTvrDelta) {
        TvrTableSnapshot fromSnapshot = maxTvrDelta.fromSnapshot();
        TvrTableSnapshot toSnapshot = maxTvrDelta.toSnapshot();
        long addedRows = 0;
        long addedFileSize = 0;
        for (TvrTableDeltaTrait deltaTrait : tableDeltaTraits) {
            // TODO: We may need to handle the case where the deltaTrait is not append-only.
            if (!deltaTrait.isAppendOnly()) {
                throw new SemanticException("TvrTableDeltaTrait is not append-only for base table: %s.%s",
                        baseTableInfo.getDbName(), baseTableInfo.getTableName());
            }
            addedRows += deltaTrait.getTvrDeltaStats().getAddedRows();
            addedFileSize += deltaTrait.getTvrDeltaStats().getAddedFileSize();

            if (addedRows >= Config.mv_max_rows_per_refresh || addedFileSize >= Config.mv_max_bytes_per_refresh) {
                logger.info("Base table: {}, db: {}, added rows: {}, added file size:{}, snapshot:{}" +
                                "reached the max rows per refresh, stop processing further deltas",
                        baseTableInfo.getTableName(), baseTableInfo.getDbName(), addedRows, addedFileSize, deltaTrait);
                break;
            }
            logger.info("Base table: {}, db: {}, deltaTrait: {}, added rows: {}, fromSnapshot: {}, toSnapshot: {}",
                    baseTableInfo.getTableName(), baseTableInfo.getDbName(), deltaTrait, addedRows,
                    fromSnapshot, toSnapshot);
            toSnapshot = deltaTrait.getTvrDelta().toSnapshot();
        }
        TvrTableDelta result = TvrTableDelta.of(fromSnapshot.to, toSnapshot.to);
        // if the adaptive tvr delta is different from the max tvr delta, generate the next task run
        hasNextTaskRun |= addedRows >= Config.mv_max_rows_per_refresh
                && !toSnapshot.equals(maxTvrDelta.toSnapshot())
                && !toSnapshot.to.isMax();
        logger.info("Base table: {}, db: {}, max tvr delta: {}, adaptive tvr delta: {}, " +
                        "toSnapshot:{}, hasNextTaskRun:{}", baseTableInfo.getTableName(), baseTableInfo.getDbName(),
                maxTvrDelta, result, toSnapshot, hasNextTaskRun);
        return result;
    }

    @Override
    public void generateNextTaskRunIfNeeded() {
        if (!hasNextTaskRun || mvContext.getTaskRun().isKilled()) {
            return;
        }
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Map<String, String> properties = mvContext.getProperties();
        long mvId = Long.parseLong(properties.get(MV_ID));
        String taskName = TaskBuilder.getMvTaskName(mvId);
        Map<String, String> newProperties = Maps.newHashMap();
        for (Map.Entry<String, String> proEntry : properties.entrySet()) {
            // skip uncopyable properties: force/partition_values/... which only can be set specifically.
            if (proEntry.getKey() == null || proEntry.getValue() == null
                    || MV_UNCOPYABLE_PROPERTIES.contains(proEntry.getKey())) {
                continue;
            }
            newProperties.put(proEntry.getKey(), proEntry.getValue());
        }

        if (mvContext.getStatus() != null) {
            newProperties.put(TaskRun.START_TASK_RUN_ID, mvContext.getStatus().getStartTaskRunId());
        }
        // warehouse
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            newProperties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
        }
        // Partition refreshing task run should have the HIGHER priority, and be scheduled before other tasks
        // Otherwise this round of partition refreshing would be staved and never got finished
        ExecuteOption executeOption = mvContext.getExecuteOption();
        int priority = executeOption.getPriority() > Constants.TaskRunPriority.LOWEST.value() ?
                executeOption.getPriority() : Constants.TaskRunPriority.HIGHER.value();
        ExecuteOption option = new ExecuteOption(priority, true, newProperties);
        logger.info("[MV] Generate a task to refresh next batches of partitions for MV {}-{}, start={}, end={}, " +
                        "priority={}, properties={}", mv.getName(), mv.getId(),
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(), priority, properties);
        if (properties.containsKey(TaskRun.IS_TEST) && properties.get(TaskRun.IS_TEST).equalsIgnoreCase("true")) {
            // for testing
            TaskRun taskRun = TaskRunBuilder
                    .newBuilder(taskManager.getTask(taskName))
                    .properties(option.getTaskRunProperties())
                    .setExecuteOption(option)
                    .build();
            nextTaskRun = taskRun;
        } else {
            taskManager.executeTask(taskName, option);
        }
    }

    private InsertStmt prepareRefreshPlan() throws AnalysisException, LockTimeoutException {
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

        // set tvr target mvid
        ctx.getSessionVariable().setEnableIVMRefresh(true);
        ctx.getSessionVariable().setTvrTargetMvid(GsonUtils.GSON.toJson(mv.getMvId()));

        final Set<Table> baseTables = snapshotBaseTables.values()
                .stream()
                .map(BaseTableSnapshotInfo::getBaseTable)
                .collect(Collectors.toSet());
        changeDefaultConnectContextIfNeeded(ctx, baseTables);

        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshParser")) {
            // generate insert statement from defined query
            insertStmt = generateInsertAst(ctx, Sets.newHashSet(), mv.getIVMTaskDefinition());
        }

        PlannerMetaLocker locker = new PlannerMetaLocker(ctx, insertStmt);
        if (!locker.tryLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database in prepareRefreshPlan");
        }
        try (ConnectContext.ScopeGuard guard = ctx.bindScope()) {
            // analyze the insert statement
            try (Timer ignored = Tracers.watchScope("MVRefreshAnalyzer")) {
                analyzeInsertStmt(insertStmt);
                // build the insert plan
                insertStmt = buildInsertPlan(insertStmt);
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
            }
        } finally {
            locker.unlock();
        }

        try (Timer ignored = Tracers.watchScope("MVRefreshPlanner")) {
            ctx.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(false); //already refreshed before
            ExecPlan execPlan = StatementPlanner.plan(insertStmt, ctx);
            mvContext.setExecPlan(execPlan);
        }
        return insertStmt;
    }

    private void analyzeInsertStmt(InsertStmt insertStmt) throws AnalysisException {
        ConnectContext ctx = mvContext.getCtx();
        Analyzer.analyze(insertStmt, ctx);
    }

    private InsertStmt buildInsertPlan(InsertStmt insertStmt) throws AnalysisException {
        QueryStatement queryStatement = insertStmt.getQueryStatement();
        Multimap<String, TableRelation> tableRelations = AnalyzerUtils.collectAllTableRelation(queryStatement);
        Map<String, TvrVersionRange> baseTableNameToTvrVersionRangeMap = snapshotBaseTables.values()
                .stream()
                .map(snapshotInfo -> (TvrTableSnapshotInfo) snapshotInfo)
                .map(snapshotInfo -> {
                    BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
                    TvrVersionRange tvrVersionRange = snapshotInfo.getTvrSnapshot();
                    if (tvrVersionRange == null) {
                        throw new SemanticException("Base table %s.%s does not have a valid tvr version range",
                                baseTableInfo.getDbName(), baseTableInfo.getTableName());
                    }
                    return Maps.immutableEntry(baseTableInfo.getTableName(), tvrVersionRange);
                })
                .collect(Collectors.toMap(entry -> entry.getKey(), Map.Entry::getValue));
        for (Map.Entry<String, TableRelation> entry : tableRelations.entries()) {
            TableRelation tableRelation = entry.getValue();
            Table table = tableRelation.getTable();
            if (!baseTableNameToTvrVersionRangeMap.containsKey(table.getName())) {
                throw new SemanticException("Base table %s.%s is not found in the changed version ranges",
                        tableRelation.getName().getDb(), tableRelation.getName().getTbl());
            }
            TvrVersionRange tvrVersionRange = baseTableNameToTvrVersionRangeMap.get(table.getName());
            tableRelation.setTvrVersionRange(tvrVersionRange);
        }
        return insertStmt;
    }

    @Override
    protected BaseTableSnapshotInfo buildBaseTableSnapshotInfo(BaseTableInfo baseTableInfo, Table table) {
        return new TvrTableSnapshotInfo(baseTableInfo, table);
    }
}
