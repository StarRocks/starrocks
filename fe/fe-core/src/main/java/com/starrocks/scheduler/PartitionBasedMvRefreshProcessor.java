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

package com.starrocks.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Core logic of materialized view refresh task run
 * PartitionBasedMvRefreshProcessor is not thread safe for concurrent runs of the same materialized view
 */
public class PartitionBasedMvRefreshProcessor extends BaseTaskRunProcessor {

    private static final Logger LOG = LogManager.getLogger(PartitionBasedMvRefreshProcessor.class);
    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);

    public static final String MV_ID = "mvId";

    private static final int MAX_RETRY_NUM = 10;

    private Database database;
    private MaterializedView materializedView;
    private MvTaskRunContext mvContext;
    // table id -> <base table info, snapshot table>
    private Map<Long, Pair<BaseTableInfo, Table>> snapshotBaseTables;

    private long oldTransactionVisibleWaitTimeout;

    @VisibleForTesting
    public MvTaskRunContext getMvContext() {
        return mvContext;
    }

    @VisibleForTesting
    public void setMvContext(MvTaskRunContext mvContext) {
        this.mvContext = mvContext;
    }

    // Core logics:
    // 1. prepare to check some conditions
    // 2. sync partitions with base tables(add or drop partitions, which will be optimized  by dynamic partition creation later)
    // 3. decide which partitions of materialized view to refresh and the corresponding base tables' source partitions
    // 4. construct the refresh sql and execute it
    // 5. update the source table version map if refresh task completes successfully
    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        prepare(context);

        ConnectContext connectContext = context.getCtx();
        try {
            doMvRefresh(context);
            connectContext.getState().setOk();
        } catch (Throwable e) {
            connectContext.getState().setError(e.getMessage());
            throw e;
        } finally {
            postProcess();
        }
    }

    private void doMvRefresh(TaskRunContext context) throws Exception {
        int retryNum = 0;

        Set<String> partitionsToRefresh = null;
        Map<String, Set<String>> sourceTablePartitions = null;
        boolean checked = false;
        while (!checked) {
            // sync partitions between materialized view and base tables out of lock
            // do it outside lock because it is a time-cost operation
            syncPartitions(context);
            // refresh external table meta cache before check the partition changed
            refreshExternalTable(context);
            database.readLock();
            try {
                // the following steps should be done in the same lock:
                // 1. check base table partitions change
                // 2. get affected materialized view partitions
                // 3. generate insert stmt
                // 4. generate insert ExecPlan

                // check whether there are partition changes for base tables, eg: partition rename
                // retry to sync partitions if any base table changed the partition infos
                if (checkBaseTablePartitionChange()) {
                    retryNum++;
                    if (retryNum > MAX_RETRY_NUM) {
                        throw new DmlException("materialized view:%s refresh task failed", materializedView.getName());
                    }
                    LOG.info("materialized view:{} base partition has changed. retry to sync partitions, retryNum:{}",
                            materializedView.getName(), retryNum);
                    continue;
                }
                checked = true;
                partitionsToRefresh = getPartitionsToRefreshForMaterializedView(context.getProperties());
                if (partitionsToRefresh.isEmpty()) {
                    LOG.info("no partitions to refresh for materialized view {}, query_id:{}",
                            materializedView.getName(), mvContext.getCtx().getQueryId());
                    return;
                }
                // Only refresh the first partition refresh number partitions, other partitions will generate new tasks
                filterPartitionByRefreshNumber(partitionsToRefresh, materializedView);

                LOG.debug("materialized view partitions to refresh:{}", partitionsToRefresh);
                sourceTablePartitions = getSourceTablePartitions(partitionsToRefresh);
                LOG.debug("materialized view:{} source partitions :{}",
                        materializedView.getName(), sourceTablePartitions);
                if (this.getMVTaskRunExtraMessage() != null) {
                    MVTaskRunExtraMessage extraMessage = getMVTaskRunExtraMessage();
                    extraMessage.setMvPartitionsToRefresh(partitionsToRefresh);
                    extraMessage.setBasePartitionsToRefreshMap(sourceTablePartitions);
                }
            } finally {
                database.readUnlock();
            }
        }

        InsertStmt insertStmt = prepareRefreshPlan(partitionsToRefresh, sourceTablePartitions);
        // execute the ExecPlan of insert outside lock
        refreshMaterializedView(mvContext, mvContext.getExecPlan(), insertStmt);

        // insert execute successfully, update the meta of materialized view according to ExecPlan
        updateMeta(mvContext.getExecPlan());

        if (mvContext.hasNextBatchPartition()) {
            generateNextTaskRun();
        }
    }

    /**
     * Prepare the statement and plan for mv refreshing, considering the partitions of ref table
     */
    private InsertStmt prepareRefreshPlan(Set<String> mvToRefreshedPartitions,
                                          Map<String, Set<String>> refTablePartitionNames) throws AnalysisException {
        // 1. Prepare context
        ConnectContext ctx = mvContext.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase());

        // 3. AST
        InsertStmt insertStmt = generateInsertAst(mvToRefreshedPartitions, materializedView, ctx);

        // 4. Analyze and prepare partition
        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(ctx, insertStmt);
        ExecPlan execPlan = null;
        try (ConnectContext.ScopeGuard guard = ctx.bindScope()) {
            StatementPlanner.lock(dbs);

            insertStmt =
                    analyzeInsertStmt(insertStmt, mvToRefreshedPartitions, refTablePartitionNames, materializedView);
            execPlan = StatementPlanner.plan(insertStmt, ctx);
            if (mvContext.getCtx().getSessionVariable().isEnableOptimizerTraceLog()) {
                StringBuffer sb = new StringBuffer();
                sb.append(String.format("[TRACE QUERY] MV: %s\n", materializedView.getName()));
                sb.append(String.format("MV PartitionsToRefresh: %s \n", Joiner.on(",").join(mvToRefreshedPartitions)));
                sb.append(String.format("Base PartitionsToScan:%s\n", refTablePartitionNames));
                sb.append("Insert Plan:\n");
                sb.append(execPlan.getExplainString(StatementBase.ExplainLevel.VERBOSE));
                LOG.info(sb.toString());
            }
            LOG.info("[MV FINAL PLAN] Materialized view {} partitions to refresh:{}, ref base partitions:{}, query_id:{}",
                    materializedView.getName(), Joiner.on(",").join(mvToRefreshedPartitions, ctx.getQueryId()),
                    refTablePartitionNames);
        } catch (Throwable e) {
            LOG.warn("prepareRefreshPlan for mv {} failed", materializedView.getName(), e);
            throw e;
        } finally {
            StatementPlanner.unLock(dbs);
        }

        if (mvContext.getCtx().getSessionVariable().isEnableOptimizerTraceLog()) {
            StringBuffer sb = new StringBuffer();
            sb.append(String.format("[TRACE QUERY] MV: %s\n", materializedView.getName()));
            sb.append(String.format("MV PartitionsToRefresh: %s \n", Joiner.on(",").join(mvToRefreshedPartitions)));
            if (refTablePartitionNames != null) {
                sb.append(String.format("Base PartitionsToScan:%s\n", refTablePartitionNames));
            }
            sb.append("Insert Plan:\n");
            sb.append(execPlan.getExplainString(StatementBase.ExplainLevel.VERBOSE));
            LOG.info(sb.toString());
        }

        mvContext.setExecPlan(execPlan);

        return insertStmt;
    }

    private void postProcess() {
        mvContext.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(oldTransactionVisibleWaitTimeout);
    }

    public MVTaskRunExtraMessage getMVTaskRunExtraMessage() {
        if (this.mvContext.status == null) {
            return null;
        }
        return this.mvContext.status.getMvTaskRunExtraMessage();
    }

    @VisibleForTesting
    public void filterPartitionByRefreshNumber(Set<String> partitionsToRefresh, MaterializedView materializedView) {
        int partitionRefreshNumber = materializedView.getTableProperty().getPartitionRefreshNumber();
        if (partitionRefreshNumber <= 0) {
            return;
        }
        Map<String, Range<PartitionKey>> rangePartitionMap = materializedView.getRangePartitionMap();
        if (partitionRefreshNumber >= rangePartitionMap.size()) {
            return;
        }
        Map<String, Range<PartitionKey>> mappedPartitionsToRefresh = Maps.newHashMap();
        for (String partitionName : partitionsToRefresh) {
            mappedPartitionsToRefresh.put(partitionName, rangePartitionMap.get(partitionName));
        }
        LinkedHashMap<String, Range<PartitionKey>> sortedPartition = mappedPartitionsToRefresh.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(RangeUtils.RANGE_COMPARATOR))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        Iterator<String> partitionNameIter = sortedPartition.keySet().iterator();
        for (int i = 0; i < partitionRefreshNumber; i++) {
            if (partitionNameIter.hasNext()) {
                partitionNameIter.next();
            }
        }
        String nextPartitionStart = null;
        String endPartitionName = null;
        if (partitionNameIter.hasNext()) {
            String startPartitionName = partitionNameIter.next();
            Range<PartitionKey> partitionKeyRange = mappedPartitionsToRefresh.get(startPartitionName);
            LiteralExpr lowerExpr = partitionKeyRange.lowerEndpoint().getKeys().get(0);
            nextPartitionStart = AnalyzerUtils.parseLiteralExprToDateString(lowerExpr, 0);
            endPartitionName = startPartitionName;
            partitionsToRefresh.remove(endPartitionName);
        }
        while (partitionNameIter.hasNext()) {
            endPartitionName = partitionNameIter.next();
            partitionsToRefresh.remove(endPartitionName);
        }

        mvContext.setNextPartitionStart(nextPartitionStart);

        if (endPartitionName != null) {
            LiteralExpr upperExpr = mappedPartitionsToRefresh.get(endPartitionName).upperEndpoint().getKeys().get(0);
            mvContext.setNextPartitionEnd(AnalyzerUtils.parseLiteralExprToDateString(upperExpr, 1));
        } else {
            // partitionNameIter has just been traversed, and endPartitionName is not updated
            // will cause endPartitionName == null
            mvContext.setNextPartitionEnd(null);
        }
    }

    private void generateNextTaskRun() {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Map<String, String> properties = mvContext.getProperties();
        long mvId = Long.parseLong(properties.get(MV_ID));
        String taskName = TaskBuilder.getMvTaskName(mvId);
        Map<String, String> newProperties = Maps.newHashMap();
        for (Map.Entry<String, String> proEntry : properties.entrySet()) {
            if (proEntry.getValue() != null) {
                newProperties.put(proEntry.getKey(), proEntry.getValue());
            }
        }
        newProperties.put(TaskRun.PARTITION_START, mvContext.getNextPartitionStart());
        newProperties.put(TaskRun.PARTITION_END, mvContext.getNextPartitionEnd());
        // Partition refreshing task run should have the HIGHEST priority, and be scheduled before other tasks
        // Otherwise this round of partition refreshing would be staved and never got finished
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.HIGHEST.value(), true, newProperties);
        taskManager.executeTask(taskName, option);
        LOG.info("[MV] Generate a task to refresh next batches of partitions for MV {}-{}, start={}, end={}",
                materializedView.getName(), materializedView.getId(),
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd());
    }

    private void refreshExternalTable(TaskRunContext context) {
        List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Database db = baseTableInfo.getDb();
            if (db == null) {
                LOG.warn("database {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getDbInfoStr(), materializedView.getName());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }

            Table table = baseTableInfo.getTable();
            if (table == null) {
                LOG.warn("table {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getTableInfoStr(), materializedView.getName());
                LocalMetastore.inactiveRelatedMaterializedView(db, materializedView,
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            if (!table.isNativeTableOrMaterializedView()) {
                context.getCtx().getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, Lists.newArrayList(), true);
            }
        }
    }

    private void updateMeta(ExecPlan execPlan) {
        // update the meta if succeed
        if (!database.writeLockAndCheckExist()) {
            throw new DmlException("update meta failed. database:" + database.getFullName() + " not exist");
        }
        try {
            // check
            Table mv = database.getTable(materializedView.getId());
            if (mv == null) {
                throw new DmlException(
                        "update meta failed. materialized view:" + materializedView.getName() + " not exist");
            }
            MaterializedView.AsyncRefreshContext refreshContext =
                    materializedView.getRefreshScheme().getAsyncRefreshContext();

            updateMetaForOlapTable(refreshContext, execPlan);
            updateMetaForExternalTable(refreshContext, execPlan);
        } finally {
            database.writeUnlock();
        }
    }

    private void updateMetaForOlapTable(MaterializedView.AsyncRefreshContext refreshContext, ExecPlan execPlan) {
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableVisibleVersionMap();
        // should write this to log at one time
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> changedTablePartitionInfos =
                getSourceTablePartitionInfos(execPlan);

        Table partitionTable = null;
        if (mvContext.hasNextBatchPartition()) {
            Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
            partitionTable = partitionTableAndColumn.first;
        }
        // update version map of materialized view
        for (Map.Entry<Long, Map<String, MaterializedView.BasePartitionInfo>> tableEntry
                : changedTablePartitionInfos.entrySet()) {
            Long tableId = tableEntry.getKey();
            if (partitionTable != null && tableId != partitionTable.getId()) {
                continue;
            }
            currentVersionMap.computeIfAbsent(tableId, (v) -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.get(tableId);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = tableEntry.getValue();
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // remove partition info of not-exist partition for snapshot table from version map
            Table snapshotTable = Preconditions.checkNotNull(snapshotBaseTables.get(tableId),
                    "base table not found: " + tableId).second;
            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                currentTablePartitionInfo.keySet().removeIf(partitionName ->
                        !snapshotOlapTable.getPartitionNames().contains(partitionName));
            }
        }
        if (!changedTablePartitionInfos.isEmpty()) {
            ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                    new ChangeMaterializedViewRefreshSchemeLog(materializedView);
            GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);

            // TODO: To be simple, MV's refresh time is defined by max(baseTables' refresh time) which
            // is not very correct, because it may be not monotonically increasing.
            long maxChangedTableRefreshTime = changedTablePartitionInfos.values().stream()
                    .map(x -> x.values().stream().map(
                            MaterializedView.BasePartitionInfo::getLastRefreshTime).max(Long::compareTo))
                    .map(x -> x.orElse(null)).filter(Objects::nonNull)
                    .max(Long::compareTo)
                    .orElse(System.currentTimeMillis());
            materializedView.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
        }
    }

    private void updateMetaForExternalTable(MaterializedView.AsyncRefreshContext refreshContext, ExecPlan execPlan) {
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableInfoVisibleVersionMap();
        // should write this to log at one time
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> changedTablePartitionInfos =
                getSourceTableInfoPartitionInfos(execPlan);

        BaseTableInfo partitionTableInfo = null;
        if (mvContext.hasNextBatchPartition()) {
            Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
            partitionTableInfo =
                    Preconditions.checkNotNull(snapshotBaseTables.get(partitionTableAndColumn.first.getId()),
                            "base table not found: " + partitionTableAndColumn.first.getId())
                            .first;
        }
        // update version map of materialized view
        for (Map.Entry<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> tableEntry
                : changedTablePartitionInfos.entrySet()) {
            BaseTableInfo baseTableInfo = tableEntry.getKey();
            if (partitionTableInfo != null && !partitionTableInfo.equals(baseTableInfo)) {
                continue;
            }
            currentVersionMap.computeIfAbsent(baseTableInfo, (v) -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.get(baseTableInfo);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = tableEntry.getValue();
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // remove partition info of not-exist partition for snapshot table from version map
            Set<String> partitionNames =
                    Sets.newHashSet(PartitionUtil.getPartitionNames(baseTableInfo.getTableChecked()));
            currentTablePartitionInfo.keySet().removeIf(partitionName ->
                    !partitionNames.contains(partitionName));
        }
        if (!changedTablePartitionInfos.isEmpty()) {
            ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                    new ChangeMaterializedViewRefreshSchemeLog(materializedView);
            GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
        }
    }

    private void prepare(TaskRunContext context) {
        Map<String, String> properties = context.getProperties();
        // NOTE: mvId is set in Task's properties when creating
        long mvId = Long.parseLong(properties.get(MV_ID));
        database = GlobalStateMgr.getCurrentState().getDb(context.ctx.getDatabase());
        if (database == null) {
            LOG.warn("database {} do not exist when refreshing materialized view:{}", context.ctx.getDatabase(), mvId);
            throw new DmlException("database " + context.ctx.getDatabase() + " do not exist.");
        }
        Table table = database.getTable(mvId);
        if (table == null) {
            LOG.warn("materialized view:{} in database:{} do not exist when refreshing", mvId,
                    context.ctx.getDatabase());
            throw new DmlException("database " + context.ctx.getDatabase() + " do not exist.");
        }
        materializedView = (MaterializedView) table;

        // try to activate the mv before refresh
        if (!materializedView.isActive()) {
            MVActiveChecker.tryToActivate(materializedView);
            LOG.info("Activated the MV before refreshing: {}", materializedView.getName());
        }
        if (!materializedView.isActive()) {
            String errorMsg = String.format("Materialized view: %s/%d is not active",
                    materializedView.getName(), mvId);
            LOG.warn(errorMsg);
            throw new DmlException(errorMsg);
        }
        // wait util transaction is visible for mv refresh task
        // because mv will update base tables' visible version after insert, the mv's visible version
        // should keep up with the base tables, or it will return outdated result.
        oldTransactionVisibleWaitTimeout = context.ctx.getSessionVariable().getTransactionVisibleWaitTimeout();
        context.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(Long.MAX_VALUE / 1000);
        mvContext = new MvTaskRunContext(context);
    }

    private void syncPartitions(TaskRunContext context) {
        snapshotBaseTables = collectBaseTables(materializedView);
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            syncPartitionsForExpr(context);
        }
    }

    private Pair<Table, Column> getPartitionTableAndColumn(
            Map<Long, Pair<BaseTableInfo, Table>> tables) {
        SlotRef slotRef = MaterializedView.getPartitionSlotRef(materializedView);
        for (Pair<BaseTableInfo, Table> tableInfo : tables.values()) {
            BaseTableInfo baseTableInfo = tableInfo.first;
            Table table = tableInfo.second;
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(baseTableInfo.getTableName())) {
                return Pair.create(table, table.getColumn(slotRef.getColumnName()));
            }
        }
        return Pair.create(null, null);
    }

    private void syncPartitionsForExpr(TaskRunContext context) {
        Expr partitionExpr = MaterializedView.getPartitionExpr(materializedView);
        Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
        Table partitionBaseTable = partitionTableAndColumn.first;
        Preconditions.checkNotNull(partitionBaseTable);
        Column partitionColumn = partitionTableAndColumn.second;
        Preconditions.checkNotNull(partitionColumn);

        PartitionDiff partitionDiff = new PartitionDiff();
        Map<String, Range<PartitionKey>> basePartitionMap;

        int partitionTTLNumber = materializedView.getTableProperty().getPartitionTTLNumber();
        mvContext.setPartitionTTLNumber(partitionTTLNumber);
        Map<String, Range<PartitionKey>> mvPartitionMap = materializedView.getRangePartitionMap();
        database.readLock();
        try {
            basePartitionMap = PartitionUtil.getPartitionRange(partitionBaseTable, partitionColumn);
            if (partitionExpr instanceof SlotRef) {
                partitionDiff = SyncPartitionUtils.calcSyncSamePartition(basePartitionMap, mvPartitionMap);
            } else if (partitionExpr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
                String start = context.getProperties().get(TaskRun.PARTITION_START);
                String end = context.getProperties().get(TaskRun.PARTITION_END);
                Range<PartitionKey> rangeToInclude = null;
                if (start != null || end != null) {
                    rangeToInclude = SyncPartitionUtils.createRange(start, end, partitionColumn);
                }
                String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
                partitionDiff = SyncPartitionUtils.calcSyncRollupPartition(basePartitionMap, mvPartitionMap,
                        granularity, partitionColumn.getPrimitiveType(), rangeToInclude);
            }
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return;
        } finally {
            database.readUnlock();
        }

        Map<String, Range<PartitionKey>> deletes = partitionDiff.getDeletes();

        // We should delete the old partition first and then add the new one,
        // because the old and new partitions may overlap

        for (Map.Entry<String, Range<PartitionKey>> deleteEntry : deletes.entrySet()) {
            String mvPartitionName = deleteEntry.getKey();
            dropPartition(database, materializedView, mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] delete partitions range [{}]",
                materializedView.getName(), deletes);

        Map<String, String> partitionProperties = getPartitionProperties(materializedView);
        DistributionDesc distributionDesc = getDistributionDesc(materializedView);
        Map<String, Range<PartitionKey>> adds = partitionDiff.getAdds();

        addPartitions(database, materializedView, adds, partitionProperties, distributionDesc);
        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            mvPartitionMap.put(mvPartitionName, addEntry.getValue());
        }

        LOG.info("The process of synchronizing materialized view [{}] add partitions range [{}]",
                materializedView.getName(), adds);

        // used to get partitions to refresh
        Map<String, Set<String>> baseToMvNameRef = SyncPartitionUtils
                .generatePartitionRefMap(basePartitionMap, mvPartitionMap);
        Map<String, Set<String>> mvToBaseNameRef = SyncPartitionUtils
                .generatePartitionRefMap(mvPartitionMap, basePartitionMap);
        mvContext.setBaseToMvNameRef(baseToMvNameRef);
        mvContext.setMvToBaseNameRef(mvToBaseNameRef);
        mvContext.setBasePartitionMap(basePartitionMap);
    }

    private boolean needToRefreshTable(Table table) {
        return CollectionUtils.isNotEmpty(materializedView.getUpdatedPartitionNamesOfTable(table));
    }

    private boolean needToRefreshNonPartitionTable(Table partitionTable) {
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table snapshotTable = tablePair.second;
            if (snapshotTable.getId() == partitionTable.getId()) {
                continue;
            }
            if (!supportRefreshByPartition(snapshotTable)) {
                continue;
            }
            if (needToRefreshTable(snapshotTable)) {
                return true;
            }
        }
        return false;
    }

    private boolean supportRefreshByPartition(Table table) {
        if (table.isOlapTableOrMaterializedView() || table.isHiveTable() || table.isCloudNativeTable()) {
            return true;
        }
        return false;
    }

    private boolean unPartitionedMVNeedToRefresh() {
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table snapshotTable = tablePair.second;
            if (!supportRefreshByPartition(snapshotTable)) {
                return true;
            }
            if (needToRefreshTable(snapshotTable)) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    public Set<String> getPartitionsToRefreshForMaterializedView(Map<String, String> properties)
            throws AnalysisException {
        String start = properties.get(TaskRun.PARTITION_START);
        String end = properties.get(TaskRun.PARTITION_END);
        boolean force = Boolean.parseBoolean(properties.get(TaskRun.FORCE));
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        Set<String> needRefreshMvPartitionNames = getPartitionsToRefreshForMaterializedView(partitionInfo,
                start, end, force);
        // update stats
        if (this.getMVTaskRunExtraMessage() != null) {
            MVTaskRunExtraMessage extraMessage = this.getMVTaskRunExtraMessage();
            extraMessage.setForceRefresh(force);
            extraMessage.setPartitionStart(start);
            extraMessage.setPartitionEnd(end);
        }
        return needRefreshMvPartitionNames;
    }

    private Set<String> getPartitionsToRefreshForMaterializedView(PartitionInfo partitionInfo,
                                                                  String start,
                                                                  String end,
                                                                  boolean force) throws AnalysisException {
        int partitionTTLNumber = mvContext.getPartitionTTLNumber();
        if (force && start == null && end == null) {
            if (partitionInfo instanceof SinglePartitionInfo) {
                return materializedView.getPartitionNames();
            } else {
                return materializedView.getValidPartitionMap(partitionTTLNumber).keySet();
            }
        }

        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();
        if (partitionInfo instanceof SinglePartitionInfo) {
            // for non-partitioned materialized view
            if (force || unPartitionedMVNeedToRefresh()) {
                return materializedView.getPartitionNames();
            }
        } else if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            Expr partitionExpr = MaterializedView.getPartitionExpr(materializedView);
            Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
            Table partitionTable = partitionTableAndColumn.first;

            boolean isAutoRefresh = (mvContext.type == Constants.TaskType.PERIODICAL ||
                    mvContext.type == Constants.TaskType.EVENT_TRIGGERED);
            Set<String> mvRangePartitionNames = SyncPartitionUtils.getPartitionNamesByRangeWithPartitionLimit(
                    materializedView, start, end, partitionTTLNumber, isAutoRefresh);

            if (needToRefreshNonPartitionTable(partitionTable)) {
                if (start == null && end == null) {
                    // if non partition table changed, should refresh all partitions of materialized view
                    return mvRangePartitionNames;
                } else {
                    // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                    // it should be refreshed according to the user-specified range, not all partitions.
                    return getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(partitionTable,
                            mvRangePartitionNames, true);
                }
            }
            // check partition table
            if (partitionExpr instanceof SlotRef) {
                return getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(partitionTable, mvRangePartitionNames,
                        force);
            } else if (partitionExpr instanceof FunctionCallExpr) {
                needRefreshMvPartitionNames = getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(partitionTable,
                        mvRangePartitionNames, force);
                Set<String> baseChangedPartitionNames =
                        getBasePartitionNamesByMVPartitionNames(needRefreshMvPartitionNames);
                // because the relation of partitions between materialized view and base partition table is n : m,
                // should calculate the candidate partitions recursively.
                LOG.debug("Start calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                        " baseChangedPartitionNames: {}", needRefreshMvPartitionNames, baseChangedPartitionNames);
                SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                        mvContext.baseToMvNameRef, mvContext.mvToBaseNameRef);
                LOG.debug("Finish calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                        " baseChangedPartitionNames: {}", needRefreshMvPartitionNames, baseChangedPartitionNames);
            }
        } else {
            throw new DmlException("unsupported partition info type:" + partitionInfo.getClass().getName());
        }
        return needRefreshMvPartitionNames;
    }

    private Set<String> getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(Table partitionTable,
                                                                                  Set<String> mvRangePartitionNames,
                                                                                  boolean force) {
        if (force || !supportRefreshByPartition(partitionTable)) {
            return Sets.newHashSet(mvRangePartitionNames);
        }
        // check if there is a load in the base table and add it to the refresh candidate
        Set<String> updatePartitionNames = materializedView.getUpdatedPartitionNamesOfTable(partitionTable);
        if (updatePartitionNames == null) {
            return mvRangePartitionNames;
        }
        Set<String> result = getMVPartitionNamesByBasePartitionNames(updatePartitionNames);
        result.retainAll(mvRangePartitionNames);
        return result;
    }

    private Set<String> getMVPartitionNamesByBasePartitionNames(Set<String> basePartitionNames) {
        Set<String> result = Sets.newHashSet();
        for (String basePartitionName : basePartitionNames) {
            result.addAll(mvContext.baseToMvNameRef.get(basePartitionName));
        }
        return result;
    }

    private Set<String> getBasePartitionNamesByMVPartitionNames(Set<String> mvPartitionNames) {
        Set<String> result = Sets.newHashSet();
        for (String mvPartitionName : mvPartitionNames) {
            result.addAll(mvContext.mvToBaseNameRef.get(mvPartitionName));
        }
        return result;
    }

    @VisibleForTesting
    public Map<String, Set<String>> getSourceTablePartitions(Set<String> affectedMaterializedViewPartitions) {
        Table partitionTable = null;
        if (materializedView.getPartitionInfo() instanceof ExpressionRangePartitionInfo) {
            Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
            partitionTable = partitionTableAndColumn.first;
        }
        Map<String, Set<String>> tableNamePartitionNames = Maps.newHashMap();
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table table = tablePair.second;

            if (partitionTable != null && partitionTable == table) {
                Set<String> needRefreshTablePartitionNames = Sets.newHashSet();
                Map<String, Set<String>> mvToBaseNameRef = mvContext.getMvToBaseNameRef();
                for (String mvPartitionName : affectedMaterializedViewPartitions) {
                    needRefreshTablePartitionNames.addAll(mvToBaseNameRef.get(mvPartitionName));
                }
                tableNamePartitionNames.put(table.getName(), needRefreshTablePartitionNames);
            } else {
                if (table.isNativeTableOrMaterializedView()) {
                    tableNamePartitionNames.put(table.getName(), ((OlapTable) table).getPartitionNames());
                }
            }
        }
        return tableNamePartitionNames;
    }

    private ExecPlan generateRefreshPlan(ConnectContext ctx, InsertStmt insertStmt) throws AnalysisException {
        return StatementPlanner.plan(insertStmt, ctx);
    }

    /**
     * Build an AST for insert stmt
     */
    private InsertStmt generateInsertAst(Set<String> materializedViewPartitions, MaterializedView materializedView,
                                         ConnectContext ctx) {
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase());
        ctx.getPlannerProfile().reset();
        ctx.getSessionVariable().setEnableMaterializedViewRewrite(false);
        String definition = mvContext.getDefinition();
        InsertStmt insertStmt =
                (InsertStmt) SqlParser.parse(definition, ctx.getSessionVariable()).get(0);
        insertStmt.setTargetPartitionNames(new PartitionNames(false, new ArrayList<>(materializedViewPartitions)));
        // insert overwrite mv must set system = true
        insertStmt.setSystem(true);
        return insertStmt;
    }

    @VisibleForTesting
    public InsertStmt analyzeInsertStmt(InsertStmt insertStmt,
                                        Set<String> materializedViewPartitions,
                                        Map<String, Set<String>> refTableRefreshPartitions,
                                        MaterializedView materializedView) throws AnalysisException {
        Analyzer.analyze(insertStmt, ConnectContext.get());
        // after analyze, we could get the table meta info of the tableRelation.
        QueryStatement queryStatement = insertStmt.getQueryStatement();
        Map<String, TableRelation> tableRelations =
                AnalyzerUtils.collectAllTableRelation(queryStatement);
        for (Map.Entry<String, TableRelation> nameTableRelationEntry : tableRelations.entrySet()) {
            Set<String> tablePartitionNames = refTableRefreshPartitions.get(nameTableRelationEntry.getKey());
            TableRelation tableRelation = nameTableRelationEntry.getValue();
            tableRelation.setPartitionNames(
                    new PartitionNames(false, tablePartitionNames == null ? null :
                            new ArrayList<>(tablePartitionNames)));
            // generate partition predicate for external table because it can not use partition names
            // to scan partial partitions
            Table table = tableRelation.getTable();
            if (tablePartitionNames != null && !table.isNativeTableOrMaterializedView()) {
                generatePartitionPredicate(tablePartitionNames, queryStatement, tableRelation);
            }
        }
        return insertStmt;
    }

    private void generatePartitionPredicate(Set<String> tablePartitionNames, QueryStatement queryStatement,
                                            TableRelation tableRelation) {
        List<Range<PartitionKey>> sourceTablePartitionRange = Lists.newArrayList();
        for (String partitionName : tablePartitionNames) {
            sourceTablePartitionRange.add(mvContext.getBasePartitionMap().get(partitionName));
        }
        sourceTablePartitionRange = MvUtils.mergeRanges(sourceTablePartitionRange);
        SlotRef partitionSlot = MaterializedView.getPartitionSlotRef(materializedView);
        List<String> columnOutputNames = queryStatement.getQueryRelation().getColumnOutputNames();
        List<Expr> outputExpressions = queryStatement.getQueryRelation().getOutputExpression();
        Expr outputPartitionSlot = null;
        for (int i = 0; i < outputExpressions.size(); ++i) {
            if (columnOutputNames.get(i).equalsIgnoreCase(partitionSlot.getColumnName())) {
                outputPartitionSlot = outputExpressions.get(i);
                break;
            }
        }
        if (outputPartitionSlot != null) {
            List<Expr> partitionPredicates =
                    MvUtils.convertRange(outputPartitionSlot, sourceTablePartitionRange);
            // range contains the min value could be null value
            Optional<Range<PartitionKey>> nullRange = sourceTablePartitionRange.stream().
                    filter(range -> range.lowerEndpoint().isMinValue()).findAny();
            if (nullRange.isPresent()) {
                Expr isNullPredicate = new IsNullPredicate(outputPartitionSlot, false);
                partitionPredicates.add(isNullPredicate);
            }
            tableRelation.setPartitionPredicate(Expr.compoundOr(partitionPredicates));
        }
    }

    private boolean checkBaseTablePartitionChange() {
        // check snapshotBaseTables and current tables in catalog
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            BaseTableInfo baseTableInfo = tablePair.first;
            Table snapshotTable = tablePair.second;

            Database db = baseTableInfo.getDb();
            if (db == null) {
                return true;
            }
            db.readLock();
            try {
                Table table = baseTableInfo.getTable();
                if (table == null) {
                    return true;
                }

                if (snapshotTable.isOlapOrCloudNativeTable()) {
                    OlapTable snapShotOlapTable = (OlapTable) snapshotTable;
                    if (snapShotOlapTable.getPartitionInfo() instanceof SinglePartitionInfo) {
                        Set<String> partitionNames = ((OlapTable) table).getPartitionNames();
                        if (!snapShotOlapTable.getPartitionNames().equals(partitionNames)) {
                            // there is partition rename
                            return true;
                        }
                    } else {
                        Map<String, Range<PartitionKey>> snapshotPartitionMap =
                                snapShotOlapTable.getRangePartitionMap();
                        Map<String, Range<PartitionKey>> currentPartitionMap =
                                ((OlapTable) table).getRangePartitionMap();
                        boolean changed =
                                SyncPartitionUtils.hasPartitionChange(snapshotPartitionMap, currentPartitionMap);
                        if (changed) {
                            return true;
                        }
                    }
                } else if (snapshotTable.isHiveTable() || snapshotTable.isHudiTable()) {
                    HiveMetaStoreTable snapShotHMSTable = (HiveMetaStoreTable) snapshotTable;
                    if (snapShotHMSTable.isUnPartitioned()) {
                        if (!((HiveMetaStoreTable) table).isUnPartitioned()) {
                            return true;
                        }
                    } else {
                        PartitionInfo mvPartitionInfo = materializedView.getPartitionInfo();
                        // do not need to check base partition table changed when mv is not partitioned
                        if (!(mvPartitionInfo instanceof ExpressionRangePartitionInfo)) {
                            return false;
                        }

                        Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
                        Column partitionColumn = partitionTableAndColumn.second;
                        // For Non-partition based base table, it's not necessary to check the partition changed.
                        if (!snapshotTable.equals(partitionTableAndColumn.first)
                                || !snapshotTable.containColumn(partitionColumn.getName())) {
                            continue;
                        }

                        Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.
                                getPartitionRange(snapshotTable, partitionColumn);
                        Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.
                                getPartitionRange(table, partitionColumn);
                        boolean changed =
                                SyncPartitionUtils.hasPartitionChange(snapshotPartitionMap, currentPartitionMap);
                        if (changed) {
                            return true;
                        }
                    }
                } else if (snapshotTable.isIcebergTable()) {
                    IcebergTable snapShotIcebergTable = (IcebergTable) snapshotTable;
                    if (snapShotIcebergTable.isUnPartitioned()) {
                        if (!table.isUnPartitioned()) {
                            return true;
                        }
                    } else {
                        PartitionInfo mvPartitionInfo = materializedView.getPartitionInfo();
                        // do not need to check base partition table changed when mv is not partitioned
                        if (!(mvPartitionInfo instanceof ExpressionRangePartitionInfo)) {
                            return false;
                        }

                        Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
                        Column partitionColumn = partitionTableAndColumn.second;
                        // For Non-partition based base table, it's not necessary to check the partition changed.
                        if (!snapshotTable.equals(partitionTableAndColumn.first)
                                || !snapShotIcebergTable.containColumn(partitionColumn.getName())) {
                            continue;
                        }

                        Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.
                                getPartitionRange(snapshotTable, partitionColumn);
                        Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.
                                getPartitionRange(table, partitionColumn);
                        boolean changed =
                                SyncPartitionUtils.hasPartitionChange(snapshotPartitionMap, currentPartitionMap);
                        if (changed) {
                            return true;
                        }
                    }
                }
            } catch (UserException e) {
                LOG.warn("Materialized view compute partition change failed", e);
                return true;
            } finally {
                db.readUnlock();
            }
        }
        return false;
    }

    private Map<Long, Map<String, MaterializedView.BasePartitionInfo>> getSourceTablePartitionInfos(ExecPlan execPlan) {
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> selectedBasePartitionInfos = Maps.newHashMap();
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        for (ScanNode scanNode : scanNodes) {
            if (scanNode instanceof OlapScanNode) {
                OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                Map<String, MaterializedView.BasePartitionInfo> selectedPartitionIdVersions =
                        getSelectedPartitionInfos(olapScanNode);
                OlapTable olapTable = olapScanNode.getOlapTable();
                selectedBasePartitionInfos.put(olapTable.getId(), selectedPartitionIdVersions);
            }
        }
        return selectedBasePartitionInfos;
    }

    private Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> getSourceTableInfoPartitionInfos(
            ExecPlan execPlan) {
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> selectedBasePartitionInfos =
                Maps.newHashMap();
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        for (ScanNode scanNode : scanNodes) {
            if (scanNode instanceof HdfsScanNode) {
                HdfsScanNode hdfsScanNode = (HdfsScanNode) scanNode;
                Table hiveTable = hdfsScanNode.getHiveTable();

                Optional<BaseTableInfo> baseTableInfoOptional = materializedView.getBaseTableInfos().stream().filter(
                                baseTableInfo -> baseTableInfo.getTableIdentifier().equals(hiveTable.getTableIdentifier())).
                        findAny();
                if (!baseTableInfoOptional.isPresent()) {
                    continue;
                }

                Map<String, MaterializedView.BasePartitionInfo> selectedPartitionIdVersions =
                        getSelectedPartitionInfos(hdfsScanNode, baseTableInfoOptional.get());
                selectedBasePartitionInfos.put(baseTableInfoOptional.get(), selectedPartitionIdVersions);
            }
        }
        return selectedBasePartitionInfos;
    }

    @VisibleForTesting
    public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan, InsertStmt insertStmt)
            throws Exception {
        long beginTimeInNanoSecond = TimeUtils.getStartTime();
        Preconditions.checkNotNull(execPlan);
        Preconditions.checkNotNull(insertStmt);
        ConnectContext ctx = mvContext.getCtx();
        StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
        ctx.setExecutor(executor);
        if (ctx.getParent() != null && ctx.getParent().getExecutor() != null) {
            StmtExecutor parentStmtExecutor = ctx.getParent().getExecutor();
            parentStmtExecutor.registerSubStmtExecutor(executor);
        }
        ctx.setStmtId(STMT_ID_GENERATOR.incrementAndGet());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        LOG.info("start to refresh materialized view {}, query_id:{}", materializedView.getName(),
                ctx.getQueryId());
        try {
            executor.handleDMLStmtWithProfile(execPlan, insertStmt, beginTimeInNanoSecond);
        } finally {
            LOG.info("finished to refresh materialized view {}, query_id:{}", materializedView.getName(),
                    ctx.getQueryId());
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
            auditAfterExec(mvContext, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
        }
    }

    @VisibleForTesting
    public Map<Long, Pair<BaseTableInfo, Table>> collectBaseTables(MaterializedView materializedView) {
        Map<Long, Pair<BaseTableInfo, Table>> tables = Maps.newHashMap();
        List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();

        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Database db = baseTableInfo.getDb();
            if (db == null) {
                LOG.warn("database {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getDbInfoStr(), materializedView.getName());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }

            Table table = baseTableInfo.getTable();
            if (table == null) {
                LOG.warn("table {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getTableInfoStr(), materializedView.getName());
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            db.readLock();
            try {
                if (table.isOlapTable()) {
                    Table copied = DeepCopy.copyWithGson(table, OlapTable.class);
                    if (copied == null) {
                        throw new DmlException("Failed to copy olap table: %s", table.getName());
                    }
                    tables.put(table.getId(), Pair.create(baseTableInfo, copied));
                } else if (table.isCloudNativeTable()) {
                    LakeTable copied = DeepCopy.copyWithGson(table, LakeTable.class);
                    if (copied == null) {
                        throw new DmlException("Failed to copy lake table: %s", table.getName());
                    }
                    tables.put(table.getId(), Pair.create(baseTableInfo, copied));
                } else {
                    tables.put(table.getId(), Pair.create(baseTableInfo, table));
                }
            } finally {
                db.readUnlock();
            }
        }

        return tables;
    }

    private Map<String, String> getPartitionProperties(MaterializedView materializedView) {
        Map<String, String> partitionProperties = new HashMap<>(4);
        partitionProperties.put("replication_num",
                String.valueOf(materializedView.getDefaultReplicationNum()));
        partitionProperties.put("storage_medium", materializedView.getStorageMedium());
        String storageCooldownTime =
                materializedView.getTableProperty().getProperties().get("storage_cooldown_time");
        if (storageCooldownTime != null
                && !storageCooldownTime.equals(String.valueOf(DataProperty.MAX_COOLDOWN_TIME_MS))) {
            // cast long str to time str e.g.  '1587473111000' -> '2020-04-21 15:00:00'
            String storageCooldownTimeStr = TimeUtils.longToTimeString(Long.parseLong(storageCooldownTime));
            partitionProperties.put("storage_cooldown_time", storageCooldownTimeStr);
        }
        return partitionProperties;
    }

    private DistributionDesc getDistributionDesc(MaterializedView materializedView) {
        HashDistributionInfo hashDistributionInfo =
                (HashDistributionInfo) materializedView.getDefaultDistributionInfo();
        List<String> distColumnNames = new ArrayList<>();
        for (Column distributionColumn : hashDistributionInfo.getDistributionColumns()) {
            distColumnNames.add(distributionColumn.getName());
        }
        return new HashDistributionDesc(hashDistributionInfo.getBucketNum(), distColumnNames);
    }

    private void addPartitions(Database database, MaterializedView materializedView,
                               Map<String, Range<PartitionKey>> adds, Map<String, String> partitionProperties,
                               DistributionDesc distributionDesc) {
        if (adds.isEmpty()) {
            return;
        }
        List<PartitionDesc> partitionDescs = Lists.newArrayList();

        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            Range<PartitionKey> partitionKeyRange = addEntry.getValue();

            String lowerBound = partitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue();
            String upperBound = partitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue();
            boolean isMaxValue = partitionKeyRange.upperEndpoint().isMaxValue();
            PartitionValue upperPartitionValue;
            if (isMaxValue) {
                upperPartitionValue = PartitionValue.MAX_VALUE;
            } else {
                upperPartitionValue = new PartitionValue(upperBound);
            }
            PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(
                    Collections.singletonList(new PartitionValue(lowerBound)),
                    Collections.singletonList(upperPartitionValue));
            SingleRangePartitionDesc singleRangePartitionDesc =
                    new SingleRangePartitionDesc(false, mvPartitionName, partitionKeyDesc, partitionProperties);
            partitionDescs.add(singleRangePartitionDesc);
        }
        RangePartitionDesc rangePartitionDesc =
                new RangePartitionDesc(materializedView.getPartitionColumnNames(), partitionDescs);

        try {
            GlobalStateMgr.getCurrentState().addPartitions(
                    database, materializedView.getName(),
                    new AddPartitionClause(rangePartitionDesc, distributionDesc,
                            partitionProperties, false));
        } catch (Exception e) {
            throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                    database.getFullName(), materializedView.getName());
        }
    }

    private void dropPartition(Database database, MaterializedView materializedView, String mvPartitionName) {
        String dropPartitionName = materializedView.getPartition(mvPartitionName).getName();
        if (!database.writeLockAndCheckExist()) {
            throw new DmlException("drop partition failed. database:" + database.getFullName() + " not exist");
        }
        try {
            // check
            Table mv = database.getTable(materializedView.getId());
            if (mv == null) {
                throw new DmlException("drop partition failed. mv:" + materializedView.getName() + " not exist");
            }
            Partition mvPartition = mv.getPartition(dropPartitionName);
            if (mvPartition == null) {
                throw new DmlException("drop partition failed. partition:" + dropPartitionName + " not exist");
            }

            GlobalStateMgr.getCurrentState().dropPartition(
                    database, materializedView,
                    new DropPartitionClause(false, dropPartitionName, false, true));
        } catch (Exception e) {
            throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                    database.getFullName(), materializedView.getName());
        } finally {
            database.writeUnlock();
        }
    }

    private Map<String, MaterializedView.BasePartitionInfo> getSelectedPartitionInfos(OlapScanNode olapScanNode) {
        Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
        List<Long> selectedPartitionIds = olapScanNode.getSelectedPartitionIds();
        OlapTable olapTable = olapScanNode.getOlapTable();
        for (long partitionId : selectedPartitionIds) {
            Partition partition = olapTable.getPartition(partitionId);
            MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                    partitionId, partition.getVisibleVersion(), partition.getVisibleVersionTime());
            partitionInfos.put(partition.getName(), basePartitionInfo);
        }
        return partitionInfos;
    }

    private Map<String, MaterializedView.BasePartitionInfo> getSelectedPartitionInfos(HdfsScanNode hdfsScanNode,
                                                                                      BaseTableInfo baseTableInfo) {
        Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();

        HiveTable hiveTable = hdfsScanNode.getHiveTable();
        List<String> partitionColumnNames = hiveTable.getPartitionColumnNames();

        List<String> selectedPartitionNames;
        if (hiveTable.isUnPartitioned()) {
            selectedPartitionNames = Lists.newArrayList(hiveTable.getTableName());
        } else {
            Collection<Long> selectedPartitionIds = hdfsScanNode.getScanNodePredicates().getSelectedPartitionIds();
            List<PartitionKey> selectedPartitionKey = Lists.newArrayList();
            for (Long selectedPartitionId : selectedPartitionIds) {
                selectedPartitionKey
                        .add(hdfsScanNode.getScanNodePredicates().getIdToPartitionKey().get(selectedPartitionId));
            }
            selectedPartitionNames = selectedPartitionKey.stream().map(partitionKey ->
                    PartitionUtil.toHivePartitionName(partitionColumnNames, partitionKey)).collect(Collectors.toList());
        }

        List<com.starrocks.connector.PartitionInfo> hivePartitions = GlobalStateMgr.
                getCurrentState().getMetadataMgr().getPartitions(baseTableInfo.getCatalogName(), hiveTable,
                        selectedPartitionNames);

        for (int index = 0; index < selectedPartitionNames.size(); ++index) {
            long modifiedTime = hivePartitions.get(index).getModifiedTime();
            partitionInfos.put(selectedPartitionNames.get(index),
                    new MaterializedView.BasePartitionInfo(-1, modifiedTime, modifiedTime));
        }
        return partitionInfos;
    }
}
