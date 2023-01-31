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

import com.clearspring.analytics.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Core logic of materialized view refresh task run
 * PartitionBasedMaterializedViewRefreshProcessor is not thread safe for concurrent runs of the same materialized view
 */
public class PartitionBasedMaterializedViewRefreshProcessor extends BaseTaskRunProcessor {

    private static final Logger LOG = LogManager.getLogger(PartitionBasedMaterializedViewRefreshProcessor.class);

    public static final String MV_ID = "mvId";

    private static final int MAX_RETRY_NUM = 10;

    private Database database;
    private MaterializedView materializedView;
    private MvTaskRunContext mvContext;
    // table id -> <base table info, snapshot table>
    private Map<Long, Pair<MaterializedView.BaseTableInfo, Table>> snapshotBaseTables;

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

        InsertStmt insertStmt = null;
        ExecPlan execPlan = null;
        int retryNum = 0;
        boolean checked = false;
        while (!checked) {
            // sync partitions between materialized view and base tables out of lock
            // do it outside lock because it is a time-cost operation
            syncPartitions();

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
                // refresh external table meta cache
                refreshExternalTable(context);
                Set<String> partitionsToRefresh = getPartitionsToRefreshForMaterializedView(context.getProperties());
                if (partitionsToRefresh.isEmpty()) {
                    LOG.info("no partitions to refresh for materialized view {}", materializedView.getName());
                    return;
                }
                // Only refresh the first partition refresh number partitions, other partitions will generate new tasks
                filterPartitionByRefreshNumber(partitionsToRefresh, materializedView);

                LOG.debug("materialized view partitions to refresh:{}", partitionsToRefresh);
                Map<String, Set<String>> sourceTablePartitions = getSourceTablePartitions(partitionsToRefresh);
                LOG.debug("materialized view:{} source partitions :{}",
                        materializedView.getName(), sourceTablePartitions);

                // create ExecPlan
                insertStmt = generateInsertStmt(partitionsToRefresh, sourceTablePartitions);
                execPlan = generateRefreshPlan(mvContext.getCtx(), insertStmt);
            } finally {
                database.readUnlock();
            }
        }

        // execute the ExecPlan of insert outside lock
        refreshMaterializedView(mvContext, execPlan, insertStmt);

        // insert execute successfully, update the meta of materialized view according to ExecPlan
        updateMeta(execPlan);

        if (mvContext.hasNextBatchPartition()) {
            generateNextTaskRun();
        }
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
        if (partitionNameIter.hasNext())  {
            String startPartitionName = partitionNameIter.next();
            Range<PartitionKey> partitionKeyRange = mappedPartitionsToRefresh.get(startPartitionName);
            LiteralExpr lowerExpr = partitionKeyRange.lowerEndpoint().getKeys().get(0);
            nextPartitionStart = AnalyzerUtils.parseLiteralExprToDateString(lowerExpr, 0);
            endPartitionName = startPartitionName;
            partitionsToRefresh.remove(endPartitionName);
        }
        while (partitionNameIter.hasNext())  {
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
        ExecuteOption option = new ExecuteOption(mvContext.getPriority(), false, newProperties);
        taskManager.executeTask(taskName, option);
        LOG.info("Submit a generate taskRun for task:{}, partitionStart:{}, partitionEnd:{}", mvId,
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd());
    }

    private void refreshExternalTable(TaskRunContext context) {
        for (Pair<MaterializedView.BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            MaterializedView.BaseTableInfo baseTableInfo = tablePair.first;
            Table table = tablePair.second;
            if (!table.isLocalTable()) {
                context.getCtx().getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, Lists.newArrayList());
            }
        }
        // External table support cache meta for query level (refer to CachingHiveMetastore::createQueryLevelInstance),
        // we need to invalid these cache after refresh table
        context.getCtx().getGlobalStateMgr().getMetadataMgr().removeQueryMetadata();
        // Refresh table will generate new table, there need to add relatedMaterializedViews for it
        for (Pair<MaterializedView.BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            MaterializedView.BaseTableInfo baseTableInfo = tablePair.first;
            Table table = tablePair.second;
            if (!table.isLocalTable()) {
                baseTableInfo.getTable().addRelatedMaterializedView(materializedView.getMvId());
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
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                    refreshContext.getBaseTableVisibleVersionMap();
            // should write this to log at one time
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> changedTablePartitionInfos =
                    getSourceTablePartitionInfos(execPlan);
            // update version map of materialized view
            for (Map.Entry<Long, Map<String, MaterializedView.BasePartitionInfo>> tableEntry
                    : changedTablePartitionInfos.entrySet()) {
                Long tableId = tableEntry.getKey();
                if (!currentVersionMap.containsKey(tableId)) {
                    currentVersionMap.put(tableId, Maps.newHashMap());
                }
                Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                        currentVersionMap.get(tableId);
                Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = tableEntry.getValue();
                currentTablePartitionInfo.putAll(partitionInfoMap);

                // remove partition info of not-exist partition for snapshot table from version map
                Table snapshotTable = snapshotBaseTables.get(tableId).second;
                if (snapshotTable.isOlapTable()) {
                    OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                    currentTablePartitionInfo.keySet().removeIf(partitionName ->
                            !snapshotOlapTable.getPartitionNames().contains(partitionName));
                }
            }
            ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                    new ChangeMaterializedViewRefreshSchemeLog(materializedView);
            GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
        } finally {
            database.writeUnlock();
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
        if (!materializedView.isActive()) {
            String errorMsg = String.format("Materialized view: %s, id: %d is not active, " +
                    "skip sync partition and data with base tables", materializedView.getName(), mvId);
            LOG.warn(errorMsg);
            throw new DmlException(errorMsg);
        }
        mvContext = new MvTaskRunContext(context);
    }

    private void syncPartitions() {
        snapshotBaseTables = collectBaseTables(materializedView);
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            syncPartitionsForExpr();
        }
    }

    private Expr getPartitionExpr() {
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                ((ExpressionRangePartitionInfo) materializedView.getPartitionInfo());
        // currently, mv only supports one expression
        Preconditions.checkState(expressionRangePartitionInfo.getPartitionExprs().size() == 1);
        return materializedView.getPartitionRefTableExprs().get(0);
    }

    private Pair<Table, Column> getPartitionTableAndColumn(
            Map<Long, Pair<MaterializedView.BaseTableInfo, Table>> tables) {
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr partitionExpr = getPartitionExpr();
        partitionExpr.collect(SlotRef.class, slotRefs);
        // if partitionExpr is FunctionCallExpr, get first SlotRef
        Preconditions.checkState(slotRefs.size() == 1);
        SlotRef slotRef = slotRefs.get(0);
        for (Pair<MaterializedView.BaseTableInfo, Table> tableInfo : tables.values()) {
            MaterializedView.BaseTableInfo baseTableInfo = tableInfo.first;
            Table table = tableInfo.second;
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(baseTableInfo.getTableName())) {
                return Pair.create(table, table.getColumn(slotRef.getColumnName()));
            }
        }
        return Pair.create(null, null);
    }

    private void syncPartitionsForExpr() {
        Expr partitionExpr = getPartitionExpr();
        Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
        Table partitionBaseTable = partitionTableAndColumn.first;
        Preconditions.checkNotNull(partitionBaseTable);
        Column partitionColumn = partitionTableAndColumn.second;
        Preconditions.checkNotNull(partitionColumn);

        PartitionDiff partitionDiff = new PartitionDiff();
        Map<String, Range<PartitionKey>> basePartitionMap;
        Map<String, Range<PartitionKey>> mvPartitionMap = materializedView.getRangePartitionMap();
        database.readLock();
        try {
            basePartitionMap = PartitionUtil.getPartitionRange(partitionBaseTable, partitionColumn);
            if (partitionExpr instanceof SlotRef) {
                partitionDiff = SyncPartitionUtils.calcSyncSamePartition(basePartitionMap, mvPartitionMap);
            } else if (partitionExpr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
                String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue();
                partitionDiff = SyncPartitionUtils.calcSyncRollupPartition(basePartitionMap, mvPartitionMap,
                        granularity, partitionColumn.getPrimitiveType());
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
        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            addPartition(database, materializedView, mvPartitionName,
                    addEntry.getValue(), partitionProperties, distributionDesc);
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
    }

    private boolean needToRefreshTable(Table table) {
        return !materializedView.getUpdatedPartitionNamesOfTable(table).isEmpty();
    }

    private boolean needToRefreshNonPartitionTable(Table partitionTable) {
        for (Pair<MaterializedView.BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table snapshotTable = tablePair.second;
            if (snapshotTable.getId() == partitionTable.getId()) {
                continue;
            }
            // External tables don't need to check here
            if (!snapshotTable.isOlapTable()) {
                continue;
            }
            if (needToRefreshTable(snapshotTable)) {
                return true;
            }
        }
        return false;
    }

    private boolean unPartitionedMVNeedToRefresh() {
        for (Pair<MaterializedView.BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table snapshotTable = tablePair.second;
            // External tables need to refresh, we can't get updated info of external table now.
            if (!snapshotTable.isOlapTable()) {
                return true;
            }
            if (needToRefreshTable(snapshotTable)) {
                return true;
            }
        }
        return false;
    }

    private Set<String> getPartitionsToRefreshForMaterializedView(Map<String, String> properties)
            throws AnalysisException {
        String start = properties.get(TaskRun.PARTITION_START);
        String end = properties.get(TaskRun.PARTITION_END);
        boolean force = Boolean.parseBoolean(properties.get(TaskRun.FORCE));
        if (force && start == null && end == null) {
            return Sets.newHashSet(materializedView.getPartitionNames());
        }
        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            // for non-partitioned materialized view
            if (force || unPartitionedMVNeedToRefresh()) {
                return Sets.newHashSet(materializedView.getPartitionNames());
            }
        } else if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            Expr partitionExpr = getPartitionExpr();
            Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
            Table partitionTable = partitionTableAndColumn.first;
            Set<String> mvRangePartitionNames = SyncPartitionUtils.getPartitionNamesByRange(materializedView, start, end);

            if (needToRefreshNonPartitionTable(partitionTable)) {
                if (start == null && end == null) {
                    // if non partition table changed, should refresh all partitions of materialized view
                    return Sets.newHashSet(materializedView.getPartitionNames());
                } else {
                    // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                    // it should be refreshed according to the user-specified range, not all partitions.
                    return getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(partitionTable,
                            mvRangePartitionNames, true);
                }
            }
            // check partition table
            if (partitionExpr instanceof SlotRef) {
                return getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(partitionTable, mvRangePartitionNames, force);
            } else if (partitionExpr instanceof FunctionCallExpr) {
                needRefreshMvPartitionNames = getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(partitionTable,
                        mvRangePartitionNames, force);
                Set<String> baseChangedPartitionNames = getBasePartitionNamesByMVPartitionNames(needRefreshMvPartitionNames);
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
            Set<String> mvRangePartitionNames, boolean force) {
        if (force || !partitionTable.isOlapTable()) {
            return Sets.newHashSet(mvRangePartitionNames);
        }
        // check if there is a load in the base table and add it to the refresh candidate
        Set<String> result = getMVPartitionNamesByBasePartitionNames(
                materializedView.getUpdatedPartitionNamesOfTable(getPartitionTableAndColumn(snapshotBaseTables).first));
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

    private Map<String, Set<String>> getSourceTablePartitions(Set<String> affectedMaterializedViewPartitions) {
        Table partitionTable = null;
        if (materializedView.getPartitionInfo() instanceof ExpressionRangePartitionInfo) {
            Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
            partitionTable = partitionTableAndColumn.first;
        }
        Map<String, Set<String>> tableNamePartitionNames = Maps.newHashMap();
        for (Pair<MaterializedView.BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table table = tablePair.second;
            if (!table.isLocalTable()) {
                // TODO(ywb) support external table refresh according to partition later
                return tableNamePartitionNames;
            }
            OlapTable olapTable = (OlapTable) table;
            if (partitionTable != null && olapTable.getId() == partitionTable.getId()) {
                Set<String> needRefreshTablePartitionNames = Sets.newHashSet();
                Map<String, Set<String>> mvToBaseNameRef = mvContext.getMvToBaseNameRef();
                for (String mvPartitionName : affectedMaterializedViewPartitions) {
                    needRefreshTablePartitionNames.addAll(mvToBaseNameRef.get(mvPartitionName));
                }
                tableNamePartitionNames.put(olapTable.getName(), needRefreshTablePartitionNames);
            } else {
                tableNamePartitionNames.put(olapTable.getName(), olapTable.getPartitionNames());
            }
        }
        return tableNamePartitionNames;
    }

    private ExecPlan generateRefreshPlan(ConnectContext ctx, InsertStmt insertStmt) throws AnalysisException {
        return StatementPlanner.plan(insertStmt, ctx);
    }

    private InsertStmt generateInsertStmt(Set<String> materializedViewPartitions,
                                          Map<String, Set<String>> sourceTablePartitions) {
        ConnectContext ctx = mvContext.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase());
        ctx.getPlannerProfile().reset();
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().setEnableMaterializedViewRewrite(false);
        String definition = mvContext.getDefinition();
        InsertStmt insertStmt =
                (InsertStmt) SqlParser.parse(definition, ctx.getSessionVariable()).get(0);
        insertStmt.setTargetPartitionNames(new PartitionNames(false, new ArrayList<>(materializedViewPartitions)));
        QueryStatement queryStatement = insertStmt.getQueryStatement();
        Map<String, TableRelation> tableRelations =
                AnalyzerUtils.collectAllTableRelation(queryStatement);
        for (Map.Entry<String, TableRelation> nameTableRelationEntry : tableRelations.entrySet()) {
            Set<String> tablePartitionNames = sourceTablePartitions.get(nameTableRelationEntry.getKey());
            TableRelation tableRelation = nameTableRelationEntry.getValue();
            tableRelation.setPartitionNames(
                    new PartitionNames(false, tablePartitionNames == null ? null :
                            new ArrayList<>(tablePartitionNames)));
        }
        // insert overwrite mv must set system = true
        insertStmt.setSystem(true);
        Analyzer.analyze(insertStmt, ctx);
        return insertStmt;
    }

    private boolean checkBaseTablePartitionChange() {
        // check snapshotBaseTables and current tables in catalog
        for (Pair<MaterializedView.BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            MaterializedView.BaseTableInfo baseTableInfo = tablePair.first;
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
                if (snapshotTable.isOlapTable()) {
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
                        HiveMetaStoreTable currentHMSTable = (HiveMetaStoreTable) table;
                        Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.
                                getPartitionRange(snapshotTable, snapShotHMSTable.getPartitionColumns().get(0));
                        Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.
                                getPartitionRange(table, currentHMSTable.getPartitionColumns().get(0));
                        boolean changed =
                                SyncPartitionUtils.hasPartitionChange(snapshotPartitionMap, currentPartitionMap);
                        if (changed) {
                            return true;
                        }
                    }
                } else if (snapshotTable.isIcebergTable()) {
                    IcebergTable snapShotIcebergTable = (IcebergTable) snapshotTable;
                    if (snapShotIcebergTable.isUnPartitioned()) {
                        if (!((IcebergTable) table).isUnPartitioned()) {
                            return true;
                        }
                    } else {
                        IcebergTable currentIcebergTable = (IcebergTable) table;
                        Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.
                                getPartitionRange(snapshotTable, snapShotIcebergTable.getPartitionColumns().get(0));
                        Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.
                                getPartitionRange(table, currentIcebergTable.getPartitionColumns().get(0));
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

    @VisibleForTesting
    public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan, InsertStmt insertStmt)
            throws Exception {
        Preconditions.checkNotNull(execPlan);
        Preconditions.checkNotNull(insertStmt);
        ConnectContext ctx = mvContext.getCtx();
        StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
        ctx.setExecutor(executor);
        ctx.setStmtId(new AtomicInteger().incrementAndGet());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        try {
            executor.handleDMLStmt(execPlan, insertStmt);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(ctx.getExecutionId());
            auditAfterExec(mvContext, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
        }
    }

    @VisibleForTesting
    public Map<Long, Pair<MaterializedView.BaseTableInfo, Table>> collectBaseTables(MaterializedView materializedView) {
        Map<Long, Pair<MaterializedView.BaseTableInfo, Table>> tables = Maps.newHashMap();
        List<MaterializedView.BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();

        for (MaterializedView.BaseTableInfo baseTableInfo : baseTableInfos) {
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
                    Table copied = new OlapTable();
                    if (!DeepCopy.copy(table, copied, OlapTable.class)) {
                        throw new DmlException("Failed to copy olap table: %s", table.getName());
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
        if (storageCooldownTime != null) {
            partitionProperties.put("storage_cooldown_time", storageCooldownTime);
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

    private void addPartition(Database database, MaterializedView materializedView, String partitionName,
                              Range<PartitionKey> partitionKeyRange, Map<String, String> partitionProperties,
                              DistributionDesc distributionDesc) {
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
                new SingleRangePartitionDesc(false, partitionName, partitionKeyDesc, partitionProperties);
        try {
            GlobalStateMgr.getCurrentState().addPartitions(
                    database, materializedView.getName(),
                    new AddPartitionClause(singleRangePartitionDesc, distributionDesc,
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
        Collection<Long> selectedPartitionIds = olapScanNode.getSelectedPartitionIds();
        Collection<String> selectedPartitionNames = olapScanNode.getSelectedPartitionNames();
        Collection<Long> selectedPartitionVersions = olapScanNode.getSelectedPartitionVersions();
        Iterator<Long> selectPartitionIdIterator = selectedPartitionIds.iterator();
        Iterator<String> selectPartitionNameIterator = selectedPartitionNames.iterator();
        Iterator<Long> selectPartitionVersionIterator = selectedPartitionVersions.iterator();
        while (selectPartitionIdIterator.hasNext()) {
            long partitionId = selectPartitionIdIterator.next();
            String partitionName = selectPartitionNameIterator.next();
            long partitionVersion = selectPartitionVersionIterator.next();
            partitionInfos.put(partitionName, new MaterializedView.BasePartitionInfo(partitionId, partitionVersion));
        }
        return partitionInfos;
    }
}
