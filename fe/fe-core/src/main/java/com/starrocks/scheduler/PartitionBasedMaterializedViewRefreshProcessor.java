// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.google.common.annotations.VisibleForTesting;
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
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.planner.HdfsScanNode;
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
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
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
import java.util.Optional;
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
    private Map<Long, Pair<BaseTableInfo, Table>> snapshotBaseTables;

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

                // refresh external table meta cache before check the partition changed
                refreshExternalTable(context);
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
                TableProperty mvTableProperty = materializedView.getTableProperty();
                Set<String> partitionsToRefresh = getPartitionsToRefreshForMaterializedView(context.getProperties(),
                        mvTableProperty);
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
                mvContext.setExecPlan(execPlan);
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
        if (partitionNameIter.hasNext()) {
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
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            BaseTableInfo baseTableInfo = tablePair.first;
            Table table = tablePair.second;
            if (!table.isLocalTable()) {
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
            if (snapshotTable.isOlapOrLakeTable()) {
                OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                currentTablePartitionInfo.keySet().removeIf(partitionName ->
                        !snapshotOlapTable.getPartitionNames().contains(partitionName));
            }
        }
        if (!changedTablePartitionInfos.isEmpty()) {
            ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                    new ChangeMaterializedViewRefreshSchemeLog(materializedView);
            GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
        }
    }

    private void updateMetaForExternalTable(MaterializedView.AsyncRefreshContext refreshContext, ExecPlan execPlan) {
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableInfoVisibleVersionMap();
        // should write this to log at one time
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> changedTablePartitionInfos =
                getSourceTableInfoPartitionInfos(execPlan);
        // update version map of materialized view
        for (Map.Entry<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> tableEntry
                : changedTablePartitionInfos.entrySet()) {
            BaseTableInfo baseTableInfo = tableEntry.getKey();
            if (!currentVersionMap.containsKey(baseTableInfo)) {
                currentVersionMap.put(baseTableInfo, Maps.newHashMap());
            }
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.get(baseTableInfo);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = tableEntry.getValue();
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // remove partition info of not-exist partition for snapshot table from version map
            Set<String> partitionNames = Sets.newHashSet(PartitionUtil.getPartitionNames(baseTableInfo.getTable()));
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

    private void syncPartitionsForExpr() {
        Expr partitionExpr = MaterializedView.getPartitionExpr(materializedView);
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
                String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
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
        if (table.isLocalTable() || table.isHiveTable() || table.isLakeTable()) {
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
    public Set<String> getPartitionsToRefreshForMaterializedView(Map<String, String> taskProperty,
                                                                 TableProperty mvTableProperty)
            throws AnalysisException {
        String start = taskProperty.get(TaskRun.PARTITION_START);
        String end = taskProperty.get(TaskRun.PARTITION_END);
        boolean force = Boolean.parseBoolean(taskProperty.get(TaskRun.FORCE));
        int partitionTTLNumber = mvTableProperty.getPartitionTTLNumber();
        if (force && start == null && end == null) {
            return materializedView.getValidPartitionMap(partitionTTLNumber).keySet();
        }
        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
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
                    return materializedView.getValidPartitionMap(partitionTTLNumber).keySet();
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
                if (table.isLocalTable()) {
                    tableNamePartitionNames.put(table.getName(), ((OlapTable) table).getPartitionNames());
                }
            }
        }
        return tableNamePartitionNames;
    }

    private ExecPlan generateRefreshPlan(ConnectContext ctx, InsertStmt insertStmt) throws AnalysisException {
        return StatementPlanner.plan(insertStmt, ctx);
    }

    @VisibleForTesting
    public InsertStmt generateInsertStmt(Set<String> materializedViewPartitions,
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
        // insert overwrite mv must set system = true
        insertStmt.setSystem(true);
        Analyzer.analyze(insertStmt, ctx);
        // after analyze, we could get the table meta info of the tableRelation.
        QueryStatement queryStatement = insertStmt.getQueryStatement();
        Map<String, TableRelation> tableRelations =
                AnalyzerUtils.collectAllTableRelation(queryStatement);
        for (Map.Entry<String, TableRelation> nameTableRelationEntry : tableRelations.entrySet()) {
            Set<String> tablePartitionNames = sourceTablePartitions.get(nameTableRelationEntry.getKey());
            TableRelation tableRelation = nameTableRelationEntry.getValue();
            tableRelation.setPartitionNames(
                    new PartitionNames(false, tablePartitionNames == null ? null :
                            new ArrayList<>(tablePartitionNames)));
            // generate partition predicate for external table because it can not use partition names
            // to scan partial partitions
            Table table = tableRelation.getTable();
            if (tablePartitionNames != null && !table.isLocalTable()) {
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
                        PartitionInfo mvPartitionInfo = materializedView.getPartitionInfo();
                        // do not need to check base partition table changed when mv is not partitioned
                        if  (!(mvPartitionInfo instanceof ExpressionRangePartitionInfo)) {
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
                        if  (!(mvPartitionInfo instanceof ExpressionRangePartitionInfo)) {
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
            partitionInfos.put(selectedPartitionNames.get(index),
                    new MaterializedView.BasePartitionInfo(-1, hivePartitions.get(index).getModifiedTime()));
        }
        return partitionInfos;
    }
}
