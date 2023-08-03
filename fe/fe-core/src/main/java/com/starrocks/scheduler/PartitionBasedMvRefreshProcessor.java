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
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
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
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.PropertyAnalyzer;
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
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
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
import com.starrocks.sql.ast.RandomDistributionDesc;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Core logic of materialized view refresh task run
 * PartitionBasedMvRefreshProcessor is not thread safe for concurrent runs of the same materialized view
 */
public class PartitionBasedMvRefreshProcessor extends BaseTaskRunProcessor {

    private static final Logger LOG = LogManager.getLogger(PartitionBasedMvRefreshProcessor.class);

    public static final String MV_ID = "mvId";

    // session.enable_spill
    public static final String MV_SESSION_ENABLE_SPILL =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.ENABLE_SPILL;
    // session.query_timeout
    public static final String MV_SESSION_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.QUERY_TIMEOUT;
    // default query timeout for mv: 1 hour
    private static final int MV_DEFAULT_QUERY_TIMEOUT = 3600;

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
        try {
            doMvRefresh(context);
        } finally {
            postProcess();
        }
    }

    private void doMvRefresh(TaskRunContext context) throws Exception {
        InsertStmt insertStmt = null;
        ExecPlan execPlan = null;
        int retryNum = 0;
        boolean checked = false;

        Map<Table, Set<String>> refTableRefreshPartitions = null;
        Set<String> mvToRefreshedPartitions = null;
        while (!checked) {
            // sync partitions between materialized view and base tables out of lock
            // do it outside lock because it is a time-cost operation
            syncPartitions();
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
                mvToRefreshedPartitions = getPartitionsToRefreshForMaterializedView(context.getProperties());
                if (mvToRefreshedPartitions.isEmpty()) {
                    LOG.info("no partitions to refresh for materialized view {}", materializedView.getName());
                    return;
                }

                // Only refresh the first partition refresh number partitions, other partitions will generate new tasks
                filterPartitionByRefreshNumber(mvToRefreshedPartitions, materializedView);
                LOG.debug("materialized view partitions to refresh:{}", mvToRefreshedPartitions);

                // Get to refreshed base table partition infos.
                refTableRefreshPartitions = getRefTableRefreshPartitions(mvToRefreshedPartitions);

                Map<String, Set<String>> refTablePartitionNames =
                        refTableRefreshPartitions.entrySet().stream()
                                .collect(Collectors.toMap(x -> x.getKey().getName(), Map.Entry::getValue));
                LOG.debug("materialized view:{} source partitions :{}",
                        materializedView.getName(), refTableRefreshPartitions);

                // add message into information_schema
                if (this.getMVTaskRunExtraMessage() != null) {
                    MVTaskRunExtraMessage extraMessage = getMVTaskRunExtraMessage();
                    extraMessage.setMvPartitionsToRefresh(mvToRefreshedPartitions);
                    extraMessage.setRefBasePartitionsToRefreshMap(refTablePartitionNames);
                }

                // change default connect context for mv.
                changeDefaultConnectContextIfNeeded(mvContext.getCtx());

                // generate insert statement by using incremental base partition info
                insertStmt = generateInsertStmt(mvToRefreshedPartitions, refTablePartitionNames, materializedView);

                // create refresh ExecPlan
                execPlan = generateRefreshPlan(mvContext.getCtx(), insertStmt);

                // add trace info if needed
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
            } catch (Exception e) {
                LOG.warn("Refresh mv {} failed: {}", materializedView.getName(), e);
                throw e;
            } finally {
                database.readUnlock();
            }
        }

        // execute the ExecPlan of insert outside lock
        refreshMaterializedView(mvContext, execPlan, insertStmt);

        // insert execute successfully, update the meta of materialized view according to ExecPlan
        updateMeta(mvToRefreshedPartitions, execPlan, refTableRefreshPartitions);

        if (mvContext.hasNextBatchPartition()) {
            generateNextTaskRun();
        }
    }

    /**
     * Change default connect context when for mv refresh this is because:
     * - MV Refresh may take much resource to load base tables' data into the final materialized view.
     * - Those changes are set by default and also able to be changed by users for their needs.
     * @param mvConnectCtx
     */
    private void changeDefaultConnectContextIfNeeded(ConnectContext mvConnectCtx) {
        // add resource group if resource group is enabled
        TableProperty mvProperty  = materializedView.getTableProperty();
        SessionVariable mvSessionVariable = mvConnectCtx.getSessionVariable();
        if (mvSessionVariable.isEnableResourceGroup()) {
            String rg = mvProperty.getResourceGroup();
            if (rg == null || rg.isEmpty()) {
                rg = ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME;
            }
            mvSessionVariable.setResourceGroup(rg);
        }

        // enable spill by default for mv if spill is not set by default and `session.enable_spill` session variable
        // is not set.
        if (!mvSessionVariable.getEnableSpill() &&
                !mvProperty.getProperties().containsKey(MV_SESSION_ENABLE_SPILL)) {
            mvSessionVariable.setEnableSpill(true);
        }

        // change `query_timeout` to 1 hour by default for better user experience.
        if (!mvProperty.getProperties().containsKey(MV_SESSION_TIMEOUT)) {
            mvSessionVariable.setQueryTimeoutS(MV_DEFAULT_QUERY_TIMEOUT);
        }
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
            nextPartitionStart = AnalyzerUtils.parseLiteralExprToDateString(partitionKeyRange.lowerEndpoint(), 0);
            endPartitionName = startPartitionName;
            partitionsToRefresh.remove(endPartitionName);
        }
        while (partitionNameIter.hasNext())  {
            endPartitionName = partitionNameIter.next();
            partitionsToRefresh.remove(endPartitionName);
        }

        mvContext.setNextPartitionStart(nextPartitionStart);

        if (endPartitionName != null) {
            PartitionKey upperEndpoint = mappedPartitionsToRefresh.get(endPartitionName).upperEndpoint();
            mvContext.setNextPartitionEnd(AnalyzerUtils.parseLiteralExprToDateString(upperEndpoint, 0));
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
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            BaseTableInfo baseTableInfo = tablePair.first;
            Table table = tablePair.second;
            if (!table.isNativeTableOrMaterializedView()) {
                context.getCtx().getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, Lists.newArrayList(), true);
            }
        }
    }

    /**
     * After materialized view is refreshed, update materialized view's meta info to record history refreshes.
     *
     * @param refTableAndPartitionNames : refreshed base table and its partition names mapping.
     */
    private void updateMeta(Set<String> mvRefreshedPartitions,
                            ExecPlan execPlan,
                            Map<Table, Set<String>> refTableAndPartitionNames) {
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
            // check
            if (mvRefreshedPartitions == null || refTableAndPartitionNames == null) {
                return;
            }

            // NOTE: For each task run, ref-base table's incremental partition and all non-ref base tables' partitions
            // are refreshed, so we need record it into materialized view.
            // NOTE: We don't use the pruned partition infos from ExecPlan because the optimized partition infos are not
            // exact to describe which partitions are refreshed.
            Map<Table, Set<String>> baseTableAndPartitionNames = Maps.newHashMap();
            for (Map.Entry<Table, Set<String>> e : refTableAndPartitionNames.entrySet()) {
                Set<String> realPartitionNames =
                        e.getValue().stream()
                                .flatMap(name -> convertMVPartitionNameToRealPartitionName(e.getKey(), name).stream())
                        .collect(Collectors.toSet());;
                baseTableAndPartitionNames.put(e.getKey(), realPartitionNames);
            }
            Map<Table, Set<String>> nonRefTableAndPartitionNames = getNonRefTableRefreshPartitions();
            if (!nonRefTableAndPartitionNames.isEmpty()) {
                baseTableAndPartitionNames.putAll(nonRefTableAndPartitionNames);
            }

            for (Map.Entry<Table, Set<String>> e : baseTableAndPartitionNames.entrySet()) {
                Table baseTable = e.getKey();
                if (!(baseTable.isNativeTableOrMaterializedView() || baseTable.isHiveTable()) || baseTable.isView()) {
                    throw new DmlException(
                            "update meta failed. only OlapTable or HiveTable is supported");
                }
            }
            MaterializedView.AsyncRefreshContext refreshContext =
                    materializedView.getRefreshScheme().getAsyncRefreshContext();

            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> changedOlapTablePartitionInfos =
                    getSelectedPartitionInfosOfOlapTable(baseTableAndPartitionNames);
            Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> changedExternalTablePartitionInfos
                    = getSelectedPartitionInfosOfExternalTable(baseTableAndPartitionNames);
            Preconditions.checkState(changedOlapTablePartitionInfos.size() + changedExternalTablePartitionInfos.size()
                    == baseTableAndPartitionNames.size());
            updateMetaForOlapTable(refreshContext, changedOlapTablePartitionInfos);
            updateMetaForExternalTable(refreshContext, changedExternalTablePartitionInfos);

            // add message into information_schema
            if (this.getMVTaskRunExtraMessage() != null) {
                try {
                    MVTaskRunExtraMessage extraMessage = getMVTaskRunExtraMessage();
                    Map<String, Set<String>> baseTableRefreshedPartitionsByExecPlan =
                            getBaseTableRefreshedPartitionsByExecPlan(execPlan);
                    extraMessage.setBasePartitionsToRefreshMap(baseTableRefreshedPartitionsByExecPlan);
                } catch (Exception e) {
                    // just log warn and no throw exceptions for updating task runs message.
                    LOG.warn("update task run messages failed:", e);
                }
            }
        } catch (Exception e) {
            LOG.warn("update final meta failed after mv refreshed:", e);
            throw e;
        } finally {
            database.writeUnlock();
        }
    }

    private void updateMetaForOlapTable(MaterializedView.AsyncRefreshContext refreshContext,
                                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> changedTablePartitionInfos) {
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableVisibleVersionMap();
        Table partitionTable = null;
        if (mvContext.hasNextBatchPartition()) {
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
            partitionTable = partitionTableAndColumn.first;
        }
        // update version map of materialized view
        for (Map.Entry<Long, Map<String, MaterializedView.BasePartitionInfo>> tableEntry
                : changedTablePartitionInfos.entrySet()) {
            Long tableId = tableEntry.getKey();
            if (partitionTable != null && tableId != partitionTable.getId()) {
                continue;
            }
            if (!currentVersionMap.containsKey(tableId)) {
                currentVersionMap.put(tableId, Maps.newHashMap());
            }
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.get(tableId);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = tableEntry.getValue();
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // remove partition info of not-exist partition for snapshot table from version map
            Table snapshotTable = snapshotBaseTables.get(tableId).second;
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

            long maxChangedTableRefreshTime =
                    MvUtils.getMaxTablePartitionInfoRefreshTime(changedTablePartitionInfos.values());
            materializedView.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
        }
    }

    private void updateMetaForExternalTable(
            MaterializedView.AsyncRefreshContext refreshContext,
            Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> changedTablePartitionInfos) {
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableInfoVisibleVersionMap();
        BaseTableInfo partitionTableInfo = null;
        if (mvContext.hasNextBatchPartition()) {
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
            partitionTableInfo = snapshotBaseTables.get(partitionTableAndColumn.first.getId()).first;
        }
        // update version map of materialized view
        for (Map.Entry<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> tableEntry
                : changedTablePartitionInfos.entrySet()) {
            BaseTableInfo baseTableInfo = tableEntry.getKey();
            if (partitionTableInfo != null && !partitionTableInfo.equals(baseTableInfo)) {
                continue;
            }
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
            long maxChangedTableRefreshTime =
                    MvUtils.getMaxTablePartitionInfoRefreshTime(changedTablePartitionInfos.values());
            materializedView.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
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
        // wait util transaction is visible for mv refresh task
        // because mv will update base tables' visible version after insert, the mv's visible version
        // should keep up with the base tables, or it will return outdated result.
        oldTransactionVisibleWaitTimeout = context.ctx.getSessionVariable().getTransactionVisibleWaitTimeout();
        context.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(Long.MAX_VALUE / 1000);
        mvContext = new MvTaskRunContext(context);
    }

    /**
     * Sync base table's partition infos to be used later.
     */
    private void syncPartitions() {
        snapshotBaseTables = collectBaseTables(materializedView);
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();

        if (!(partitionInfo instanceof SinglePartitionInfo)) {
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
            mvContext.setRefBaseTable(partitionTableAndColumn.first);
            mvContext.setRefBaseTablePartitionColumn(partitionTableAndColumn.second);
        }
        int partitionTTLNumber = materializedView.getTableProperty().getPartitionTTLNumber();
        mvContext.setPartitionTTLNumber(partitionTTLNumber);

        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            syncPartitionsForExpr();
        }
    }

    /**
     * @param tables : base tables of the materialized view
     * @return       : return the ref base table and column that materialized view's partition column
     * derives from if it exists, otherwise return null.
     */
    private Pair<Table, Column> getRefBaseTableAndPartitionColumn(
            Map<Long, Pair<BaseTableInfo, Table>> tables) {
        SlotRef slotRef = MaterializedView.getRefBaseTablePartitionSlotRef(materializedView);
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
        Pair<Table, Column> partitionTableAndColumn = materializedView.getBaseTableAndPartitionColumn();
        Table refBaseTable = partitionTableAndColumn.first;
        Preconditions.checkNotNull(refBaseTable);
        Column refBaseTablePartitionColumn = partitionTableAndColumn.second;
        Preconditions.checkNotNull(refBaseTablePartitionColumn);

        PartitionDiff rangePartitionDiff = new PartitionDiff();

        int partitionTTLNumber = materializedView.getTableProperty().getPartitionTTLNumber();
        mvContext.setPartitionTTLNumber(partitionTTLNumber);
        Map<String, Range<PartitionKey>> mvPartitionMap = materializedView.getRangePartitionMap();
        database.readLock();

        Map<String, Range<PartitionKey>> mvRangePartitionMap = materializedView.getRangePartitionMap();
        Map<String, Range<PartitionKey>> refBaseTablePartitionMap;
        Map<String, Set<String>> refBaseTableMVPartitionMap = Maps.newHashMap();
        try {
            // Collect the ref base table's partition range map.
            refBaseTablePartitionMap = PartitionUtil.getPartitionKeyRange(refBaseTable, refBaseTablePartitionColumn);

            // To solve multi partition columns' problem of external table, record the mv partition name to all the same
            // partition names map here.
            if (!refBaseTable.isNativeTableOrMaterializedView()) {
                refBaseTableMVPartitionMap = PartitionUtil.getMVPartitionNameMapOfExternalTable(refBaseTable,
                        refBaseTablePartitionColumn, PartitionUtil.getPartitionNames(refBaseTable));
            }

            if (partitionExpr instanceof SlotRef) {
                rangePartitionDiff = SyncPartitionUtils.getRangePartitionDiffOfSlotRef(refBaseTablePartitionMap, mvPartitionMap);
            } else if (partitionExpr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
                String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
                rangePartitionDiff = SyncPartitionUtils.getRangePartitionDiffOfExpr(refBaseTablePartitionMap, mvPartitionMap,
                        granularity, refBaseTablePartitionColumn.getPrimitiveType());
            }
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return;
        } finally {
            database.readUnlock();
        }

        Map<String, Range<PartitionKey>> deletes = rangePartitionDiff.getDeletes();

        // Delete old partitions and then add new partitions because the old and new partitions may overlap
        for (Map.Entry<String, Range<PartitionKey>> deleteEntry : deletes.entrySet()) {
            String mvPartitionName = deleteEntry.getKey();
            dropPartition(database, materializedView, mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] delete partitions range [{}]",
                materializedView.getName(), deletes);

        // Create new added materialized views' ranges
        Map<String, String> partitionProperties = getPartitionProperties(materializedView);
        DistributionDesc distributionDesc = getDistributionDesc(materializedView);
        Map<String, Range<PartitionKey>> adds = rangePartitionDiff.getAdds();

        addPartitions(database, materializedView, adds, partitionProperties, distributionDesc);
        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            mvRangePartitionMap.put(mvPartitionName, addEntry.getValue());
        }
        LOG.info("The process of synchronizing materialized view [{}] add partitions range [{}]",
                materializedView.getName(), adds);

        // used to get partitions to refresh
        Map<String, Set<String>> baseToMvNameRef = SyncPartitionUtils
                .getIntersectedPartitions(refBaseTablePartitionMap, mvRangePartitionMap);
        Map<String, Set<String>> mvToBaseNameRef = SyncPartitionUtils
                .getIntersectedPartitions(mvRangePartitionMap, refBaseTablePartitionMap);

        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableRangePartitionMap(refBaseTablePartitionMap);
        mvContext.setExternalRefBaseTableMVPartitionMap(refBaseTableMVPartitionMap);
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
            Table refBaseTable = mvContext.getRefBaseTable();

            boolean isAutoRefresh = (mvContext.type == Constants.TaskType.PERIODICAL ||
                    mvContext.type == Constants.TaskType.EVENT_TRIGGERED);
            Set<String> mvRangePartitionNames = SyncPartitionUtils.getPartitionNamesByRangeWithPartitionLimit(
                    materializedView, start, end, partitionTTLNumber, isAutoRefresh);

            if (needToRefreshNonPartitionTable(refBaseTable)) {
                if (start == null && end == null) {
                    // if non partition table changed, should refresh all partitions of materialized view
                    return mvRangePartitionNames;
                } else {
                    // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                    // it should be refreshed according to the user-specified range, not all partitions.
                    return getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(refBaseTable,
                            mvRangePartitionNames, true);
                }
            }
            // check partition table
            if (partitionExpr instanceof SlotRef) {
                return getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(refBaseTable, mvRangePartitionNames, force);
            } else if (partitionExpr instanceof FunctionCallExpr) {
                needRefreshMvPartitionNames = getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(refBaseTable,
                        mvRangePartitionNames, force);
                Set<String> baseChangedPartitionNames = getBasePartitionNamesByMVPartitionNames(needRefreshMvPartitionNames);
                // because the relation of partitions between materialized view and base partition table is n : m,
                // should calculate the candidate partitions recursively.
                LOG.debug("Start calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                        " baseChangedPartitionNames: {}", needRefreshMvPartitionNames, baseChangedPartitionNames);
                SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                        mvContext.refBaseTableMVIntersectedPartitions, mvContext.mvRefBaseTableIntersectedPartitions);
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
            result.addAll(mvContext.refBaseTableMVIntersectedPartitions.get(basePartitionName));
        }
        return result;
    }

    private Set<String> getBasePartitionNamesByMVPartitionNames(Set<String> mvPartitionNames) {
        Set<String> result = Sets.newHashSet();
        for (String mvPartitionName : mvPartitionNames) {
            result.addAll(mvContext.mvRefBaseTableIntersectedPartitions.get(mvPartitionName));
        }
        return result;
    }

    private ExecPlan generateRefreshPlan(ConnectContext ctx, InsertStmt insertStmt) throws AnalysisException {
        return StatementPlanner.plan(insertStmt, ctx);
    }

    @VisibleForTesting
    public InsertStmt generateInsertStmt(Set<String> materializedViewPartitions,
                                         Map<String, Set<String>> refTableRefreshPartitions,
                                         MaterializedView materializedView) throws AnalysisException {
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
        Multimap<String, TableRelation> tableRelations =
                AnalyzerUtils.collectAllTableRelation(queryStatement);
        for (Map.Entry<String, TableRelation> nameTableRelationEntry : tableRelations.entries()) {
            if (refTableRefreshPartitions.containsKey(nameTableRelationEntry.getKey())) {
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
        }
        return insertStmt;
    }

    private void generatePartitionPredicate(Set<String> tablePartitionNames, QueryStatement queryStatement,
                                            TableRelation tableRelation) {
        List<Range<PartitionKey>> sourceTablePartitionRange = Lists.newArrayList();
        for (String partitionName : tablePartitionNames) {
            sourceTablePartitionRange.add(mvContext.getRefBaseTableRangePartitionMap().get(partitionName));
        }
        sourceTablePartitionRange = MvUtils.mergeRanges(sourceTablePartitionRange);

        SlotRef partitionSlot = MaterializedView.getRefBaseTablePartitionSlotRef(materializedView);
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
                        if  (!(mvPartitionInfo instanceof ExpressionRangePartitionInfo)) {
                            return false;
                        }

                        Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
                        Column partitionColumn = partitionTableAndColumn.second;
                        // For Non-partition based base table, it's not necessary to check the partition changed.
                        if (!snapshotTable.equals(partitionTableAndColumn.first)
                                || !snapshotTable.containColumn(partitionColumn.getName())) {
                            continue;
                        }

                        Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.
                                getPartitionKeyRange(snapshotTable, partitionColumn);
                        Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.
                                getPartitionKeyRange(table, partitionColumn);
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

                        Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
                        Column partitionColumn = partitionTableAndColumn.second;
                        // For Non-partition based base table, it's not necessary to check the partition changed.
                        if (!snapshotTable.equals(partitionTableAndColumn.first)
                                || !snapShotIcebergTable.containColumn(partitionColumn.getName())) {
                            continue;
                        }

                        Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.
                                getPartitionKeyRange(snapshotTable, partitionColumn);
                        Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.
                                getPartitionKeyRange(table, partitionColumn);
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
        ctx.setStmtId(new AtomicInteger().incrementAndGet());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        try {
            executor.handleDMLStmtWithProfile(execPlan, insertStmt, beginTimeInNanoSecond);
        } catch (Exception e) {
            LOG.warn("refresh materialized view {} failed: {}", materializedView.getName(), e);
            throw e;
        } finally {
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
        DistributionInfo distributionInfo = materializedView.getDefaultDistributionInfo();
        if (distributionInfo instanceof HashDistributionInfo) {
            List<String> distColumnNames = new ArrayList<>();
            for (Column distributionColumn : ((HashDistributionInfo) distributionInfo).getDistributionColumns()) {
                distColumnNames.add(distributionColumn.getName());
            }
            return new HashDistributionDesc(distributionInfo.getBucketNum(), distColumnNames);
        } else {
            return new RandomDistributionDesc();
        }
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

    /**
     * For external table, the partition name is normalized which should convert it into original partition name.
     *
     * For multi-partition columns, `refTableAndPartitionNames` is not fully exact to describe which partitions
     *  of ref base table are refreshed, use `getSelectedPartitionInfosOfExternalTable` later if we can solve the multi
     * partition columns problem.
     * eg:
     *  partitionName1 : par_col=0/par_date=2020-01-01 => p20200101
     *  partitionName2 : par_col=1/par_date=2020-01-01 => p20200101
     */
    private Set<String> convertMVPartitionNameToRealPartitionName(Table table, String mvPartitionName) {
        if (!table.isNativeTableOrMaterializedView()) {
            Map<String, Set<String>> refBaseTableRangePartitionMap = mvContext.getExternalRefBaseTableMVPartitionMap();
            Preconditions.checkState(refBaseTableRangePartitionMap.containsKey(mvPartitionName));
            return refBaseTableRangePartitionMap.get(mvPartitionName);
        } else {
            return Sets.newHashSet(mvPartitionName);
        }
    }

    /**
     * @param mvToRefreshedPartitions :  to-refreshed materialized view partition names
     * @return : return to-refreshed base table's table name and partition names mapping
     */
    private Map<Table, Set<String>> getRefTableRefreshPartitions(Set<String> mvToRefreshedPartitions) {
        Table refBaseTable = mvContext.getRefBaseTable();
        Map<Table, Set<String>> refTableAndPartitionNames = Maps.newHashMap();
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table table = tablePair.second;
            if (refBaseTable != null && refBaseTable == table) {
                Set<String> needRefreshTablePartitionNames = Sets.newHashSet();
                Map<String, Set<String>> mvToBaseNameRef = mvContext.getMvRefBaseTableIntersectedPartitions();
                for (String mvPartitionName : mvToRefreshedPartitions) {
                    needRefreshTablePartitionNames.addAll(mvToBaseNameRef.get(mvPartitionName));
                }
                refTableAndPartitionNames.put(table, needRefreshTablePartitionNames);
                return refTableAndPartitionNames;
            }
        }
        return refTableAndPartitionNames;
    }

    /**
     * Return all non-ref base table and refreshed partitions.
     */
    private Map<Table, Set<String>> getNonRefTableRefreshPartitions() {
        Table partitionTable = mvContext.getRefBaseTable();
        Map<Table, Set<String>> tableNamePartitionNames = Maps.newHashMap();
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table table = tablePair.second;
            if (partitionTable != null && partitionTable == table) {
                // do nothing.
            } else {
                if (table.isNativeTableOrMaterializedView()) {
                    tableNamePartitionNames.put(table, ((OlapTable) table).getPartitionNames());
                } else if (table.isHiveTable()) {
                    tableNamePartitionNames.put(table, Sets.newHashSet(PartitionUtil.getPartitionNames(table)));
                } else if (table.isView()) {
                    continue;
                } else {
                    Preconditions.checkState(false, "Do not support get partition names and columns for" +
                            "table type %s", table.getType());
                }
            }
        }
        return tableNamePartitionNames;
    }

    /**
     * Collect base olap tables and its partition infos based on refreshed table infos.
     *
     * @param baseTableAndPartitionNames : refreshed base table and its partition names mapping.
     * @return
     */
    private Map<Long, Map<String, MaterializedView.BasePartitionInfo>> getSelectedPartitionInfosOfOlapTable(
            Map<Table, Set<String>> baseTableAndPartitionNames) {
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> changedOlapTablePartitionInfos = Maps.newHashMap();
        for (Map.Entry<Table, Set<String>> entry : baseTableAndPartitionNames.entrySet()) {
            if (entry.getKey().isNativeTableOrMaterializedView()) {
                Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
                OlapTable olapTable = (OlapTable) entry.getKey();
                for (String partitionName : entry.getValue()) {
                    Partition partition = olapTable.getPartition(partitionName);
                    MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                            partition.getId(), partition.getVisibleVersion(), partition.getVisibleVersionTime());
                    partitionInfos.put(partition.getName(), basePartitionInfo);
                }
                changedOlapTablePartitionInfos.put(olapTable.getId(), partitionInfos);
            }
        }
        return changedOlapTablePartitionInfos;
    }

    /**
     * Collect base hive tables and its partition infos based on refreshed table infos.
     *
     * @param baseTableAndPartitionNames : refreshed base table and its partition names mapping.
     * @return
     */
    private Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> getSelectedPartitionInfosOfExternalTable(
            Map<Table, Set<String>> baseTableAndPartitionNames) {
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> changedOlapTablePartitionInfos = Maps.newHashMap();
        for (Map.Entry<Table, Set<String>> entry : baseTableAndPartitionNames.entrySet()) {
            if (entry.getKey().isHiveTable()) {
                HiveTable hiveTable = (HiveTable) entry.getKey();
                Optional<BaseTableInfo> baseTableInfoOptional = materializedView.getBaseTableInfos().stream().filter(
                                baseTableInfo -> baseTableInfo.getTableIdentifier().equals(hiveTable.getTableIdentifier())).
                        findAny();
                if (!baseTableInfoOptional.isPresent()) {
                    continue;
                }
                BaseTableInfo baseTableInfo = baseTableInfoOptional.get();
                Map<String, MaterializedView.BasePartitionInfo> partitionInfos =
                        getSelectedPartitionInfos(hiveTable, Lists.newArrayList(entry.getValue()),
                                baseTableInfo);
                changedOlapTablePartitionInfos.put(baseTableInfo, partitionInfos);
            }
        }
        return changedOlapTablePartitionInfos;
    }

    /**
     * @param hiveTable                 : input hive table to collect refresh partition infos
     * @param selectedPartitionNames    : input hive table refreshed partition names
     * @param baseTableInfo             : input hive table's base table info
     * @return                          : return the given hive table's refresh partition infos
     */
    private Map<String, MaterializedView.BasePartitionInfo> getSelectedPartitionInfos(HiveTable hiveTable,
                                                                                      List<String> selectedPartitionNames,
                                                                                      BaseTableInfo baseTableInfo) {
        Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
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

    /**
     * Extract refreshed/scanned base table and its refreshed partition names
     * NOTE: this is used to trace in task_runs.
     */
    private Map<String, Set<String>> getBaseTableRefreshedPartitionsByExecPlan(
            ExecPlan execPlan) {
        Map<String, Set<String>> baseTableRefreshPartitionNames = Maps.newHashMap();
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        for (ScanNode scanNode : scanNodes) {
            Set<String> selectedPartitionNames = Sets.newHashSet();
            if (scanNode instanceof OlapScanNode) {
                OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                List<Long> selectedPartitionIds = olapScanNode.getSelectedPartitionIds();
                OlapTable olapTable = olapScanNode.getOlapTable();
                selectedPartitionNames = selectedPartitionIds.stream().map(p -> olapTable.getPartition(p).getName())
                        .collect(Collectors.toSet());
                baseTableRefreshPartitionNames.put(olapTable.getName(), selectedPartitionNames);
            } else if (scanNode instanceof HdfsScanNode) {
                HdfsScanNode hdfsScanNode = (HdfsScanNode) scanNode;
                HiveTable hiveTable = (HiveTable) hdfsScanNode.getHiveTable();
                Optional<BaseTableInfo> baseTableInfoOptional = materializedView.getBaseTableInfos().stream().filter(
                                baseTableInfo -> baseTableInfo.getTableIdentifier().equals(hiveTable.getTableIdentifier())).
                        findAny();
                if (!baseTableInfoOptional.isPresent()) {
                    continue;
                }
                selectedPartitionNames = Sets.newHashSet(getSelectedPartitionNamesOfHiveTable(hiveTable, hdfsScanNode));
                baseTableRefreshPartitionNames.put(hiveTable.getName(), selectedPartitionNames);
            } else {
                // do nothing.
            }
        }
        return baseTableRefreshPartitionNames;
    }

    /**
     * Extract olap table partition name and its partition info from olap scan node.
     */
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

    /**
     * Extract hive partition name and its partition info from hdfs scan node.
     */
    private Map<String, MaterializedView.BasePartitionInfo> getSelectedPartitionInfos(HdfsScanNode hdfsScanNode,
                                                                                      BaseTableInfo baseTableInfo) {
        Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
        HiveTable hiveTable = hdfsScanNode.getHiveTable();
        List<String> selectedPartitionNames = getSelectedPartitionNamesOfHiveTable(hiveTable, hdfsScanNode);
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

    /**
     * Extract hive partition names from hdfs scan node.
     */
    private List<String> getSelectedPartitionNamesOfHiveTable(HiveTable hiveTable, HdfsScanNode hdfsScanNode) {
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
        return selectedPartitionNames;
    }
}
