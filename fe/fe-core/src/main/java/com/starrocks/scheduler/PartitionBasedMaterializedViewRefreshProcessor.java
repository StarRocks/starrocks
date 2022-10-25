// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.clearspring.analytics.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
                Set<String> partitionsToRefresh = getPartitionsToRefreshForMaterializedView(context.getProperties());
                LOG.debug("materialized view partitions to refresh:{}", partitionsToRefresh);
                if (partitionsToRefresh.isEmpty()) {
                    LOG.info("no partitions to refresh for materialized view {}", materializedView.getName());
                    return;
                }

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
                throw new DmlException("update meta failed. materialized view:" + materializedView.getName() + " not exist");
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
                Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo = currentVersionMap.get(tableId);
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
            LOG.warn("materialized view:{} in database:{} do not exist when refreshing", mvId, context.ctx.getDatabase());
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

    private Pair<Table, Column> getPartitionTableAndColumn(Map<Long, Pair<MaterializedView.BaseTableInfo, Table>> tables) {
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr partitionExpr = getPartitionExpr();
        partitionExpr.collect(SlotRef.class, slotRefs);
        // if partitionExpr is FunctionCallExpr, get first SlotRef
        Preconditions.checkState(slotRefs.size() == 1);
        SlotRef slotRef = slotRefs.get(0);
        for (Pair<MaterializedView.BaseTableInfo, Table> tableInfo : tables.values()) {
            Table table = tableInfo.second;
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(table.getName())) {
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
        Map<String, Range<PartitionKey>> basePartitionMap = Maps.newHashMap();
        Map<String, Range<PartitionKey>> mvPartitionMap;
        database.readLock();
        try {
            if (partitionBaseTable.isOlapTable()) {
                basePartitionMap = ((OlapTable) partitionBaseTable).getRangePartitionMap();
            }
            mvPartitionMap = materializedView.getRangePartitionMap();
            if (partitionExpr instanceof SlotRef) {
                partitionDiff = SyncPartitionUtils.calcSyncSamePartition(basePartitionMap, mvPartitionMap);
            } else if (partitionExpr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
                String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue();
                partitionDiff = SyncPartitionUtils.calcSyncRollupPartition(basePartitionMap, mvPartitionMap,
                        granularity, partitionColumn.getPrimitiveType());
            }
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

    private boolean needToRefreshTable(OlapTable olapTable) {
        return !materializedView.getNeedRefreshPartitionNames(olapTable).isEmpty();
    }

    private boolean needToRefreshNonPartitionTable(Table partitionTable) {
        if (snapshotBaseTables.values().stream().anyMatch(tablePair -> !tablePair.second.isOlapTable())) {
            return true;
        }
        for (Pair<MaterializedView.BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            OlapTable olapTable = (OlapTable) tablePair.second;
            if (olapTable.getId() == partitionTable.getId()) {
                continue;
            }
            if (needToRefreshTable(olapTable)) {
                return true;
            }
        }
        return false;
    }

    private boolean needToRefreshNonPartitionTable() {
        if (snapshotBaseTables.values().stream().anyMatch(tablePair -> !tablePair.second.isOlapTable())) {
            return true;
        }
        for (Pair<MaterializedView.BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            OlapTable olapTable = (OlapTable) tablePair.second;
            if (needToRefreshTable(olapTable)) {
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
        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();
        Set<String> rangePartitionNames = null;
        if (StringUtils.isEmpty(start) && StringUtils.isEmpty(end) && force) {
            LOG.debug("materialized view force to refresh all partitions.");
            return materializedView.getPartitionNames();
        }
        if (!StringUtils.isEmpty(start) && !StringUtils.isEmpty(end)) {
            rangePartitionNames = SyncPartitionUtils.getPartitionNamesByRange(materializedView, start, end);
            LOG.debug("materialized view refresh partitions {}", rangePartitionNames);
            if (force) {
                return rangePartitionNames;
            }
        }
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            // for non-partitioned materialized view
            if (needToRefreshNonPartitionTable()) {
                needRefreshMvPartitionNames.addAll(materializedView.getPartitionNames());
                return needRefreshMvPartitionNames;
            }
        } else if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            Expr partitionExpr = getPartitionExpr();
            Pair<Table, Column> partitionTableAndColumn = getPartitionTableAndColumn(snapshotBaseTables);
            Table partitionTable = partitionTableAndColumn.first;

            if (needToRefreshNonPartitionTable(partitionTable)) {
                // if non partition table changed, should refresh all partitions of materialized view
                needRefreshMvPartitionNames.addAll(materializedView.getPartitionNames());
                return needRefreshMvPartitionNames;
            }

            // check partition table
            if (partitionExpr instanceof SlotRef) {
                Set<String> baseChangedPartitionNames = materializedView.getNeedRefreshPartitionNames(partitionTable);
                for (String basePartitionName : baseChangedPartitionNames) {
                    needRefreshMvPartitionNames.addAll(mvContext.baseToMvNameRef.get(basePartitionName));
                }
            } else if (partitionExpr instanceof FunctionCallExpr) {
                // check if there is a load in the base table and add it to the refresh candidate
                Set<String> baseChangedPartitionNames = materializedView.getNeedRefreshPartitionNames(partitionTable);
                for (String baseChangedPartitionName : baseChangedPartitionNames) {
                    needRefreshMvPartitionNames.addAll(mvContext.baseToMvNameRef.get(baseChangedPartitionName));
                }
                // because the relation of partitions between materialized view and base partition table is n : m,
                // should calculate the candidate partitions recursively.
                SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                        mvContext.baseToMvNameRef, mvContext.mvToBaseNameRef);
            }
        } else {
            throw new DmlException("unsupported partition info type:" + partitionInfo.getClass().getName());
        }
        if (null != rangePartitionNames) {
            needRefreshMvPartitionNames.retainAll(rangePartitionNames);
        }
        return needRefreshMvPartitionNames;
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
            if (!table.isOlapTable()) {
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
                    new PartitionNames(false, tablePartitionNames == null ?  null :
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
            Table table = tablePair.second;
            if (!table.isOlapTable()) {
                // do not check other type table have changed now.
                continue;
            }
            OlapTable snapshotTable = (OlapTable) table;
            Database db = baseTableInfo.getDb();
            if (db == null) {
                return true;
            }
            db.readLock();
            try {
                if (snapshotTable.getPartitionInfo() instanceof SinglePartitionInfo) {
                    Set<String> partitionNames =
                            ((OlapTable) db.getTable(snapshotTable.getId())).getPartitionNames();
                    if (!snapshotTable.getPartitionNames().equals(partitionNames)) {
                        // there is partition rename
                        return true;
                    }
                } else {
                    Map<String, Range<PartitionKey>> snapshotPartitionMap = snapshotTable.getRangePartitionMap();
                    Map<String, Range<PartitionKey>> currentPartitionMap =
                            ((OlapTable) db.getTable(snapshotTable.getId())).getRangePartitionMap();
                    boolean changed = SyncPartitionUtils.hasPartitionChange(snapshotPartitionMap, currentPartitionMap);
                    if (changed) {
                        return true;
                    }
                }
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
    public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan, InsertStmt insertStmt) throws Exception {
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
        PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(
                Collections.singletonList(new PartitionValue(lowerBound)),
                Collections.singletonList(new PartitionValue(upperBound)));
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
