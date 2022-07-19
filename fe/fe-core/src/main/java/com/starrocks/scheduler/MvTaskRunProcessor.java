// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.DropPartitionClause;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.PartitionKeyDesc;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ExpressionPartitionUtil;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mv task run core logic，if multiple MvTaskRunProcessor instances are running, it will cause some concurrency issues
 */
public class MvTaskRunProcessor extends BaseTaskRunProcessor {

    private static final Logger LOG = LogManager.getLogger(MvTaskRunProcessor.class);

    public static final String MV_ID = "mvId";

    @Override
    public void processTaskRun(TaskRunContext context) {
        Map<String, String> properties = context.getProperties();
        // NOTE: need set mvId in Task's properties when creating
        long mvId = Long.parseLong(properties.get(MV_ID));
        // 0. prepare
        Database database = GlobalStateMgr.getCurrentState().getDb(context.ctx.getDatabase());
        MaterializedView materializedView = (MaterializedView) database.getTable(mvId);
        if (!materializedView.isActive()) {
            String errorMsg = String.format("Materialized view: %s, id: %d is not active, " +
                    "skip sync partition and data with base tables", materializedView.getName(), mvId);
            LOG.warn(errorMsg);
            throw new DmlException(errorMsg);
        }
        // collect all base table partition info
        Map<Long, OlapTable> olapTables = collectBaseTables(materializedView, database);
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        // if mv has not partition by clause
        if (partitionInfo instanceof SinglePartitionInfo) {
            // must create partition when creating mv
            Preconditions.checkState(!materializedView.getPartitions().isEmpty());
            boolean needRefresh = false;
            for (Map.Entry<Long, OlapTable> idOlapTableEntry : olapTables.entrySet()) {
                long baseTableId = idOlapTableEntry.getKey();
                OlapTable olapTable = idOlapTableEntry.getValue();
                Collection<Partition> partitions = olapTable.getPartitions();
                for (Partition partition : partitions) {
                    if (materializedView.needAddBasePartition(baseTableId, partition)) {
                        materializedView.addBasePartition(baseTableId, partition);
                    }
                }
                Set<String> syncedPartitionNames = materializedView.getSyncedPartitionNames(baseTableId);
                for (String syncedPartitionName : syncedPartitionNames) {
                    Partition baseTablePartition = olapTable.getPartition(syncedPartitionName);
                    if (baseTablePartition == null) {
                        needRefresh = true;
                        materializedView.removeBasePartition(baseTableId, syncedPartitionName);
                    } else {
                        if (materializedView.needRefreshPartition(baseTableId, baseTablePartition)) {
                            needRefresh = true;
                            materializedView.updateBasePartition(baseTableId, baseTablePartition);
                        }
                    }
                }
            }
            if (needRefresh) {
                refreshMv(context, materializedView, database, olapTables, null, materializedView.getPartitionNames());
            }
            return;
        }
        Map<String, String> partitionProperties = getPartitionProperties(materializedView);
        DistributionDesc distributionDesc = getDistributionDesc(materializedView);
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                ((ExpressionRangePartitionInfo) partitionInfo);
        // currently, mv only supports one expression
        Preconditions.checkState(expressionRangePartitionInfo.getPartitionExprs().size() == 1);
        // 1. scan base table, get table id which used in partition
        Expr partitionExpr = materializedView.getPartitionRefTableExprs().get(0);
        OlapTable partitionTable = null;
        Column partitionColumn = null;
        List<SlotRef> slotRefs = Lists.newArrayList();
        partitionExpr.collect(SlotRef.class, slotRefs);
        // if partitionExpr is FunctionCallExpr, get first SlotRef
        Preconditions.checkState(slotRefs.size() == 1);
        SlotRef slotRef = slotRefs.get(0);
        for (OlapTable olapTable : olapTables.values()) {
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(olapTable.getName())) {
                partitionTable = olapTable;
                partitionColumn = partitionTable.getColumn(slotRef.getColumnName());
            }
        }
        // 2. sync partition with partition table, get need refresh mv partition names
        Set<String> needRefreshPartitionNames = Sets.newHashSet();
        processPartitionWithPartitionTable(database, materializedView, partitionTable,
                partitionExpr, partitionColumn, partitionProperties,
                distributionDesc, needRefreshPartitionNames);
        // 3. collect need refresh mv partition names
        boolean refreshAllPartitions = false;
        for (OlapTable olapTable : olapTables.values()) {
            if (olapTable.getId() == partitionTable.getId()) {
                continue;
            }
            // check with no partition expression related table
            if (checkNoPartitionRefTablePartitions(materializedView, olapTable)) {
                refreshAllPartitions = true;
            }
        }
        if (materializedView.getPartitions().isEmpty()) {
            return;
        }
        if (refreshAllPartitions) {
            needRefreshPartitionNames =  materializedView.getPartitionNames();
        }
        // 4. refresh mv
        if (!needRefreshPartitionNames.isEmpty()) {
            refreshMv(context, materializedView, database, olapTables, partitionTable, needRefreshPartitionNames);
        }
    }
    public Map<Long, OlapTable> collectBaseTables(MaterializedView materializedView, Database database) {
        Map<Long, OlapTable> olapTables = Maps.newHashMap();
        Set<Long> baseTableIds = materializedView.getBaseTableIds();
        database.readLock();
        try {
            for (Long baseTableId : baseTableIds) {
                OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
                if (olapTable == null) {
                    throw new SemanticException("Materialized view base table: " + baseTableId + " not exist.");
                }
                OlapTable copied = new OlapTable();
                if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                    throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                }
                olapTables.put(olapTable.getId(), copied);
            }
        } finally {
            database.readUnlock();
        }
        return olapTables;
    }

    private void processPartitionWithPartitionTable(Database database, MaterializedView materializedView,
                                                    OlapTable olapTable, Expr partitionExpr,
                                                    Column partitionColumn, Map<String, String> partitionProperties,
                                                    DistributionDesc distributionDesc,
                                                    Set<String> needRefreshPartitionNames) {
        // 0. prepare get need add partition
        long baseTableId = olapTable.getId();
        Set<String> partitionNames = Sets.newHashSet();
        Set<Partition> needAddPartitions = Sets.newHashSet();
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                (ExpressionRangePartitionInfo) materializedView.getPartitionInfo();
        RangePartitionInfo baseRangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        Collection<Partition> basePartitions = olapTable.getPartitions();
        for (Partition basePartition : basePartitions) {
            // record full partitions
            partitionNames.add(basePartition.getName());
            // if exists, not check
            if (!materializedView.needAddBasePartition(baseTableId, basePartition)) {
                continue;
            }
            needAddPartitions.add(basePartition);
        }
        // 1. remove partition refs & get need refresh mv partition ids
        Set<String> deletedPartitionNames = materializedView.getNoExistBasePartitionNames(baseTableId, partitionNames);
        // record checked mv and get need refresh partition ids
        Set<String> checkedMvPartitionNames = Sets.newHashSet();
        for (String deletedPartitionName : deletedPartitionNames) {
            // if base partition dropped
            Set<String> refMvPartitionNames = materializedView.getMvPartitionNamesByTable(deletedPartitionName);
            for (String refMvPartitionName : refMvPartitionNames) {
                if (checkedMvPartitionNames.contains(refMvPartitionNames)) {
                    continue;
                }
                checkedMvPartitionNames.add(refMvPartitionName);
                Set<String> refTablePartitionNames = materializedView.getTablePartitionNamesByMv(refMvPartitionName);
                if (deletedPartitionNames.containsAll(refTablePartitionNames)) {
                    dropPartition(database, materializedView, refMvPartitionName);
                } else {
                    needRefreshPartitionNames.add(refMvPartitionName);
                }
            }
            materializedView.removePartitionNameRefByTable(deletedPartitionName);
            materializedView.removeBasePartition(baseTableId, deletedPartitionName);
        }
        // 2. add partitions & partition refs
        for (Partition needAddPartition : needAddPartitions) {
            long basePartitionId = needAddPartition.getId();
            String basePartitionName = needAddPartition.getName();
            materializedView.addBasePartition(baseTableId, needAddPartition);
            Range<PartitionKey> basePartitionRange = baseRangePartitionInfo.getRange(basePartitionId);
            List<Column> basePartitionColumns = baseRangePartitionInfo.getPartitionColumns();
            int basePartitionIndex = -1;
            for (int i = 0; i < basePartitionColumns.size(); i++) {
                if (basePartitionColumns.get(i).equals(partitionColumn)) {
                    basePartitionIndex = i;
                    break;
                }
            }
            // e.g. base table range 2020-04-21 ~ 2020-07-21, 2020-10-21 ~ 2021-01-21
            // expr is SlotRef, mv range 2020-04-21 ~ 2020-07-21, 2020-10-21 ~ 2021-01-21
            // expr is FunctionCallExpr, use function date_trunc, fmt is quarter
            // mv range 2020-04-01 ~ 2020-10-01, 2020-10-01 ~ 2021-04-01
            Range<PartitionKey> mvPartitionKeyRange = ExpressionPartitionUtil
                    .getPartitionKeyRange(partitionExpr, partitionColumn,
                            expressionRangePartitionInfo.getIdToRange(false).values(),
                            basePartitionRange, basePartitionIndex);
            if (mvPartitionKeyRange != null) {
                addPartition(database, materializedView, basePartitionName,
                        mvPartitionKeyRange, partitionProperties, distributionDesc);
            } else {
                Map<Long, Range<PartitionKey>> idToRange = expressionRangePartitionInfo.getIdToRange(false);
                for (Map.Entry<Long, Range<PartitionKey>> idRangeEntry : idToRange.entrySet()) {
                    if (RangeUtils.isRangeIntersect(basePartitionRange, idRangeEntry.getValue())) {
                        materializedView.addPartitionNameRef(basePartitionName,
                                materializedView.getPartition(idRangeEntry.getKey()).getName());
                        break;
                    }
                }
            }
        }
        // 3. merge need refresh mv with comparing other partitions (include added partitions)
        Set<String> existBasePartitionNames = materializedView.getExistBasePartitionNames(baseTableId);
        for (String basePartitionName : existBasePartitionNames) {
            Partition partition = olapTable.getPartition(basePartitionName);
            if (materializedView.needRefreshPartition(baseTableId, partition)) {
                needRefreshPartitionNames.addAll(
                        materializedView.getMvPartitionNamesByTable(basePartitionName));
            }
        }
    }

    private boolean checkNoPartitionRefTablePartitions(MaterializedView materializedView, OlapTable olapTable) {
        boolean refreshAllPartitions = false;
        long baseTableId = olapTable.getId();
        Collection<Partition> basePartitions = olapTable.getPartitions();
        for (Partition basePartition : basePartitions) {
            if (materializedView.needAddBasePartition(baseTableId, basePartition)) {
                refreshAllPartitions = true;
                materializedView.addBasePartition(baseTableId, basePartition);
            }
        }
        Set<String> syncedPartitionNames = materializedView.getSyncedPartitionNames(baseTableId);
        for (String syncedPartitionName : syncedPartitionNames) {
            Partition baseTablePartition = olapTable.getPartition(syncedPartitionName);
            if (baseTablePartition == null) {
                refreshAllPartitions = true;
                materializedView.removeBasePartition(baseTableId, syncedPartitionName);
            } else if (materializedView.needRefreshPartition(baseTableId, baseTablePartition)) {
                refreshAllPartitions = true;
            }
        }
        return refreshAllPartitions;
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

    private void addPartition(Database database, MaterializedView materializedView, String basePartitionName,
                              Range<PartitionKey> partitionKeyRange, Map<String, String> partitionProperties,
                              DistributionDesc distributionDesc) {
        String lowerBound = partitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue();
        String upperBound = partitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue();
        PrimitiveType type = partitionKeyRange.lowerEndpoint().getKeys().get(0).getType().getPrimitiveType();
        PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(
                Collections.singletonList(new PartitionValue(lowerBound)),
                Collections.singletonList(new PartitionValue(upperBound)));
        String partitionName = "p" + ExpressionPartitionUtil.getFormattedPartitionName(lowerBound, upperBound, type);
        SingleRangePartitionDesc singleRangePartitionDesc =
                new SingleRangePartitionDesc(false, partitionName, partitionKeyDesc, partitionProperties);
        try {
            GlobalStateMgr.getCurrentState().addPartitions(
                    database, materializedView.getName(),
                    new AddPartitionClause(singleRangePartitionDesc, distributionDesc,
                            partitionProperties, false));
            materializedView.addPartitionNameRef(basePartitionName, partitionName);
        } catch (Exception e) {
            throw new SemanticException("Expression add partition failed: %s, db: %s, table: %s", e.getMessage(),
                    database.getFullName(), materializedView.getName());
        }
    }

    private void dropPartition(Database database, MaterializedView materializedView, String mvPartitionName) {
        String dropPartitionName = materializedView.getPartition(mvPartitionName).getName();
        database.writeLock();
        try {
            GlobalStateMgr.getCurrentState().dropPartition(
                    database, materializedView,
                    new DropPartitionClause(false, dropPartitionName, false, true));
            materializedView.removePartitionNameRefByMv(mvPartitionName);
        } catch (Exception e) {
            throw new SemanticException("Expression drop partition failed: {}, db: {}, table: {}", e.getMessage(),
                    database.getFullName(), materializedView.getName());
        } finally {
            database.writeUnlock();
        }
    }

    private void refreshMv(TaskRunContext context, MaterializedView materializedView,
                           Database database, Map<Long, OlapTable> olapTables, OlapTable partitionTable,
                           Set<String> needRefreshMvPartitionNames) {
        // get table partition names
        Map<String, Set<String>> tableNamePartitionNames = Maps.newHashMap();
        for (OlapTable olapTable : olapTables.values()) {
            if (partitionTable != null && olapTable.getId() == partitionTable.getId()) {
                Set<String> needRefreshTablePartitionNames = needRefreshMvPartitionNames.stream()
                        .map(materializedView::getTablePartitionNamesByMv)
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());
                Preconditions.checkState(!needRefreshTablePartitionNames.isEmpty());
                tableNamePartitionNames.put(olapTable.getName(), needRefreshTablePartitionNames);
            } else {
                tableNamePartitionNames.put(olapTable.getName(), olapTable.getPartitionNames());
            }
        }
        // set partition names into insertStmt
        ConnectContext ctx = context.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(context.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase());
        ctx.getPlannerProfile().reset();
        String definition = context.getDefinition();
        InsertStmt insertStmt =
                (InsertStmt) SqlParser.parse(definition, ctx.getSessionVariable().getSqlMode()).get(0);
        insertStmt.setTargetPartitionNames(new PartitionNames(false, new ArrayList<>(needRefreshMvPartitionNames)));
        QueryStatement queryStatement = insertStmt.getQueryStatement();
        Map<String, TableRelation> tableRelations =
                AnalyzerUtils.collectAllTableRelation(queryStatement);
        for (Map.Entry<String, TableRelation> nameTableRelationEntry : tableRelations.entrySet()) {
            Set<String> tablePartitionNames = tableNamePartitionNames.get(nameTableRelationEntry.getKey());
            TableRelation tableRelation = nameTableRelationEntry.getValue();
            tableRelation.setPartitionNames(
                    new PartitionNames(false, new ArrayList<>(tablePartitionNames)));
        }
        // insert overwrite mv must set system = true
        insertStmt.setSystem(true);
        Analyzer.analyze(insertStmt, ctx);
        // execute insert stmt
        execInsertStmt(database, insertStmt, context, materializedView,
                collectPartitionInfo(olapTables, tableNamePartitionNames));
    }

    private Map<Long, Map<String, MaterializedView.BasePartitionInfo>> collectPartitionInfo(
            Map<Long, OlapTable> olapTables, Map<String, Set<String>> tableNamePartitionNames) {
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> tablePartitionInfos = Maps.newHashMap();
        for (OlapTable olapTable : olapTables.values()) {
            Map<String, MaterializedView.BasePartitionInfo> namePartitionInfos = Maps.newHashMap();
            Set<String> tablePartitionNames = tableNamePartitionNames.get(olapTable.getName());
            for (String tablePartitionName : tablePartitionNames) {
                Partition partition = olapTable.getPartition(tablePartitionName);
                namePartitionInfos.put(partition.getName(),
                        new MaterializedView.BasePartitionInfo(partition.getId(),
                                partition.getVisibleVersion()));
            }
            tablePartitionInfos.put(olapTable.getId(), namePartitionInfos);
        }
        return tablePartitionInfos;
    }

    private void execInsertStmt(Database database, InsertStmt insertStmt,
                                TaskRunContext context, MaterializedView materializedView,
                                Map<Long, Map<String, MaterializedView.BasePartitionInfo>> plannedOlapTablePartitionInfos) {
        ConnectContext ctx = context.getCtx();
        StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
        ctx.setExecutor(executor);
        ctx.setThreadLocalInfo();
        try {
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> selectedBasePartitionInfos = Maps.newHashMap();
            database.readLock();
            ExecPlan execPlan;
            try {
                execPlan = new StatementPlanner().plan(insertStmt, ctx);
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
                // check partition changed
                checkPartitionChange(selectedBasePartitionInfos, plannedOlapTablePartitionInfos);
            } finally {
                database.readUnlock();
            }
            executor.handleDMLStmt(execPlan, insertStmt);
            // use selected base partition info to update mv version map & partition name map
            for (Map.Entry<Long, Map<String, MaterializedView.BasePartitionInfo>> longMapEntry :
                    selectedBasePartitionInfos.entrySet()) {
                long baseTableId = longMapEntry.getKey();
                Map<String, MaterializedView.BasePartitionInfo> baseTableIdAndVersion = longMapEntry.getValue();
                for (Map.Entry<String, MaterializedView.BasePartitionInfo> basicTablePartitionIdVersions :
                        baseTableIdAndVersion.entrySet()) {
                    String baseTablePartitionName = basicTablePartitionIdVersions.getKey();
                    MaterializedView.BasePartitionInfo baseTablePartitionVersion =
                            basicTablePartitionIdVersions.getValue();
                    materializedView.updateBasePartition(baseTableId, baseTablePartitionName,
                            baseTablePartitionVersion);
                }
            }
            materializedView.getRefreshScheme().setLastRefreshTime(Utils.getLongFromDateTime(LocalDateTime.now()));
        } catch (Exception e) {
            throw new SemanticException("Refresh materialized view failed: " + e.getMessage(), e);
        } finally {
            auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
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

    /**
     * Why need to do this:
     * e.g.
     * base table tbl1 has partitions p11 & p12 & p13
     * base table tbl2 has partitions p21 & p22 & p23
     * create materialized view mv1 partition by tbl1.a as select tbl1.a, tbl2.b from tbl1 join tbl2 on tbl1.a = tbl2.a;
     * materialized view mv1 has partitions m1 & m2 & m3, m1 <-> p11, m2 <-> p12, m3 <-> p13
     * if tbl1 p11 p12 inserted data, task use this sql for sync data:
     * insert overwrite mv partition(m1,m2) select tbl1.a, tbl2.b from tbl1 partition(p11,p12) join tbl2 on tbl1.a = tbl2.a;
     * if tbl1 p12 renamed to p120 or dropped when task running, only use tbl1(p11) tbl2(p21,p22,p23) to refresh m1 & m2;
     * if tbl2 p22 renamed to p220 or dropped when task running, only use tbl1(p11,p12) tbl2(p21,p23) to refresh m1 & m2;
     * but we do not remove the partition name relationships in maps
     *
     * @param selectedOlapTablePartitionInfos
     * @param plannedOlapTablePartitionInfos
     */
    private void checkPartitionChange(
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> selectedOlapTablePartitionInfos,
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> plannedOlapTablePartitionInfos) {
        for (Map.Entry<Long, Map<String, MaterializedView.BasePartitionInfo>> plannedOlapTablePartitionEntry :
                plannedOlapTablePartitionInfos.entrySet()) {
            long olapTableId = plannedOlapTablePartitionEntry.getKey();
            Map<String, MaterializedView.BasePartitionInfo> plannedPartitionNameInfos =
                    plannedOlapTablePartitionEntry.getValue();
            Map<String, MaterializedView.BasePartitionInfo> selectedPartitionNameInfos =
                    selectedOlapTablePartitionInfos.get(olapTableId);
            for (Map.Entry<String, MaterializedView.BasePartitionInfo> plannedPartitionNameInfoEntry :
                    plannedPartitionNameInfos.entrySet()) {
                String plannedPartitionName = plannedPartitionNameInfoEntry.getKey();
                MaterializedView.BasePartitionInfo selectedBasePartitionInfo =
                        selectedPartitionNameInfos.get(plannedPartitionName);
                // check partition is renamed or removed
                if (selectedBasePartitionInfo == null) {
                    throw new SemanticException(
                            "Base table: " + olapTableId + " Partition: " + plannedPartitionName + " can not find");
                }
            }
        }
    }
}
