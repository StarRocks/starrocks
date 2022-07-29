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
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.PartitionKeyDesc;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.SingleRangePartitionDesc;
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
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.UUIDUtil;
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
import com.starrocks.sql.common.PartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
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
import java.util.concurrent.atomic.AtomicInteger;

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
        MvTaskRunContext mvContext = new MvTaskRunContext(context);
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
                    if (baseTablePartition != null && materializedView.needRefreshPartition(baseTableId, baseTablePartition)) {
                        needRefresh = true;
                        materializedView.updateBasePartition(baseTableId, baseTablePartition);
                    }
                }
                if (materializedView.cleanBasePartition(olapTable) > 0) {
                    needRefresh = true;
                }
            }
            if (needRefresh) {
                refreshMv(mvContext, materializedView, database, olapTables, null,
                        materializedView.getPartitionNames());
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
        Set<String> needRefreshPartitionNames = processSyncBasePartition(mvContext, database, materializedView,
                partitionTable, partitionExpr, partitionColumn, partitionProperties, distributionDesc);
        // 3. collect need refresh mv partition ids
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
            refreshMv(mvContext, materializedView, database, olapTables, partitionTable, needRefreshPartitionNames);
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


    private Set<String> processSyncBasePartition(MvTaskRunContext context, Database db, MaterializedView mv,
                                                 OlapTable base, Expr partitionExpr,
                                                 Column partitionColumn, Map<String, String> partitionProperties,
                                                 DistributionDesc distributionDesc) {
        long baseTableId = base.getId();

        Map<String, Range<PartitionKey>> basePartitionMap = base.getRangePartitionMap();
        Map<String, Range<PartitionKey>> mvPartitionMap = mv.getRangePartitionMap();

        PartitionDiff partitionDiff = new PartitionDiff();
        String granularity = null;
        if (partitionExpr instanceof SlotRef) {
            partitionDiff = SyncPartitionUtils.calcSyncSamePartition(basePartitionMap, mvPartitionMap);
        } else if (partitionExpr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
            granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue();
            partitionDiff = SyncPartitionUtils.calcSyncRollupPartition(basePartitionMap, mvPartitionMap,
                    granularity, partitionColumn.getPrimitiveType());
        }

        Map<String, Range<PartitionKey>> adds = partitionDiff.getAdds();
        Map<String, Range<PartitionKey>> deletes = partitionDiff.getDeletes();
        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();

        // We should delete the old partition first and then add the new one,
        // because the old and new partitions may overlap

        for (Map.Entry<String, Range<PartitionKey>> deleteEntry : deletes.entrySet()) {
            String mvPartitionName = deleteEntry.getKey();
            dropPartition(db, mv, mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] partitions delete range [{}]", mv.getName(), deletes);

        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            addPartition(db, mv, mvPartitionName,
                    addEntry.getValue(), partitionProperties, distributionDesc);
            mvPartitionMap.put(mvPartitionName, addEntry.getValue());
            // The newly added partition must be the partition that needs to be refreshed
            needRefreshMvPartitionNames.add(mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] partitions add range [{}]", mv.getName(), adds);

        Map<String, Set<String>> baseToMvNameRef = SyncPartitionUtils
                .generatePartitionRefMap(basePartitionMap, mvPartitionMap);
        Map<String, Set<String>> mvToBaseNameRef = SyncPartitionUtils
                .generatePartitionRefMap(mvPartitionMap, basePartitionMap);
        context.setBaseToMvNameRef(baseToMvNameRef);
        context.setMvToBaseNameRef(mvToBaseNameRef);

        if (partitionExpr instanceof SlotRef) {
            Set<String> baseChangedPartitionNames = mv.getNeedRefreshPartitionNames(base);
            needRefreshMvPartitionNames.addAll(baseChangedPartitionNames);
            // The partition of base and mv is 1 to 1 relationship
            for (String mvPartitionName : adds.keySet()) {
                mv.addBasePartition(baseTableId, base.getPartition(mvPartitionName));
            }
        }  else if (partitionExpr instanceof FunctionCallExpr) {
            Set<String> addBasePartitionNames = Sets.newHashSet();
            for (String mvPartitionName : adds.keySet()) {
                addBasePartitionNames.addAll(mvToBaseNameRef.get(mvPartitionName));
            }
            for (String basePartitionName : addBasePartitionNames) {
                mv.addBasePartition(baseTableId, base.getPartition(basePartitionName));
            }
            // check if there is an import in the base table and add it to the refresh candidate
            Set<String> baseChangedPartitionNames = mv.getNeedRefreshPartitionNames(base);
            for (String baseChangedPartitionName : baseChangedPartitionNames) {
                needRefreshMvPartitionNames.addAll(baseToMvNameRef.get(baseChangedPartitionName));
            }
            SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                    baseToMvNameRef, mvToBaseNameRef);
        }
        if (!deletes.isEmpty() || !needRefreshMvPartitionNames.isEmpty()) {
            mv.cleanBasePartition(base);
        }

        LOG.info("Calculate the materialized view [{}] partitions that need to be refreshed [{}]",
                mv.getName(), needRefreshMvPartitionNames);

        return needRefreshMvPartitionNames;
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
            if (baseTablePartition != null && materializedView.needRefreshPartition(baseTableId, baseTablePartition)) {
                refreshAllPartitions = true;
            }
        }
        if (materializedView.cleanBasePartition(olapTable) > 0) {
            refreshAllPartitions = true;
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
        } catch (Exception e) {
            throw new SemanticException("Expression drop partition failed: {}, db: {}, table: {}", e.getMessage(),
                    database.getFullName(), materializedView.getName());
        } finally {
            database.writeUnlock();
        }
    }

    private void refreshMv(MvTaskRunContext context, MaterializedView mv,
                           Database db, Map<Long, OlapTable> olapTables, OlapTable partitionTable,
                           Set<String> mvRefreshPartitionNames) {
        // get table partition names
        Map<String, Set<String>> tableNamePartitionNames = Maps.newHashMap();
        for (OlapTable olapTable : olapTables.values()) {
            if (partitionTable != null && olapTable.getId() == partitionTable.getId()) {
                Set<String> needRefreshTablePartitionNames = Sets.newHashSet();
                Map<String, Set<String>> mvToBaseNameRef = context.getMvToBaseNameRef();
                for (String mvPartitionName : mvRefreshPartitionNames) {
                    needRefreshTablePartitionNames.addAll(mvToBaseNameRef.get(mvPartitionName));
                }
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
        insertStmt.setTargetPartitionNames(new PartitionNames(false, new ArrayList<>(mvRefreshPartitionNames)));
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
        execInsertStmt(db, insertStmt, context, mv,
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
                if (partition.hasData()) {
                    namePartitionInfos.put(partition.getName(),
                            new MaterializedView.BasePartitionInfo(partition.getId(),
                                    partition.getVisibleVersion()));
                }
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
        ctx.setStmtId(new AtomicInteger().incrementAndGet());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
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
                            "Base table: " + olapTableId + " partition: " + plannedPartitionName + " can not find");
                }
            }
        }
    }
}
