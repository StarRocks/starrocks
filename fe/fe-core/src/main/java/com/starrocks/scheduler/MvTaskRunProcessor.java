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
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.ViewDefBuilder;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ExpressionPartitionUtil;
import com.starrocks.sql.common.PartitionDiff;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        Set<Long> baseTableIds = materializedView.getBaseTableIds();
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            // must create partition when creating mv
            Preconditions.checkState(materializedView.getPartitions().size() != 0);
            boolean needRefresh = false;
            for (Long baseTableId : baseTableIds) {
                OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
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
                refreshMv(context, materializedView);
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
        Map<Long, OlapTable> olapTables = Maps.newHashMap();
        OlapTable partitionTable = null;
        List<SlotRef> slotRefs = Lists.newArrayList();
        partitionExpr.collect(SlotRef.class, slotRefs);
        // if partitionExpr is FunctionCallExpr, get first SlotRef
        Preconditions.checkState(slotRefs.size() == 1);
        SlotRef slotRef = slotRefs.get(0);
        for (Long baseTableId : baseTableIds) {
            OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(olapTable.getName())) {
                partitionTable = olapTable;
            }
            olapTables.put(baseTableId, olapTable);
        }
        // 2. sync partition with partition table, get need refresh mv partition ids
        Set<String> needRefreshPartitionNames = processSyncBasePartition(database, materializedView,
                partitionTable, partitionExpr, partitionProperties, distributionDesc);
        // 3. collect need refresh mv partition ids
        boolean refreshAllPartitions = false;
        for (Long baseTableId : baseTableIds) {
            if (baseTableId == partitionTable.getId()) {
                continue;
            }
            // check with no partition expression related table
            OlapTable olapTable = olapTables.get(baseTableId);
            if (checkNoPartitionRefTablePartitions(materializedView, olapTable)) {
                refreshAllPartitions = true;
            }
        }
        if (materializedView.getPartitions().isEmpty()) {
            return;
        }
        // if all partition need refresh
        if (needRefreshPartitionNames.size() == materializedView.getPartitions().size()) {
            refreshAllPartitions = true;
        }
        // 4. refresh mv
        if (refreshAllPartitions) {
            refreshMv(context, materializedView);
        } else {
            if (!needRefreshPartitionNames.isEmpty()) {
                refreshMv(context, materializedView, partitionTable, needRefreshPartitionNames);
            }
        }
    }

    private Set<String> processSyncBasePartition(Database db, MaterializedView mv, OlapTable base,
                                                 Expr partitionExpr, Map<String, String> partitionProperties,
                                                 DistributionDesc distributionDesc) {
        long baseTableId = base.getId();

        Map<String, Range<PartitionKey>> basePartitionMap = base.getRangePartitionMap();
        Map<String, Range<PartitionKey>> mvPartitionMap = mv.getRangePartitionMap();

        PartitionDiff partitionDiff = new PartitionDiff();
        if (partitionExpr instanceof SlotRef) {
            partitionDiff = ExpressionPartitionUtil.calcSyncSamePartition(basePartitionMap, mvPartitionMap);
        } else if (partitionExpr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
            String functionName = functionCallExpr.getFnName().getFunction();
            if (!functionName.equalsIgnoreCase("date_trunc")) {
                throw new SemanticException("Do not support function:" + functionCallExpr.toSql());
            }
            String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue();
            partitionDiff = ExpressionPartitionUtil.calcSyncRollupPartition(basePartitionMap, mvPartitionMap,
                    granularity);
        }

        Map<String, Range<PartitionKey>> adds = partitionDiff.getAdds();
        Map<String, Range<PartitionKey>> deletes = partitionDiff.getDeletes();
        Set<String> needRefreshPartitionNames = Sets.newHashSet();

        // We should delete the old partition first and then add the new one,
        // because the old and new partitions may overlap

        for (Map.Entry<String, Range<PartitionKey>> deleteEntry : deletes.entrySet()) {
            String partitionName = deleteEntry.getKey();
            mv.removeBasePartition(baseTableId, partitionName);
            dropPartition(db, mv, partitionName);
        }
        LOG.info("The process of synchronizing partitions delete range {}", deletes);

        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String partitionName = addEntry.getKey();
            mv.addBasePartition(baseTableId, base.getPartition(partitionName));
            addPartition(db, mv, partitionName,
                    addEntry.getValue(), partitionProperties, distributionDesc);
            needRefreshPartitionNames.add(partitionName);
        }
        LOG.info("The process of synchronizing partitions add range {} ", adds);

        return needRefreshPartitionNames;
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
                materializedView.updateBasePartition(baseTableId, baseTablePartition);
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

    private void refreshMv(TaskRunContext context, MaterializedView materializedView) {
        execInsertStmt(context.getDefinition(), context, materializedView);
    }

    private void refreshMv(TaskRunContext context, MaterializedView mv, OlapTable base,
                           Set<String> mvPartitionNames) {
        ConnectContext ctx = context.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(context.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase());
        ctx.getPlannerProfile().reset();

        Map<String, Range<PartitionKey>> mvPartitionMap = mv.getRangePartitionMap();
        Map<String, Range<PartitionKey>> basePartitionMap = base.getRangePartitionMap();

        for (String mvPartitionName : mvPartitionNames) {
            String definition = context.getDefinition();
            Range<PartitionKey> mvPartition = mvPartitionMap.get(mvPartitionName);

            Set<String> tablePartitionNames = Sets.newHashSet();
            for (Map.Entry<String, Range<PartitionKey>> rangeEntry : basePartitionMap.entrySet()) {
                Range<PartitionKey> basePartition = rangeEntry.getValue();
                // inclusion relation
                int lowerCmp = basePartition.lowerEndpoint().compareTo(mvPartition.lowerEndpoint());
                int upperCmp = basePartition.upperEndpoint().compareTo(mvPartition.upperEndpoint());
                if (lowerCmp >= 0 && upperCmp <= 0) {
                    tablePartitionNames.add(rangeEntry.getKey());
                }
            }

            QueryStatement queryStatement =
                    (QueryStatement) SqlParser.parse(definition, ctx.getSessionVariable().getSqlMode()).get(0);
            Map<String, TableRelation> tableRelations =
                    AnalyzerUtils.collectAllTableRelation(queryStatement);
            TableRelation tableRelation = tableRelations.get(base.getName());
            tableRelation.setPartitionNames(
                    new PartitionNames(false, new ArrayList<>(tablePartitionNames)));
            Analyzer.analyze(queryStatement, ctx);
            // e.g. insert into mv partition(p1) select * from table partition(p2,p3)
            String insertIntoSql = "insert overwrite " +
                    mv.getName() +
                    " partition(" + mvPartitionName + ") " +
                    ViewDefBuilder.build(queryStatement);
            execInsertStmt(insertIntoSql, context, mv);
            ctx.setQueryId(UUIDUtil.genUUID());
        }
    }

    private void execInsertStmt(String insertSql, TaskRunContext context, MaterializedView materializedView) {
        ConnectContext ctx = context.getCtx();
        InsertStmt insertStmt = ((InsertStmt) SqlParser.parse(insertSql, ctx.getSessionVariable().getSqlMode()).get(0));
        insertStmt.setSystem(true);
        insertStmt.setOrigStmt(new OriginStatement(insertSql, 0));
        StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
        ctx.setExecutor(executor);
        ctx.setThreadLocalInfo();
        try {
            executor.execute();
        } catch (Exception e) {
            throw new SemanticException("Refresh materialized view failed:" + insertSql, e);
        } finally {
            materializedView.getRefreshScheme().setLastRefreshTime(Utils.getLongFromDateTime(LocalDateTime.now()));
            auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
        }
    }

}
