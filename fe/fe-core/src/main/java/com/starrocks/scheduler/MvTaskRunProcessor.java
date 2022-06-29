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
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.ExpressionPartitionUtil;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.parser.SqlParser;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MvTaskRunProcessor extends BaseTaskRunProcessor {

    public static final String MV_ID = "mvId";

    @Override
    public void processTaskRun(TaskRunContext context) {
        Map<String, String> properties = context.getProperties();
        // NOTE: need set mvId in Task's properties when creating
        long mvId = Long.parseLong(properties.get(MV_ID));
        // 0. prepare
        Database database = GlobalStateMgr.getCurrentState().getDb(context.ctx.getDatabase());
        MaterializedView materializedView = (MaterializedView) database.getTable(mvId);
        Set<Long> baseTableIds = materializedView.getBaseTableIds();
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            // must create partition when creating mv
            Preconditions.checkState(materializedView.getPartitions().size() != 0);
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                    materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
            boolean needRefresh = false;
            for (Long baseTableId : baseTableIds) {
                OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
                Map<String, MaterializedView.BasePartitionInfo> basePartitionVersions =
                        baseTableVisibleVersionMap.computeIfAbsent(baseTableId, k -> Maps.newHashMap());
                Collection<Partition> partitions = olapTable.getPartitions();
                for (Partition partition : partitions) {
                    long basePartitionId = partition.getId();
                    String partitionName = partition.getName();
                    long basePartitionVersion = partition.getVisibleVersion();
                    MaterializedView.BasePartitionInfo basePartitionInfo = basePartitionVersions.get(partitionName);
                    if (basePartitionInfo == null
                            || basePartitionInfo.getId() != basePartitionId
                            || basePartitionVersion > basePartitionInfo.getVersion()) {
                        needRefresh = true;
                        basePartitionVersions.put(partitionName,
                                new MaterializedView.BasePartitionInfo(basePartitionId, basePartitionVersion));
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
        Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs().get(0);
        Column partitionColumn = expressionRangePartitionInfo.getPartitionColumns().get(0);
        // 1. scan base table, get table id which used in partition
        Map<Long, OlapTable> olapTables = Maps.newHashMap();
        OlapTable partitionTable = null;
        for (Long baseTableId : baseTableIds) {
            OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionExpr.collect(SlotRef.class, slotRefs);
            // if partitionExpr is FunctionCallExpr, get first SlotRef
            Preconditions.checkState(slotRefs.size() == 1);
            SlotRef slotRef = slotRefs.get(0);
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(olapTable.getName())) {
                partitionTable = olapTable;
            }
            olapTables.put(baseTableId, olapTable);
        }
        // 2. sync partition with partition table, get need refresh mv partition ids
        Set<String> needRefreshPartitionNames = Sets.newHashSet();
        processPartitionWithPartitionTable(database, materializedView, partitionTable,
                partitionExpr, partitionColumn, partitionProperties,
                distributionDesc, needRefreshPartitionNames);
        // 3. collect need refresh mv partition ids
        boolean refreshAllPartitions = false;
        for (Long baseTableId : baseTableIds) {
            if (baseTableId == partitionTable.getId()) {
                continue;
            }
            // check with no partition expression related table
            OlapTable olapTable = olapTables.get(baseTableId);
            if (checkNeedRefreshPartitions(materializedView, olapTable)) {
                refreshAllPartitions = true;
            }
        }
        // if all partition need refresh
        if (needRefreshPartitionNames.size() == materializedView.getPartitions().size()) {
            refreshAllPartitions = true;
        }
        // 4. refresh mv
        if (refreshAllPartitions) {
            refreshMv(context, materializedView);
        } else {
            refreshMv(context, materializedView, partitionTable, needRefreshPartitionNames);
        }
    }

    private void processPartitionWithPartitionTable(Database database, MaterializedView materializedView,
                                                    OlapTable olapTable, Expr partitionExpr,
                                                    Column partitionColumn, Map<String, String> partitionProperties,
                                                    DistributionDesc distributionDesc,
                                                    Set<String> needRefreshPartitionNames) {
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        // used to get delete table partitions
        Map<String, Long> newBaseTableVisibleVersionMap = Maps.newHashMap();
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                (ExpressionRangePartitionInfo) materializedView.getPartitionInfo();
        RangePartitionInfo baseRangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        Map<String, MaterializedView.BasePartitionInfo> basePartitionVisionMap =
                baseTableVisibleVersionMap.computeIfAbsent(olapTable.getId(), k -> Maps.newHashMap());
        Collection<Partition> basePartitions = olapTable.getPartitions();
        for (Partition basePartition : basePartitions) {
            long basePartitionId = basePartition.getId();
            String basePartitionName = basePartition.getName();
            // record full partitions
            newBaseTableVisibleVersionMap.put(basePartitionName, Partition.PARTITION_INIT_VERSION);
            // if exists, not check
            if (basePartitionVisionMap.get(basePartitionName) != null) {
                continue;
            }
            basePartitionVisionMap.put(basePartitionName,
                    new MaterializedView.BasePartitionInfo(basePartitionId, Partition.PARTITION_INIT_VERSION));
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
            }
        }
        Set<String> deletedPartitionNames = basePartitionVisionMap.keySet().stream()
                .filter(partitionName -> newBaseTableVisibleVersionMap.get(partitionName) == null)
                .collect(Collectors.toSet());
        // record checked mv and get need refresh partition ids
        Set<String> checkedMvPartitionNames = Sets.newHashSet();
        for (String deletedPartitionName : deletedPartitionNames) {
            // if base partition drop
            Set<String> refMvPartitionNames = materializedView.getTableMvPartitionNameRefMap().get(deletedPartitionName);
            for (String refMvPartitionName : refMvPartitionNames) {
                if (checkedMvPartitionNames.contains(refMvPartitionNames)) {
                    continue;
                }
                checkedMvPartitionNames.add(refMvPartitionName);
                Set<String> refTablePartitionNames = materializedView.getMvTablePartitionNameRefMap().get(refMvPartitionName);
                if (deletedPartitionNames.containsAll(refTablePartitionNames)) {
                    dropPartition(database, materializedView, refMvPartitionName);
                } else {
                    needRefreshPartitionNames.add(refMvPartitionName);
                }
            }
            basePartitionVisionMap.remove(deletedPartitionName);
        }
        // merge need refresh mv with comparing other partitions
        for (String basePartitionName : basePartitionVisionMap.keySet()) {
            MaterializedView.BasePartitionInfo basePartitionInfo = basePartitionVisionMap.get(basePartitionName);
            Partition partition = olapTable.getPartition(basePartitionName);
            long visibleVersion = partition.getVisibleVersion();
            long partitionId = partition.getId();
            if (basePartitionInfo.getId() != partitionId || basePartitionInfo.getVersion() < visibleVersion) {
                needRefreshPartitionNames.addAll(materializedView.getTableMvPartitionNameRefMap().get(basePartitionName));
                basePartitionVisionMap.put(basePartitionName,
                        new MaterializedView.BasePartitionInfo(partitionId, visibleVersion));
            }
        }
    }

    private boolean checkNeedRefreshPartitions(MaterializedView materializedView, OlapTable olapTable) {
        boolean refreshAllPartitions = false;
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap = materializedView
                .getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Map<String, MaterializedView.BasePartitionInfo> basePartitionVisionMap =
                baseTableVisibleVersionMap.computeIfAbsent(olapTable.getId(), k -> Maps.newHashMap());
        Collection<Partition> basePartitions = olapTable.getPartitions();
        for (Partition basePartition : basePartitions) {
            long basePartitionId = basePartition.getId();
            String basePartitionName = basePartition.getName();
            long basePartitionVersion = basePartition.getVisibleVersion();
            MaterializedView.BasePartitionInfo basePartitionInfo = basePartitionVisionMap.get(basePartitionName);
            if (basePartitionInfo == null
                    || basePartitionInfo.getId() != basePartitionId
                    || basePartitionInfo.getVersion() < basePartitionVersion) {
                refreshAllPartitions = true;
                basePartitionVisionMap.put(basePartitionName,
                        new MaterializedView.BasePartitionInfo(basePartitionId, basePartitionVersion));
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
            addPartitionIdRef(materializedView, basePartitionName,
                    partitionName);
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
            dropPartitionIdRef(materializedView, mvPartitionName);
        } catch (Exception e) {
            throw new SemanticException("Expression drop partition failed: {}, db: {}, table: {}", e.getMessage(),
                    database.getFullName(), materializedView.getName());
        } finally {
            database.writeUnlock();
        }

    }

    private void addPartitionIdRef(MaterializedView materializedView, String basePartitionName,
                                   String mvPartitionName) {
        materializedView.getTableMvPartitionNameRefMap()
                .computeIfAbsent(basePartitionName, k -> Sets.newHashSet()).add(mvPartitionName);
        materializedView.getMvTablePartitionNameRefMap()
                .computeIfAbsent(mvPartitionName, k -> Sets.newHashSet()).add(basePartitionName);
    }

    private void dropPartitionIdRef(MaterializedView materializedView, String mvPartitionName) {
        Set<String> basePartitionNames = materializedView.getMvTablePartitionNameRefMap().get(mvPartitionName);
        for (String basePartitionName : basePartitionNames) {
            materializedView.getTableMvPartitionNameRefMap().get(basePartitionName).remove(mvPartitionName);
        }
        materializedView.getMvTablePartitionNameRefMap().remove(mvPartitionName);
    }

    private void refreshMv(TaskRunContext context, MaterializedView materializedView) {
        String insertIntoSql = "insert overwrite " +
                materializedView.getName() + " " +
                context.getDefinition();
        execInsertStmt(insertIntoSql, context, materializedView);
    }

    private void refreshMv(TaskRunContext context, MaterializedView materializedView, OlapTable olapTable,
                           Set<String> mvPartitionNames) {
        ConnectContext ctx = context.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(context.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase());
        ctx.getPlannerProfile().reset();
        for (String mvPartitionName : mvPartitionNames) {
            String definition = context.getDefinition();
            Set<String> basePartitionNames = materializedView.getMvTablePartitionNameRefMap().get(mvPartitionName);
            Set<String> tablePartitionNames = Sets.newHashSet();
            for (String basePartitionName : basePartitionNames) {
                tablePartitionNames.add(olapTable.getPartition(basePartitionName).getName());
            }
            QueryStatement queryStatement =
                    (QueryStatement) SqlParser.parse(definition, ctx.getSessionVariable().getSqlMode()).get(0);
            Map<String, TableRelation> tableRelations =
                    AnalyzerUtils.collectAllTableRelation(queryStatement);
            TableRelation tableRelation = tableRelations.get(olapTable.getName());
            tableRelation.setPartitionNames(
                    new PartitionNames(false, new ArrayList<>(tablePartitionNames)));
            // e.g. insert into mv partition(p1,p2) select * from table partition(p3)
            String insertIntoSql = "insert overwrite " +
                    materializedView.getName() +
                    " partition(" + mvPartitionName + ") " +
                    AST2SQL.toString(queryStatement);
            execInsertStmt(insertIntoSql, context, materializedView);
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
