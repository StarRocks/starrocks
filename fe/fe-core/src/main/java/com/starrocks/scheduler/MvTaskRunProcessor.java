// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

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
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.sql.common.ExpressionPartitionUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.ResourceGroupMetricMgr;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.SqlDigestBuilder;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MvTaskRunProcessor implements TaskRunProcessor {

    private static final Logger LOG = LogManager.getLogger(MvTaskRunProcessor.class);

    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        Map<String, String> properties = context.getProperties();
        // NOTE: need set mvId in Task's properties when creating
        long mvId = Long.parseLong(properties.get("mvId"));
        // 0. prepare
        Database database = GlobalStateMgr.getCurrentState().getDb(context.ctx.getDatabase());
        MaterializedView materializedView = (MaterializedView) database.getTable(mvId);
        Set<Long> baseTableIds = materializedView.getBaseTableIds();
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            Map<Long, Map<Long, Long>> baseTableVisibleVersionMap =
                    materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
            for (Long baseTableId : baseTableIds) {
                OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
                Map<Long, Long> basePartitionVersions =
                        baseTableVisibleVersionMap.computeIfAbsent(baseTableId, k -> Maps.newHashMap());
                Collection<Partition> partitions = olapTable.getPartitions();
                boolean needRefresh = false;
                for (Partition partition : partitions) {
                    long basePartitionId = partition.getId();
                    long basePartitionVersion = partition.getVisibleVersion();
                    Long oldVersion = basePartitionVersions.get(basePartitionId);
                    if (oldVersion == null || basePartitionVersion > oldVersion.longValue()) {
                        needRefresh = true;
                        basePartitionVersions.put(basePartitionId, basePartitionVersion);
                    }
                }
                if (needRefresh) {
                    refreshMv(context, materializedView);
                }
            }
            return;
        }
        Map<String, String> partitionProperties = getPartitionProperties(materializedView);
        DistributionDesc distributionDesc = getDistributionDesc(materializedView);
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                ((ExpressionRangePartitionInfo) partitionInfo);
        // currently, mv only supports one expression
        Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs().get(0);
        Column partitionColumn = expressionRangePartitionInfo.getPartitionColumns().get(0);
        // 1. scan base table, get table id which used in partition
        Map<Long, OlapTable> olapTables = Maps.newHashMap();
        OlapTable partitionTable = null;
        for (Long baseTableId : baseTableIds) {
            OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
            SlotRef slotRef;
            if (partitionExpr instanceof SlotRef) {
                slotRef = ((SlotRef) partitionExpr);
            } else {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
                slotRef = getFirstSlotRefInFunction(functionCallExpr);
            }
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(olapTable.getName())) {
                partitionTable = olapTable;
            }
            olapTables.put(baseTableId, olapTable);
        }
        // 2. sync partition with partition table, get need refresh mv partition ids
        Set<Long> needRefreshPartitionIds = Sets.newHashSet();
        processPartitionWithPartitionTable(database, materializedView, partitionTable,
                partitionExpr, partitionColumn, partitionProperties,
                distributionDesc, needRefreshPartitionIds);
        // 3. collect need refresh mv partition ids
        boolean refreshAllPartitions = false;
        for (Long baseTableId : baseTableIds) {
            if (baseTableId == partitionTable.getId()) {
                continue;
            }
            OlapTable olapTable = olapTables.get(baseTableId);
            if (checkNeedRefreshPartitions(materializedView, olapTable)) {
                refreshAllPartitions = true;
            }
        }
        // 4. refresh mv
        if (refreshAllPartitions) {
            refreshMv(context, materializedView);
        } else {
            refreshMv(context, materializedView, partitionTable, needRefreshPartitionIds);
        }
    }

    public SlotRef getFirstSlotRefInFunction(FunctionCallExpr expr) {
        ArrayList<Expr> children = expr.getChildren();
        for (Expr child : children) {
            if (child instanceof SlotRef) {
                return (SlotRef) child;
            }
        }
        throw new SemanticException("Create find SlotRef in FunctionCallExpr:" + expr.toSql());
    }

    private void processPartitionWithPartitionTable(Database database, MaterializedView materializedView,
                                                    OlapTable olapTable, Expr partitionExpr,
                                                    Column partitionColumn, Map<String, String> partitionProperties,
                                                    DistributionDesc distributionDesc,
                                                    Set<Long> needRefreshPartitionIds) {
        Map<Long, Set<Long>> partitionIdRefMap = materializedView.getPartitionIdRefMap();
        Map<Long, Map<Long, Long>> baseTableVisibleVersionMap = materializedView.getRefreshScheme()
                .getAsyncRefreshContext()
                .getBaseTableVisibleVersionMap();
        // used to get delete table partitions
        Map<Long, Long> newBaseTableVisibleVersionMap = Maps.newHashMap();
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                (ExpressionRangePartitionInfo) materializedView.getPartitionInfo();
        RangePartitionInfo baseRangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        Map<Long, Long> basePartitionVisionMap =
                baseTableVisibleVersionMap.computeIfAbsent(olapTable.getId(), k -> Maps.newHashMap());
        Collection<Partition> basePartitions = olapTable.getPartitions();
        for (Partition basePartition : basePartitions) {
            long basePartitionId = basePartition.getId();
            // record full partitions
            newBaseTableVisibleVersionMap.put(basePartitionId, Partition.PARTITION_INIT_VERSION);
            // if exists, not check
            if (basePartitionVisionMap.get(basePartitionId) != null) {
                continue;
            }
            basePartitionVisionMap.put(basePartitionId, Partition.PARTITION_INIT_VERSION);
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
                PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(
                        Collections.singletonList(new PartitionValue(
                                mvPartitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue())),
                        Collections.singletonList(new PartitionValue(
                                mvPartitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue())));
                addPartition(database, materializedView, basePartitionId,
                        partitionKeyDesc, partitionProperties, distributionDesc);
            }
        }
        Set<Long> deletedPartitionIds = basePartitionVisionMap.keySet().stream()
                .filter(partitionId -> newBaseTableVisibleVersionMap.get(partitionId) == null)
                .collect(Collectors.toSet());
        // record checked mv and get need refresh partition ids
        Set<Long> checkedMvPartitionIds = Sets.newHashSet();
        for (Long deletedPartitionId : deletedPartitionIds) {
            Set<Long> refMvPartitionIds = partitionIdRefMap.get(deletedPartitionId);
            for (Long refMvPartitionId : refMvPartitionIds) {
                if (checkedMvPartitionIds.contains(refMvPartitionId)) {
                    continue;
                }
                checkedMvPartitionIds.add(refMvPartitionId);
                Set<Long> refTablePartitionIds = partitionIdRefMap.get(refMvPartitionId);
                if (deletedPartitionIds.containsAll(refTablePartitionIds)) {
                    dropPartition(database, materializedView, refMvPartitionId);
                } else {
                    needRefreshPartitionIds.add(refMvPartitionId);
                }
            }
            basePartitionVisionMap.remove(deletedPartitionId);
        }
        // merge need refresh mv with comparing partition version
        for (Long basePartitionId : basePartitionVisionMap.keySet()) {
            Long oldVisibleVersion = basePartitionVisionMap.get(basePartitionId);
            long visibleVersion = olapTable.getPartition(basePartitionId).getVisibleVersion();
            if (visibleVersion > oldVisibleVersion) {
                needRefreshPartitionIds.addAll(partitionIdRefMap.get(basePartitionId));
                basePartitionVisionMap.put(basePartitionId, visibleVersion);
            }
        }
    }

    private boolean checkNeedRefreshPartitions(MaterializedView materializedView, OlapTable olapTable) {
        boolean refreshAllPartitions = false;
        Map<Long, Map<Long, Long>> baseTableVisibleVersionMap = materializedView.getRefreshScheme()
                .getAsyncRefreshContext()
                .getBaseTableVisibleVersionMap();
        Map<Long, Long> basePartitionVisionMap =
                baseTableVisibleVersionMap.computeIfAbsent(olapTable.getId(), k -> Maps.newHashMap());
        Collection<Partition> basePartitions = olapTable.getPartitions();
        for (Partition basePartition : basePartitions) {
            long basePartitionId = basePartition.getId();
            long basePartitionVersion = basePartition.getVisibleVersion();
            Long oldVersion = basePartitionVisionMap.get(basePartitionId);
            if (oldVersion == null || basePartitionVersion > oldVersion.longValue()) {
                refreshAllPartitions = true;
                basePartitionVisionMap.put(basePartitionId, basePartitionVersion);
            }
        }
        return refreshAllPartitions;
    }

    private void refreshMv(TaskRunContext context, MaterializedView materializedView) {
        ConnectContext ctx = context.getCtx();
        StringBuilder insertSqlBuilder = new StringBuilder();
        insertSqlBuilder.append("insert into ");
        insertSqlBuilder.append(materializedView.getName() + " ");
        insertSqlBuilder.append(context.getDefinition());
        String insertSql = insertSqlBuilder.toString();
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
            if (executor != null) {
                auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
            } else {
                // executor can be null if we encounter analysis error.
                auditAfterExec(context, null, null);
            }
        }
    }

    private void refreshMv(TaskRunContext context, MaterializedView materializedView, OlapTable olapTable,
                           Set<Long> mvPartitionIds) {
        Map<Long, Set<Long>> partitionIdRefMap = materializedView.getPartitionIdRefMap();
        ConnectContext ctx = context.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(context.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase());
        ctx.getPlannerProfile().reset();
        for (Long mvPartitionId : mvPartitionIds) {
            String definition = context.getDefinition();
            Set<Long> basePartitionIds = partitionIdRefMap.get(mvPartitionId);
            Set<String> tablePartitionNames = Sets.newHashSet();
            for (Long basePartitionId : basePartitionIds) {
                tablePartitionNames.add(olapTable.getPartition(basePartitionId).getName());
            }
            String mvPartitionName = materializedView.getPartition(mvPartitionId).getName();
            QueryStatement queryStatement =
                    (QueryStatement) SqlParser.parse(definition, ctx.getSessionVariable().getSqlMode()).get(0);
            Map<String, TableRelation> tableRelations =
                    AnalyzerUtils.collectAllTableRelation(queryStatement);
            TableRelation tableRelation = tableRelations.get(olapTable.getName());
            tableRelation.setPartitionNames(
                    new PartitionNames(false, tablePartitionNames.stream().collect(Collectors.toList())));
            // e.g. insert into mv partition(p1,p2) select * from table partition(p3)
            StringBuilder insertSqlBuilder = new StringBuilder();
            insertSqlBuilder.append("insert into ");
            insertSqlBuilder.append(materializedView.getName());
            insertSqlBuilder.append(" partition(" + mvPartitionName + ") ");
            insertSqlBuilder.append(AST2SQL.toString(queryStatement));
            String insertIntoSql = insertSqlBuilder.toString();
            InsertStmt insertStmt = ((InsertStmt) SqlParser.parse(insertIntoSql,
                    ctx.getSessionVariable().getSqlMode()).get(0));
            insertStmt.setOrigStmt(new OriginStatement(insertIntoSql, 0));
            insertStmt.setSystem(true);
            StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
            ctx.setExecutor(executor);
            ctx.setThreadLocalInfo();
            try {
                executor.execute();
            } catch (Exception e) {
                throw new SemanticException("Refresh materialized view failed:" + insertIntoSql, e);
            } finally {
                if (executor != null) {
                    auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
                } else {
                    // executor can be null if we encounter analysis error.
                    auditAfterExec(context, null, null);
                }
            }
        }
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

    private void addPartition(Database database, MaterializedView materializedView, long basePartitionId,
                              PartitionKeyDesc partitionKeyDesc, Map<String, String> partitionProperties,
                              DistributionDesc distributionDesc) {
        // todo need to design partition name?
        String partitionName = "mv_" + GlobalStateMgr.getCurrentState().getNextId();
        SingleRangePartitionDesc singleRangePartitionDesc =
                new SingleRangePartitionDesc(false, partitionName, partitionKeyDesc, partitionProperties);
        try {
            GlobalStateMgr.getCurrentState().addPartitions(
                    database, materializedView.getName(),
                    new AddPartitionClause(singleRangePartitionDesc, distributionDesc,
                            partitionProperties, false));
            addPartitionIdRef(materializedView.getPartitionIdRefMap(), basePartitionId,
                    materializedView.getPartition(partitionName).getId());
        } catch (Exception e) {
            throw new SemanticException("Expression add partition failed: {}, db: {}, table: {}", e.getMessage(),
                    database.getFullName(), materializedView.getName());
        }
    }

    private void dropPartition(Database database, MaterializedView materializedView, long mvPartitionId) {
        String dropPartitionName = materializedView.getPartition(mvPartitionId).getName();
        database.writeLock();
        try {
            GlobalStateMgr.getCurrentState().dropPartition(
                    database, materializedView,
                    new DropPartitionClause(false, dropPartitionName, false, true));
            dropPartitionIdRef(materializedView.getPartitionIdRefMap(), mvPartitionId);
        } catch (Exception e) {
            throw new SemanticException("Expression drop partition failed: {}, db: {}, table: {}", e.getMessage(),
                    database.getFullName(), materializedView.getName());
        } finally {
            database.writeUnlock();
        }

    }

    private void addPartitionIdRef(Map<Long, Set<Long>> partitionIdRefMap, long basePartitionId, long mvPartitionId) {
        partitionIdRefMap.computeIfAbsent(basePartitionId, k -> Sets.newHashSet()).add(mvPartitionId);
        partitionIdRefMap.computeIfAbsent(mvPartitionId, k -> Sets.newHashSet()).add(basePartitionId);
    }

    private void dropPartitionIdRef(Map<Long, Set<Long>> partitionIdRefMap, long mvPartitionId) {
        Set<Long> basePartitionIds = partitionIdRefMap.get(mvPartitionId);
        for (Long basePartitionId : basePartitionIds) {
            partitionIdRefMap.get(basePartitionId).remove(mvPartitionId);
        }
        partitionIdRefMap.remove(mvPartitionId);
    }

    private void auditAfterExec(TaskRunContext context, StatementBase parsedStmt, PQueryStatistics statistics) {
        String origStmt = context.getDefinition();
        ConnectContext ctx = context.getCtx();

        // slow query
        long endTime = System.currentTimeMillis();
        long elapseMs = endTime - ctx.getStartTime();

        ctx.getAuditEventBuilder().setEventType(AuditEvent.EventType.AFTER_QUERY)
                .setState(ctx.getState().toString()).setErrorCode(ctx.getErrorCode()).setQueryTime(elapseMs)
                .setScanBytes(statistics == null ? 0 : statistics.scanBytes)
                .setScanRows(statistics == null ? 0 : statistics.scanRows)
                .setCpuCostNs(statistics == null || statistics.cpuCostNs == null ? 0 : statistics.cpuCostNs)
                .setMemCostBytes(statistics == null || statistics.memCostBytes == null ? 0 : statistics.memCostBytes)
                .setReturnRows(ctx.getReturnRows())
                .setStmtId(ctx.getStmtId())
                .setQueryId(ctx.getQueryId() == null ? "NaN" : ctx.getQueryId().toString());

        if (ctx.getState().isQuery()) {
            MetricRepo.COUNTER_QUERY_ALL.increase(1L);
            ResourceGroupMetricMgr.increaseQuery(ctx, 1L);
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                // err query
                MetricRepo.COUNTER_QUERY_ERR.increase(1L);
                ResourceGroupMetricMgr.increaseQueryErr(ctx, 1L);
            } else {
                // ok query
                MetricRepo.COUNTER_QUERY_SUCCESS.increase(1L);
                MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
                if (elapseMs > Config.qe_slow_log_ms || ctx.getSessionVariable().isEnableSQLDigest()) {
                    MetricRepo.COUNTER_SLOW_QUERY.increase(1L);
                    ctx.getAuditEventBuilder().setDigest(computeStatementDigest(parsedStmt));
                }
            }
            ctx.getAuditEventBuilder().setIsQuery(true);
        } else {
            ctx.getAuditEventBuilder().setIsQuery(false);
        }

        ctx.getAuditEventBuilder().setFeIp(FrontendOptions.getLocalHostAddress());

        // We put origin query stmt at the end of audit log, for parsing the log more convenient.
        if (!ctx.getState().isQuery() && (parsedStmt != null && parsedStmt.needAuditEncryption())) {
            ctx.getAuditEventBuilder().setStmt(parsedStmt.toSql());
        } else if (ctx.getState().isQuery() && containsComment(origStmt)) {
            // avoid audit log can't replay
            ctx.getAuditEventBuilder().setStmt(origStmt);
        } else {
            ctx.getAuditEventBuilder().setStmt(origStmt.replace("\n", " "));
        }

        GlobalStateMgr.getCurrentAuditEventProcessor().handleAuditEvent(ctx.getAuditEventBuilder().build());
    }

    public String computeStatementDigest(StatementBase queryStmt) {
        if (queryStmt == null) {
            return "";
        }
        String digest;
        if (queryStmt instanceof QueryStmt) {
            digest = ((QueryStmt) queryStmt).toDigest();
        } else {
            digest = SqlDigestBuilder.build(queryStmt);
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.reset();
            md.update(digest.getBytes());
            return Hex.encodeHexString(md.digest());
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

    private boolean containsComment(String sql) {
        return (sql.contains("--")) || sql.contains("#");
    }
}
