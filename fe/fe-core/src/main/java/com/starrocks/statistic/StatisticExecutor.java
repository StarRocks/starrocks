// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.statistic;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

public class StatisticExecutor {
    private static final Logger LOG = LogManager.getLogger(StatisticExecutor.class);

    public List<TStatisticData> queryStatisticSync(ConnectContext context,
                                                   Long dbId, Long tableId, List<String> columnNames) {
        String sql;
        BasicStatsMeta meta = GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(tableId);
        if (meta != null && meta.getType().equals(StatsConstants.AnalyzeType.FULL)) {
            Table table = null;
            if (dbId == null) {
                List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
                for (Long id : dbIds) {
                    Database db = GlobalStateMgr.getCurrentState().getDb(id);
                    if (db == null) {
                        continue;
                    }
                    table = db.getTable(tableId);
                    if (table == null) {
                        continue;
                    }
                    break;
                }
            } else {
                Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
                table = database.getTable(tableId);
            }

            if (table == null) {
                // Statistical information query is an unlocked operation,
                // so it is possible for the table to be deleted while the code is running
                return Collections.emptyList();
            }

            List<Column> columns = Lists.newArrayList();
            for (String colName : columnNames) {
                Column column = table.getColumn(colName);
                Preconditions.checkState(column != null);
                columns.add(column);
            }

            sql = StatisticSQLBuilder.buildQueryFullStatisticsSQL(dbId, tableId, columns);
        } else {
            sql = StatisticSQLBuilder.buildQuerySampleStatisticsSQL(dbId, tableId, columnNames);
        }

        return executeStatisticDQL(context, sql);
    }

    public void dropTableStatistics(ConnectContext statsConnectCtx, Long tableIds,
                                    StatsConstants.AnalyzeType analyzeType) {
        String sql = StatisticSQLBuilder.buildDropStatisticsSQL(tableIds, analyzeType);
        LOG.debug("Expire statistic SQL: {}", sql);

        boolean result = executeDML(statsConnectCtx, sql);
        if (!result) {
            LOG.warn("Execute statistic table expire fail.");
        }
    }

    public boolean dropPartitionStatistics(ConnectContext statsConnectCtx, List<Long> pids) {
        String sql = StatisticSQLBuilder.buildDropPartitionSQL(pids);
        LOG.debug("Expire partition statistic SQL: {}", sql);
        return executeDML(statsConnectCtx, sql);
    }

    public boolean dropTableInvalidPartitionStatistics(ConnectContext statsConnectCtx, List<Long> tables,
                                                    List<Long> pids) {
        String sql = StatisticSQLBuilder.buildDropTableInvalidPartitionSQL(tables, pids);
        LOG.debug("Expire invalid partition statistic SQL: {}", sql);
        return executeDML(statsConnectCtx, sql);
    }

    public List<TStatisticData> queryHistogram(ConnectContext statsConnectCtx, Long tableId, List<String> columnNames) {
        String sql = StatisticSQLBuilder.buildQueryHistogramStatisticsSQL(tableId, columnNames);
        return executeStatisticDQL(statsConnectCtx, sql);
    }

    public List<TStatisticData> queryMCV(ConnectContext statsConnectCtx, String sql) {
        return executeStatisticDQL(statsConnectCtx, sql);
    }

    public void dropHistogram(ConnectContext statsConnectCtx, Long tableId, List<String> columnNames) {
        String sql = StatisticSQLBuilder.buildDropHistogramSQL(tableId, columnNames);
        boolean result = executeDML(statsConnectCtx, sql);
        if (!result) {
            LOG.warn("Execute statistic table expire fail.");
        }
    }

    // If you call this function, you must ensure that the db lock is added
    public static Pair<List<TStatisticData>, Status> queryDictSync(Long dbId, Long tableId, String column)
            throws Exception {
        if (dbId == -1) {
            return Pair.create(Collections.emptyList(), Status.OK);
        }

        Database db = MetaUtils.getDatabase(dbId);
        Table table = MetaUtils.getTable(dbId, tableId);
        if (!table.isOlapTable()) {
            throw new SemanticException("Table '%s' is not a OLAP table", table.getName());
        }

        OlapTable olapTable = (OlapTable) table;
        long version = olapTable.getPartitions().stream().map(Partition::getVisibleVersionTime)
                .max(Long::compareTo).orElse(0L);
        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        String sql = "select cast(" + StatsConstants.STATISTIC_DICT_VERSION + " as Int), " +
                "cast(" + version + " as bigint), " +
                "dict_merge(" + StatisticUtils.quoting(column) + ") as _dict_merge_" + column +
                " from " + StatisticUtils.quoting(catalogName, db.getOriginName(), table.getName()) + " [_META_]";

        ConnectContext context = StatisticUtils.buildConnectContext();
        // The parallelism degree of low-cardinality dict collect task is uniformly set to 1 to
        // prevent collection tasks from occupying a large number of be execution threads and scan threads.
        context.getSessionVariable().setPipelineDop(1);
        context.setThreadLocalInfo();
        StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());

        ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.STATISTIC);
        StmtExecutor executor = new StmtExecutor(context, parsedStmt);
        Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
        if (!sqlResult.second.ok()) {
            return Pair.create(Collections.emptyList(), sqlResult.second);
        } else {
            return Pair.create(deserializerStatisticData(sqlResult.first), sqlResult.second);
        }
    }

    private static List<TStatisticData> deserializerStatisticData(List<TResultBatch> sqlResult) throws TException {
        List<TStatisticData> statistics = Lists.newArrayList();

        if (sqlResult.size() < 1) {
            return statistics;
        }

        int version = sqlResult.get(0).getStatistic_version();
        if (sqlResult.stream().anyMatch(d -> d.getStatistic_version() != version)) {
            return statistics;
        }

        if (version == StatsConstants.STATISTIC_DATA_VERSION
                || version == StatsConstants.STATISTIC_DICT_VERSION
                || version == StatsConstants.STATISTIC_HISTOGRAM_VERSION
                || version == StatsConstants.STATISTIC_TABLE_VERSION
                || version == StatsConstants.STATISTIC_BATCH_VERSION) {
            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
            for (TResultBatch resultBatch : sqlResult) {
                for (ByteBuffer bb : resultBatch.rows) {
                    TStatisticData sd = new TStatisticData();
                    byte[] bytes = new byte[bb.limit() - bb.position()];
                    bb.get(bytes);
                    deserializer.deserialize(sd, bytes);
                    statistics.add(sd);
                }
            }
        }

        return statistics;
    }

    public AnalyzeStatus collectStatistics(ConnectContext statsConnectCtx,
                                           StatisticsCollectJob statsJob,
                                           AnalyzeStatus analyzeStatus,
                                           boolean refreshAsync) {
        Database db = statsJob.getDb();
        Table table = statsJob.getTable();

        try {
            GlobalStateMgr.getCurrentAnalyzeMgr().registerConnection(analyzeStatus.getId(), statsConnectCtx);
            // Only update running status without edit log, make restart job status is failed
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.RUNNING);
            GlobalStateMgr.getCurrentAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);

<<<<<<< HEAD
            statsConnectCtx.setStatisticsConnection(true);
=======
>>>>>>> branch-2.5-mrs
            statsJob.collect(statsConnectCtx, analyzeStatus);
        } catch (Exception e) {
            LOG.warn("Collect statistics error ", e);
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            analyzeStatus.setEndTime(LocalDateTime.now());
            analyzeStatus.setReason(e.getMessage());
            GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
            return analyzeStatus;
        } finally {
            GlobalStateMgr.getCurrentAnalyzeMgr().unregisterConnection(analyzeStatus.getId(), false);
        }

        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        analyzeStatus.setEndTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

        // update StatisticsCache
        statsConnectCtx.setStatisticsConnection(false);
        if (statsJob.getType().equals(StatsConstants.AnalyzeType.HISTOGRAM)) {
            for (String columnName : statsJob.getColumns()) {
                HistogramStatsMeta histogramStatsMeta = new HistogramStatsMeta(db.getId(),
                        table.getId(), columnName, statsJob.getType(), analyzeStatus.getEndTime(),
                        statsJob.getProperties());
                GlobalStateMgr.getCurrentAnalyzeMgr().addHistogramStatsMeta(histogramStatsMeta);
                GlobalStateMgr.getCurrentAnalyzeMgr().refreshHistogramStatisticsCache(
                        histogramStatsMeta.getDbId(), histogramStatsMeta.getTableId(),
                        Lists.newArrayList(histogramStatsMeta.getColumn()), refreshAsync);
            }
        } else {
            BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), table.getId(),
                    statsJob.getColumns(), statsJob.getType(), analyzeStatus.getEndTime(), statsJob.getProperties());
            GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(basicStatsMeta);
            GlobalStateMgr.getCurrentAnalyzeMgr().refreshBasicStatisticsCache(
                    basicStatsMeta.getDbId(), basicStatsMeta.getTableId(), basicStatsMeta.getColumns(), refreshAsync);
        }
        return analyzeStatus;
    }

    public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
        List<TResultBatch> sqlResult = executeDQL(context, sql);
        try {
            return deserializerStatisticData(sqlResult);
        } catch (TException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    private List<TResultBatch> executeDQL(ConnectContext context, String sql) {
<<<<<<< HEAD
        StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
        ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.STATISTIC);
=======
        StatementBase parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
        ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, true, TResultSinkType.STATISTIC);
>>>>>>> branch-2.5-mrs
        StmtExecutor executor = new StmtExecutor(context, parsedStmt);
        context.setExecutor(executor);
        context.setQueryId(UUIDUtil.genUUID());
        Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
        if (!sqlResult.second.ok()) {
<<<<<<< HEAD
            throw new SemanticException("Statistics query fail | Error Message [%s] | QueryId [%s] | SQL [%s]",
=======
            throw new SemanticException("Statistics query fail | Error Message [%s] | {} | SQL [%s]",
>>>>>>> branch-2.5-mrs
                    context.getState().getErrorMessage(), DebugUtil.printId(context.getQueryId()), sql);
        } else {
            return sqlResult.first;
        }
    }

    private boolean executeDML(ConnectContext context, String sql) {
        StatementBase parsedStmt;
        try {
<<<<<<< HEAD
            parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
=======
            parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
>>>>>>> branch-2.5-mrs
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            executor.execute();
            return true;
        } catch (Exception e) {
<<<<<<< HEAD
            LOG.warn("Execute statistic DML fail | {} | SQL {}", DebugUtil.printId(context.getQueryId()), sql, e);
=======
            LOG.warn("Execute statistic DML " + sql + " fail.", e);
>>>>>>> branch-2.5-mrs
            return false;
        }
    }
}
