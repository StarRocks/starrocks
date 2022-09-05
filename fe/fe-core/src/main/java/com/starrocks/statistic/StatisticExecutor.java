// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.statistic;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
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

    public List<TStatisticData> queryStatisticSync(Long dbId, Long tableId, List<String> columnNames) throws Exception {
        String sql;
        BasicStatsMeta meta = GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(tableId);
        if (meta != null && meta.getType().equals(StatsConstants.AnalyzeType.FULL)) {
            Table table = null;
            if (dbId == null) {
                List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
                for (Long id : dbIds) {
                    Database db = GlobalStateMgr.getCurrentState().getDb(id);
                    table = db.getTable(tableId);
                    if (table != null) {
                        break;
                    }
                }
            } else {
                Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
                table = database.getTable(tableId);
            }
            Preconditions.checkState(table != null);

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

        return executeDQL(sql);
    }

    public void dropTableStatistics(Long tableIds, StatsConstants.AnalyzeType analyzeType) {
        String sql = StatisticSQLBuilder.buildDropStatisticsSQL(tableIds, analyzeType);
        LOG.debug("Expire statistic SQL: {}", sql);

        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt;
        try {
            parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            executor.execute();
        } catch (Exception e) {
            LOG.warn("Execute statistic table expire fail.", e);
        }
    }

    public List<TStatisticData> queryHistogram(Long tableId, List<String> columnNames) {
        String sql = StatisticSQLBuilder.buildQueryHistogramStatisticsSQL(tableId, columnNames);
        return executeDQL(sql);
    }

    public List<TStatisticData> queryMCV(String sql) {
        return executeDQL(sql);
    }

    public void dropHistogram(Long tableId, List<String> columnNames) {
        String sql = StatisticSQLBuilder.buildDropHistogramSQL(tableId, columnNames);
        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt;
        try {
            parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            executor.execute();
        } catch (Exception e) {
            LOG.warn("Execute statistic table expire fail.", e);
        }
    }

    // If you call this function, you must ensure that the db lock is added
    public static Pair<List<TStatisticData>, Status> queryDictSync(Long dbId, Long tableId, String column)
            throws Exception {
        if (dbId == -1) {
            return Pair.create(Collections.emptyList(), Status.OK);
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        Table table = db.getTable(tableId);

        if (!table.isOlapTable()) {
            throw new SemanticException("Table '%s' is not a OLAP table", table.getName());
        }

        OlapTable olapTable = (OlapTable) table;
        long version = olapTable.getPartitions().stream().map(Partition::getVisibleVersionTime)
                .max(Long::compareTo).orElse(0L);
        String dbName = db.getOriginName();
        String tableName = db.getTable(tableId).getName();
        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

        String sql = "select cast(" + StatsConstants.STATISTIC_DICT_VERSION + " as Int), " +
                "cast(" + version + " as bigint), " +
                "dict_merge(" + "`" + column +
                "`) as _dict_merge_" + column +
                " from " + catalogName + "." + dbName + "." + tableName + " [_META_]";


        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());

        ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, false, TResultSinkType.STATISTIC);
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
                || version == StatsConstants.STATISTIC_HISTOGRAM_VERSION) {
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

    public AnalyzeStatus collectStatistics(StatisticsCollectJob statsJob, AnalyzeStatus analyzeStatus, boolean refreshAsync) {
        Database db = statsJob.getDb();
        Table table = statsJob.getTable();

        //Only update running status without edit log, make restart job status is failed
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);

        try {
            ConnectContext context = StatisticUtils.buildConnectContext();
            GlobalStateMgr.getCurrentAnalyzeMgr().registerConnection(analyzeStatus.getId(), context);
            statsJob.collect(context, analyzeStatus);
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

    private List<TStatisticData> executeDQL(String sql) {
        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
        ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, true, TResultSinkType.STATISTIC);
        StmtExecutor executor = new StmtExecutor(context, parsedStmt);
        Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
        if (!sqlResult.second.ok()) {
            throw new SemanticException(sqlResult.second.getErrorMsg());
        } else {
            try {
                return deserializerStatisticData(sqlResult.first);
            } catch (TException e) {
                throw new SemanticException(e.getMessage());
            }
        }
    }
}
