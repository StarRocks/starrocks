// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.Coordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StatisticExecutor {
    private static final Logger LOG = LogManager.getLogger(StatisticExecutor.class);


    private static final String DELETE_TEMPLATE = "DELETE FROM " + StatsConstants.SampleStatisticsTableName + " WHERE ";

    private static final String SELECT_EXPIRE_TABLE_TEMPLATE =
            "SELECT DISTINCT table_id" + " FROM " + StatsConstants.SampleStatisticsTableName + " WHERE 1 = 1 ";

    public List<TStatisticData> queryStatisticSync(Long dbId, Long tableId, List<String> columnNames) throws Exception {
        String sql;
        if (Config.enable_collect_full_statistics) {
            BasicStatsMeta meta = GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(tableId);
            if (meta != null && meta.getType().equals(StatsConstants.AnalyzeType.FULL)) {
                sql = StatisticSQLBuilder.buildQueryFullStatisticsSQL(tableId, columnNames);
            } else {
                sql = StatisticSQLBuilder.buildQuerySampleStatisticsSQL(dbId, tableId, columnNames);
            }
        } else {
            sql = StatisticSQLBuilder.buildQuerySampleStatisticsSQL(dbId, tableId, columnNames);
        }
        Map<String, Database> dbs = Maps.newHashMap();

        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt;
        try {
            parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
            if (parsedStmt instanceof QueryStatement) {
                dbs = AnalyzerUtils.collectAllDatabase(context, parsedStmt);
            }
        } catch (Exception e) {
            LOG.warn("Parse statistic table query fail.", e);
            throw e;
        }

        try {
            ExecPlan execPlan = getExecutePlan(dbs, context, parsedStmt, true, true);
            List<TResultBatch> sqlResult = executeStmt(context, execPlan).first;
            return deserializerStatisticData(sqlResult);
        } catch (Exception e) {
            LOG.warn("Execute statistic table query fail.", e);
            throw e;
        }
    }

    public List<TStatisticData> queryHistogram(Long tableId, List<String> columnNames) throws Exception {
        String sql = StatisticSQLBuilder.buildQueryHistogramStatisticsSQL(tableId, columnNames);
        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
        try {
            ExecPlan execPlan = getExecutePlan(Maps.newHashMap(), context, parsedStmt, true, true);
            List<TResultBatch> sqlResult = executeStmt(context, execPlan).first;
            return deserializerStatisticData(sqlResult);
        } catch (Exception e) {
            LOG.warn("Execute statistic table query fail.", e);
            throw e;
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

        OlapTable olapTable = (OlapTable) table;
        long version = olapTable.getPartitions().stream().map(Partition::getVisibleVersionTime)
                .max(Long::compareTo).orElse(0L);
        String dbName = ClusterNamespace.getNameFromFullName(db.getFullName());
        String tableName = db.getTable(tableId).getName();
        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

        String sql = "select cast(" + StatsConstants.STATISTIC_DICT_VERSION + " as Int), " +
                "cast(" + version + " as bigint), " +
                "dict_merge(" + "`" + column +
                "`) as _dict_merge_" + column +
                " from " + catalogName + "." + dbName + "." + tableName + " [_META_]";

        Map<String, Database> dbs = Maps.newHashMap();
        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt;
        try {
            parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
            if (parsedStmt instanceof QueryStatement) {
                dbs = AnalyzerUtils.collectAllDatabase(context, parsedStmt);
            }
            Preconditions.checkState(dbs.size() == 1);
        } catch (Exception e) {
            LOG.warn("Parse statistic dict query {} fail.", sql, e);
            throw e;
        }

        try {
            ExecPlan execPlan = getExecutePlan(dbs, context, parsedStmt, true, false);
            Pair<List<TResultBatch>, Status> sqlResult = executeStmt(context, execPlan);
            if (!sqlResult.second.ok()) {
                return Pair.create(Collections.emptyList(), sqlResult.second);
            } else {
                return Pair.create(deserializerStatisticData(sqlResult.first), sqlResult.second);
            }
        } catch (Exception e) {
            LOG.warn("Execute statistic dict query {} fail.", sql, e);
            throw e;
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

    public AnalyzeStatus collectStatistics(StatisticsCollectJob statsJob) {
        Database db = statsJob.getDb();
        Table table = statsJob.getTable();
        List<String> columns = statsJob.getColumns();

        AnalyzeStatus analyzeStatus = new AnalyzeStatus(
                GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), columns,
                statsJob.getType(), statsJob.getScheduleType(), statsJob.getProperties(),
                LocalDateTime.now());
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

        try {
            statsJob.collect();
        } catch (Exception e) {
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            analyzeStatus.setEndTime(LocalDateTime.now());
            analyzeStatus.setReason(e.getMessage());
            GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
            return analyzeStatus;
        }
        GlobalStateMgr.getCurrentStatisticStorage().expireColumnStatistics(table, columns);

        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        analyzeStatus.setEndTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        if (statsJob.getType().equals(StatsConstants.AnalyzeType.HISTOGRAM)) {
            for (String columnName : statsJob.getColumns()) {
                GlobalStateMgr.getCurrentAnalyzeMgr().addHistogramStatsMeta(new HistogramStatsMeta(db.getId(),
                        table.getId(), columnName, statsJob.getType(), analyzeStatus.getEndTime(),
                        statsJob.getProperties()));
            }
        } else {
            GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(db.getId(), table.getId(),
                    statsJob.getType(), analyzeStatus.getEndTime(), statsJob.getProperties()));
        }
        return analyzeStatus;
    }

    public void expireStatisticSync(List<String> tableIds) {
        StringBuilder sql = new StringBuilder(DELETE_TEMPLATE);
        sql.append(" table_id IN (").append(StringUtils.join(tableIds, ",")).append(")");
        LOG.debug("Expire statistic SQL: {}", sql);

        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt;
        try {
            parsedStmt = SqlParser.parseFirstStatement(sql.toString(), context.getSessionVariable().getSqlMode());
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            executor.execute();
        } catch (Exception e) {
            LOG.warn("Execute statistic table expire fail.", e);
        }
    }

    public List<String> queryExpireTableSync(List<Long> tableIds) throws Exception {
        if (null == tableIds || tableIds.isEmpty()) {
            return Collections.emptyList();
        }

        StringBuilder sql = new StringBuilder(SELECT_EXPIRE_TABLE_TEMPLATE);
        sql.append(" AND table_id NOT IN (").append(StringUtils.join(tableIds, ",")).append(")");
        LOG.debug("Query expire statistic SQL: {}", sql);

        Map<String, Database> dbs = Maps.newHashMap();
        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt;
        try {
            parsedStmt = SqlParser.parseFirstStatement(sql.toString(), context.getSessionVariable().getSqlMode());
            if (parsedStmt instanceof QueryStatement) {
                dbs = AnalyzerUtils.collectAllDatabase(context, parsedStmt);
            }
        } catch (Exception e) {
            LOG.warn("Parse statistic table query fail. SQL: " + sql, e);
            throw e;
        }

        try {
            ExecPlan execPlan = getExecutePlan(dbs, context, parsedStmt, false, true);
            List<TResultBatch> sqlResult = executeStmt(context, execPlan).first;

            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

            List<String> result = Lists.newArrayList();
            for (TResultBatch batch : sqlResult) {
                for (ByteBuffer byteBuffer : batch.getRows()) {
                    result.add(decoder.decode(byteBuffer).toString().substring(1));
                }
            }

            return result;
        } catch (Exception e) {
            LOG.warn("Execute statistic table query fail.", e);
            throw e;
        }
    }

    private static ExecPlan getExecutePlan(Map<String, Database> dbs, ConnectContext context,
                                           StatementBase parsedStmt, boolean isStatistic, boolean isLockDb) {
        ExecPlan execPlan;
        try {
            if (isLockDb) {
                lock(dbs);
            }

            Analyzer.analyze(parsedStmt, context);

            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, context).transform(
                    ((QueryStatement) parsedStmt).getQueryRelation());

            Optimizer optimizer = new Optimizer();
            OptExpression optimizedPlan = optimizer.optimize(
                    context,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);

            execPlan = new PlanFragmentBuilder()
                    .createStatisticPhysicalPlan(optimizedPlan, context, logicalPlan.getOutputColumn(),
                            columnRefFactory, isStatistic);
        } finally {
            if (isLockDb) {
                unLock(dbs);
            }
        }
        return execPlan;
    }

    private static Pair<List<TResultBatch>, Status> executeStmt(ConnectContext context, ExecPlan plan)
            throws Exception {
        Coordinator coord =
                new Coordinator(context, plan.getFragments(), plan.getScanNodes(), plan.getDescTbl().toThrift());
        QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(), coord);
        List<TResultBatch> sqlResult = Lists.newArrayList();
        try {
            coord.exec();
            RowBatch batch;
            do {
                batch = coord.getNext();
                if (batch.getBatch() != null) {
                    sqlResult.add(batch.getBatch());
                }
            } while (!batch.isEos());
        } catch (Exception e) {
            LOG.warn(e);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
        }
        return Pair.create(sqlResult, coord.getExecStatus());
    }

    private int splitColumnsByRows(Long dbId, Long tableId, long rows, boolean isSample) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        OlapTable table = (OlapTable) db.getTable(tableId);

        long count =
                table.getPartitions().stream().map(p -> p.getBaseIndex().getRowCount()).reduce(Long::sum).orElse(1L);
        if (isSample) {
            count = Math.min(count, rows);
        }
        count = Math.max(count, 1L);

        // 500w data per query
        return (int) (5000000L / count + 1);
    }


    // Lock all database before analyze
    private static void lock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readLock();
        }
    }

    // unLock all database after analyze
    private static void unLock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readUnlock();
        }
    }
}
