// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.Coordinator;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.QueryState;
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
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StatisticExecutor {
    private static final Logger LOG = LogManager.getLogger(StatisticExecutor.class);

    private static final int STATISTIC_DATA_VERSION = 1;
    private static final int STATISTIC_DICT_VERSION = 101;

    private static final String QUERY_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_DATA_VERSION + " as INT), update_time, db_id, table_id, column_name,"
                    + " row_count, data_size, distinct_count, null_count, max, min"
                    + " FROM " + Constants.StatisticsTableName
                    + " WHERE 1 = 1";

    private static final String INSERT_STATISTIC_TEMPLATE = "INSERT INTO " + Constants.StatisticsTableName;

    private static final String INSERT_SELECT_FULL_TEMPLATE =
            "SELECT $tableId, '$columnName', $dbId, '$tableName', '$dbName', COUNT(1), "
                    + "$dataSize, $countDistinctFunction, $countNullFunction, $maxFunction, $minFunction, NOW() "
                    + "FROM $tableName";

    private static final String INSERT_SELECT_METRIC_SAMPLE_TEMPLATE =
            "SELECT $tableId, '$columnName', $dbId, '$tableName', '$dbName', COUNT(1) * $ratio, "
                    + "$dataSize * $ratio, 0, 0, '', '', NOW() "
                    + "FROM (SELECT `$columnName` FROM $tableName $hints ) as t";

    private static final String INSERT_SELECT_TYPE_SAMPLE_TEMPLATE =
            "SELECT $tableId, '$columnName', $dbId, '$tableName', '$dbName', IFNULL(SUM(t1.count), 0) * $ratio, "
                    + "       $dataSize * $ratio, $countDistinctFunction, "
                    + "       IFNULL(SUM(IF(t1.`$columnName` IS NULL, t1.count, 0)), 0) * $ratio, "
                    + "       IFNULL(MAX(t1.`$columnName`), ''), IFNULL(MIN(t1.`$columnName`), ''), NOW() "
                    + "FROM ( "
                    + "    SELECT t0.`$columnName`, COUNT(1) as count "
                    + "    FROM (SELECT `$columnName` FROM $tableName $hints) as t0 "
                    + "    GROUP BY t0.`$columnName` "
                    + ") as t1";

    private static final String DELETE_TEMPLATE = "DELETE FROM " + Constants.StatisticsTableName + " WHERE ";

    private static final String SELECT_EXPIRE_TABLE_TEMPLATE =
            "SELECT DISTINCT table_id" + " FROM " + Constants.StatisticsTableName + " WHERE 1 = 1 ";

    private static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_LOGSYSTEM_CLASS,
                "org.apache.velocity.runtime.log.Log4JLogChute");
        DEFAULT_VELOCITY_ENGINE.setProperty("runtime.log.logsystem.log4j.logger", "velocity");
    }

    public List<TStatisticData> queryStatisticSync(Long dbId, Long tableId, List<String> columnNames) throws Exception {
        String sql = buildQuerySQL(dbId, tableId, columnNames);
        Map<String, Database> dbs = Maps.newHashMap();

        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt;
        try {
            parsedStmt = parseSQL(sql, context);
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

        String sql = "select cast(" + STATISTIC_DICT_VERSION + " as Int), " +
                "cast(" + version + " as bigint), " +
                "dict_merge(" + "`" + column +
                "`) as _dict_merge_" + column +
                " from " + catalogName + "." + dbName + "." + tableName + " [_META_]";

        Map<String, Database> dbs = Maps.newHashMap();
        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt;
        try {
            parsedStmt = parseSQL(sql, context);
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

        if (version == STATISTIC_DATA_VERSION || version == STATISTIC_DICT_VERSION) {
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

    public void fullCollectStatisticSync(Long dbId, Long tableId, List<String> columnNames) throws Exception {
        collectStatisticSync(dbId, tableId, columnNames, false, 0);
    }

    public void sampleCollectStatisticSync(Long dbId, Long tableId, List<String> columnNames, long rows)
            throws Exception {
        collectStatisticSync(dbId, tableId, columnNames, true, rows);
    }

    public void collectStatisticSync(Long dbId, Long tableId, List<String> columnNames, boolean isSample, long rows)
            throws Exception {
        // split column
        for (List<String> list : Lists.partition(columnNames, splitColumnsByRows(dbId, tableId, rows, isSample))) {
            String sql;
            if (isSample) {
                sql = buildSampleInsertSQL(dbId, tableId, list, rows);
            } else {
                sql = buildFullInsertSQL(dbId, tableId, list);
            }

            LOG.debug("Collect statistic SQL: {}", sql);

            ConnectContext context = StatisticUtils.buildConnectContext();
            StatementBase parsedStmt = parseSQL(sql, context);
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                throw new DdlException(context.getState().getErrorMessage());
            }
        }
    }

    public void expireStatisticSync(List<String> tableIds) {
        StringBuilder sql = new StringBuilder(DELETE_TEMPLATE);
        sql.append(" table_id IN (").append(StringUtils.join(tableIds, ",")).append(")");
        LOG.debug("Expire statistic SQL: {}", sql);

        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt;
        try {
            parsedStmt = parseSQL(sql.toString(), context);
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
            parsedStmt = parseSQL(sql.toString(), context);
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
        boolean forceDisablePipeline = false;
        try {
            if (isLockDb) {
                lock(dbs);
            }

            Analyzer.analyze(parsedStmt, context);

            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, context).transform(
                    ((QueryStatement) parsedStmt).getQueryRelation());

            // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
            boolean isEnablePipeline = context.getSessionVariable().isEnablePipelineEngine();
            boolean canUsePipeline =
                    isEnablePipeline && ResultSink.canUsePipeLine(TResultSinkType.STATISTIC) &&
                            logicalPlan.canUsePipeline();
            forceDisablePipeline = isEnablePipeline && !canUsePipeline;
            if (forceDisablePipeline) {
                context.getSessionVariable().setEnablePipelineEngine(false);
            }

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
            if (forceDisablePipeline) {
                context.getSessionVariable().setEnablePipelineEngine(true);
            }
            if (isLockDb) {
                unLock(dbs);
            }
        }
        return execPlan;
    }

    private String buildQuerySQL(Long dbId, Long tableId, List<String> columnNames) {
        StringBuilder where = new StringBuilder(QUERY_STATISTIC_TEMPLATE);
        if (null != dbId) {
            where.append(" AND db_id = ").append(dbId);
        }

        if (null != tableId) {
            where.append(" AND table_id = ").append(tableId);
        }

        if (null == columnNames || columnNames.isEmpty()) {
            return where.toString();
        }

        where.append(" AND column_name");
        if (columnNames.size() == 1) {
            where.append(" = '").append(columnNames.get(0)).append("'");
        } else {
            where.append(" IN (");
            where.append(columnNames.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")));
            where.append(")");
        }

        return where.toString();
    }

    public static StatementBase parseSQL(String sql, ConnectContext context) {
        StatementBase parsedStmt = com.starrocks.sql.parser.SqlParser.parse(sql,
                context.getSessionVariable().getSqlMode()).get(0);
        parsedStmt.setOrigStmt(new OriginStatement(sql, 0));
        return parsedStmt;
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

    private String buildFullInsertSQL(Long dbId, Long tableId, List<String> columnNames) {
        StringBuilder builder = new StringBuilder(INSERT_STATISTIC_TEMPLATE).append(" ");

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        OlapTable table = (OlapTable) db.getTable(tableId);

        for (String name : columnNames) {
            VelocityContext context = new VelocityContext();
            Column column = table.getColumn(name);

            context.put("dbId", dbId);
            context.put("tableId", tableId);
            context.put("columnName", name);
            context.put("dbName", db.getFullName());
            context.put("tableName", ClusterNamespace.getNameFromFullName(db.getFullName()) + "." + table.getName());
            context.put("dataSize", getDataSize(column, false));

            if (!column.getType().canStatistic()) {
                context.put("countDistinctFunction", "0");
                context.put("countNullFunction", "0");
                context.put("maxFunction", "''");
                context.put("minFunction", "''");
            } else {
                context.put("countDistinctFunction", "approx_count_distinct(`" + name + "`)");
                context.put("countNullFunction", "COUNT(1) - COUNT(`" + name + "`)");
                context.put("maxFunction", "IFNULL(MAX(`" + name + "`), '')");
                context.put("minFunction", "IFNULL(MIN(`" + name + "`), '')");
            }

            StringWriter sw = new StringWriter();
            DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", INSERT_SELECT_FULL_TEMPLATE);

            builder.append(sw);
            builder.append(" UNION ALL ");
        }

        return builder.substring(0, builder.length() - "UNION ALL ".length());
    }

    private String buildSampleInsertSQL(Long dbId, Long tableId, List<String> columnNames, long rows) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        OlapTable table = (OlapTable) db.getTable(tableId);

        long hitRows = 1;
        long totalRows = 0;
        long totalTablet = 0;
        Set<String> randomTablets = Sets.newHashSet();
        rows = Math.max(rows, 1);

        // calculate the number of tablets by each partition
        // simpleTabletNums = simpleRows / partitionNums / (actualPartitionRows / actualTabletNums)
        long avgRowsPerPartition = rows / Math.max(table.getPartitions().size(), 1);

        for (Partition p : table.getPartitions()) {
            List<Long> ids = p.getBaseIndex().getTabletIdsInOrder();

            if (ids.isEmpty()) {
                continue;
            }

            if (p.getBaseIndex().getRowCount() < (avgRowsPerPartition / 2)) {
                continue;
            }

            long avgRowsPerTablet = Math.max(p.getBaseIndex().getRowCount() / ids.size(), 1);
            long tabletCounts = Math.max(avgRowsPerPartition / avgRowsPerTablet, 1);
            tabletCounts = Math.min(tabletCounts, ids.size());

            for (int i = 0; i < tabletCounts; i++) {
                randomTablets.add(String.valueOf(ids.get(i)));
            }

            hitRows += avgRowsPerTablet * tabletCounts;
            totalRows += p.getBaseIndex().getRowCount();
            totalTablet += ids.size();
        }

        long ratio = Math.max(totalRows / Math.min(hitRows, rows), 1);
        // all hit, direct full
        String hintTablets;
        if (randomTablets.isEmpty() || totalRows < rows) {
            // can't fill full sample rows
            return buildFullInsertSQL(dbId, tableId, columnNames);
        } else if (randomTablets.size() == totalTablet) {
            hintTablets = " LIMIT " + rows;
        } else {
            hintTablets = " Tablet(" + String.join(", ", randomTablets) + ")" + " LIMIT " + rows;
        }

        StringBuilder builder = new StringBuilder(INSERT_STATISTIC_TEMPLATE).append(" ");

        Set<String> lowerDistributeColumns =
                table.getDistributionColumnNames().stream().map(String::toLowerCase).collect(Collectors.toSet());

        for (String name : columnNames) {
            VelocityContext context = new VelocityContext();
            Column column = table.getColumn(name);

            context.put("dbId", dbId);
            context.put("tableId", tableId);
            context.put("columnName", name);
            context.put("dbName", db.getFullName());
            context.put("tableName", ClusterNamespace.getNameFromFullName(db.getFullName()) + "." + table.getName());
            context.put("dataSize", getDataSize(column, true));
            context.put("ratio", ratio);
            context.put("hints", hintTablets);

            // countDistinctFunction
            if (lowerDistributeColumns.size() == 1 && lowerDistributeColumns.contains(name.toLowerCase())) {
                context.put("countDistinctFunction", "COUNT(1) * " + ratio);
            } else {
                // From PostgreSQL: n*d / (n - f1 + f1*n/N)
                // (https://github.com/postgres/postgres/blob/master/src/backend/commands/analyze.c)
                // and paper: ESTIMATING THE NUMBER OF CLASSES IN A FINITE POPULATION
                // (http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.93.8637&rep=rep1&type=pdf)
                // sample_row * count_distinct / ( sample_row - once_count + once_count * sample_row / total_row)
                String sampleRows = "SUM(t1.count)";
                String onceCount = "SUM(IF(t1.count = 1, 1, 0))";
                String countDistinct = "COUNT(1)";

                String fn = MessageFormat.format("{0} * {1} / ({0} - {2} + {2} * {0} / {3})", sampleRows,
                        countDistinct, onceCount, String.valueOf(totalRows));
                context.put("countDistinctFunction", "IFNULL(" + fn + ", COUNT(1))");
            }

            StringWriter sw = new StringWriter();

            if (!column.getType().canStatistic()) {
                DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", INSERT_SELECT_METRIC_SAMPLE_TEMPLATE);
            } else {
                DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", INSERT_SELECT_TYPE_SAMPLE_TEMPLATE);
            }

            builder.append(sw);
            builder.append(" UNION ALL ");
        }

        return builder.substring(0, builder.length() - "UNION ALL ".length());
    }

    private String getDataSize(Column column, boolean isSample) {
        if (column.getPrimitiveType().isCharFamily() || column.getPrimitiveType().isJsonType()) {
            if (isSample) {
                return "IFNULL(SUM(CHAR_LENGTH(`" + column.getName() + "`) * t1.count), 0)";
            }
            return "IFNULL(SUM(CHAR_LENGTH(`" + column.getName() + "`)), 0)";
        }

        long typeSize = column.getType().getTypeSize();

        if (isSample && column.getType().canStatistic()) {
            return "IFNULL(SUM(t1.count), 0) * " + typeSize;
        }
        return "COUNT(1) * " + typeSize;
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
