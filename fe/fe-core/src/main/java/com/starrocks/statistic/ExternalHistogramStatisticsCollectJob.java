// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.statistic;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME;

public class ExternalHistogramStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalHistogramStatisticsCollectJob.class);

    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT '$tableUUID', '$columnNameStr', '$catalogName', '$dbName', '$tableName'," +
                    " histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double)), " +
                    " $mcv," +
                    " NOW()" +
                    " FROM (SELECT $columnName as column_key FROM `$catalogName`.`$dbName`.`$tableName`" +
                    " where rand() <= $sampleRatio" +
                    " and $columnName is not null $MCVExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

    private static final String QUERY_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT)," +
                    " '$columnNameStr'," +
                    " histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double))" +
                    " FROM (SELECT $columnName as column_key FROM `$catalogName`.`$dbName`.`$tableName`" +
                    " where rand() <= $sampleRatio" +
                    " and $columnName is not null $MCVExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

    // For char-family columns we skip the histogram() bucket aggregate, but we still need
    // Histogram.getTotalRows() to reflect the column's real cardinality. So instead of storing
    // NULL buckets we store a single placeholder bucket that represents "all values excluding
    // the MCVs".
    private static final String COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE =
            "SELECT '$tableUUID', '$columnNameStr', '$catalogName', '$dbName', '$tableName'," +
                    " $bucketExpr, $mcv, NOW()" +
                    " FROM `$catalogName`.`$dbName`.`$tableName`";

    private static final String QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT)," +
                    " '$columnNameStr', $bucketExpr" +
                    " FROM `$catalogName`.`$dbName`.`$tableName`";

    private static final String COLLECT_MCV_STATISTIC_TEMPLATE =
            "select cast(version as INT), " +
                    "cast(column_key as varchar), cast(column_value as varchar) from (" +
                    "select " + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as version, " +
                    "$columnName as column_key, " +
                    "count($columnName) as column_value " +
                    "from `$catalogName`.`$dbName`.`$tableName` where $columnName is not null " +
                    "group by $columnName " +
                    "order by column_value desc limit $topN ) t";

    private final String catalogName;

    public ExternalHistogramStatisticsCollectJob(String catalogName, Database db, Table table, List<String> columnNames,
                                                 List<Type> columnTypes, StatsConstants.AnalyzeType type,
                                                 StatsConstants.ScheduleType scheduleType,
                                                 Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties);
        this.catalogName = catalogName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getName() {
        return "ExternalHistogram";
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        context.getSessionVariable().setNewPlanerAggStage(1);

        double sampleRatio = Double.parseDouble(properties.get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));
        long bucketNum = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_BUCKET_NUM));
        long mcvSize = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_MCV_SIZE));

        if (Config.enable_batch_insert_histogram_statistics && columnNames.size() > 1) {
            collectBatched(context, analyzeStatus, sampleRatio, bucketNum, mcvSize);
        } else {
            collectLegacy(context, analyzeStatus, sampleRatio, bucketNum, mcvSize);
        }
    }

    private void collectLegacy(ConnectContext context, AnalyzeStatus analyzeStatus, double sampleRatio, long bucketNum,
                               long mcvSize) throws Exception {
        long finishedSQLNum = 0;
        long totalCollectSQL = columnNames.size();

        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            String sql = buildCollectMCV(db, table, mcvSize, columnName);
            StatisticExecutor statisticExecutor = new StatisticExecutor();
            List<TStatisticData> mcv = statisticExecutor.queryMCV(context, sql);

            Map<String, String> mostCommonValues = new HashMap<>();
            for (TStatisticData tStatisticData : mcv) {
                mostCommonValues.put(tStatisticData.columnName, tStatisticData.histogram);
            }

            sql = buildCollectHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, columnName, columnType);
            collectStatisticSync(sql, context, analyzeStatus);
            // Best-effort: remove the stale raw-keyed row this column's fresh hashed-keyed row just
            // superseded. The read side no longer depends on this for correctness (it dedups by
            // update_time), so this is purely storage hygiene - failures are logged, not fatal.
            if (!statisticExecutor.dropExternalHistogramRawColumn(context, table.getUUID(), columnName)) {
                LOG.warn("[ExternalStats] failed to clean up stale raw-keyed histogram row | catalog={} db={} table={} " +
                        "column={}", catalogName, db.getOriginName(), table.getName(), columnName);
            }

            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    private void collectBatched(ConnectContext context, AnalyzeStatus analyzeStatus, double sampleRatio, long bucketNum,
                                long mcvSize) throws Exception {
        List<List<Expr>> rowsBuffer = new ArrayList<>();
        List<String> sqlBuffer = new ArrayList<>();
        List<String> columnsBuffer = new ArrayList<>();
        List<String> insertedColumns = new ArrayList<>();
        long bufferSize = batchInsertPrefixSize();
        long bufferLimit = Math.max(1, Config.histogram_batch_insert_buffer_size);

        try {
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i);
                Type columnType = columnTypes.get(i);
                List<TStatisticData> mcv = queryStatisticSync(
                        buildCollectMCV(db, table, mcvSize, columnName), context, analyzeStatus);
                Map<String, String> mostCommonValues = new HashMap<>();
                for (TStatisticData tStatisticData : mcv) {
                    mostCommonValues.put(tStatisticData.columnName, tStatisticData.histogram);
                }

                String histogramQuery = buildQueryHistogram(
                        db, table, sampleRatio, bucketNum, mostCommonValues, columnName, columnType);
                String buckets = getSingleHistogramResult(
                        queryStatisticSync(histogramQuery, context, analyzeStatus), columnName).histogram;
                String mcvJson = buildMcvJson(mostCommonValues);
                List<Expr> row = buildBatchInsertRow(columnName, buckets, mcvJson);
                String rowSql = buildBatchInsertRowSql(columnName, buckets, mcvJson);
                long rowSize = utf8Length(rowSql) + (sqlBuffer.isEmpty() ? 0 : 2);

                if (!rowsBuffer.isEmpty() && bufferSize + rowSize > bufferLimit) {
                    flushBatchInsert(rowsBuffer, sqlBuffer, columnsBuffer, insertedColumns, context, analyzeStatus);
                    bufferSize = batchInsertPrefixSize();
                    rowSize = utf8Length(rowSql);
                }

                rowsBuffer.add(row);
                sqlBuffer.add(rowSql);
                columnsBuffer.add(columnName);
                bufferSize += rowSize;
                if (bufferSize >= bufferLimit) {
                    flushBatchInsert(rowsBuffer, sqlBuffer, columnsBuffer, insertedColumns, context, analyzeStatus);
                    bufferSize = batchInsertPrefixSize();
                }

                analyzeStatus.setProgress((i + 1) * 99L / columnNames.size());
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
            }

            flushBatchInsert(rowsBuffer, sqlBuffer, columnsBuffer, insertedColumns, context, analyzeStatus);
        } finally {
            cleanupInsertedRawHistogramRows(context, insertedColumns);
        }

        analyzeStatus.setProgress(100);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
    }

    private void cleanupInsertedRawHistogramRows(ConnectContext context, List<String> insertedColumns) {
        if (insertedColumns.isEmpty()) {
            return;
        }
        try {
            StatisticExecutor statisticExecutor = new StatisticExecutor();
            if (!statisticExecutor.dropExternalHistogramRawColumns(context, table.getUUID(), insertedColumns)) {
                LOG.warn("[ExternalStats] failed to clean up stale raw-keyed histogram rows | catalog={} db={} " +
                                "table={} columns={}",
                        catalogName, db.getOriginName(), table.getName(), insertedColumns);
            }
        } catch (Exception e) {
            LOG.warn("[ExternalStats] failed to clean up stale raw-keyed histogram rows | catalog={} db={} table={} " +
                            "columns={}",
                    catalogName, db.getOriginName(), table.getName(), insertedColumns, e);
        }
    }

    private String buildCollectMCV(Database database, Table table, Long topN, String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("catalogName", catalogName);
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", topN);

        return build(context, COLLECT_MCV_STATISTIC_TEMPLATE);
    }

    private String buildCollectHistogram(Database database, Table table, double sampleRatio,
                                         Long bucketNum, Map<String, String> mostCommonValues, String columnName,
                                         Type columnType) {
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());
        String columnNames = "(" + String.join(", ", targetColumnNames) + ")";
        StringBuilder builder = new StringBuilder("INSERT INTO ").append(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME)
                .append(columnNames).append(" ");

        String quoteColumName = StatisticUtils.quoting(table, columnName);

        VelocityContext context = new VelocityContext();
        context.put("tableUUID", StatisticUtils.hashTableUuidForPkStorage(table.getUUID()));
        context.put("columnName", quoteColumName);
        context.put("columnNameStr", StringEscapeUtils.escapeSql(columnName));
        context.put("catalogName", catalogName);
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());

        String mcvJson = buildMcvJson(mostCommonValues);
        if (mcvJson == null) {
            context.put("mcv", "NULL");
        } else {
            context.put("mcv", "'" + StringEscapeUtils.escapeSql(mcvJson) + "'");
        }

        putMcvExclude(context, mostCommonValues, quoteColumName, columnType);

        if (shouldSkipHistogramBuckets(columnType)) {
            long mcvSum = mostCommonValues.values().stream().mapToLong(Long::parseLong).sum();
            context.put("bucketExpr",
                    "concat('[[\"Infinity\",\"Infinity\",', cast(greatest(0, count(" + quoteColumName +
                            ") - " + mcvSum + ") as varchar), ',0]]')");
            builder.append(build(context, COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE));
            return builder.toString();
        }

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);

        builder.append(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
        return builder.toString();
    }

    private String buildQueryHistogram(Database database, Table table, double sampleRatio, Long bucketNum,
                                       Map<String, String> mostCommonValues, String columnName, Type columnType) {
        String quoteColumnName = StatisticUtils.quoting(table, columnName);
        VelocityContext context = new VelocityContext();
        context.put("columnName", quoteColumnName);
        context.put("columnNameStr", StringEscapeUtils.escapeSql(columnName));
        context.put("catalogName", catalogName);
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        putMcvExclude(context, mostCommonValues, quoteColumnName, columnType);

        if (shouldSkipHistogramBuckets(columnType)) {
            long mcvSum = mostCommonValues.values().stream().mapToLong(Long::parseLong).sum();
            context.put("bucketExpr",
                    "concat('[[\"Infinity\",\"Infinity\",', cast(greatest(0, count(" + quoteColumnName +
                            ") - " + mcvSum + ") as varchar), ',0]]')");
            return build(context, QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE);
        }

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        return build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }

    private String buildMcvJson(Map<String, String> mostCommonValues) {
        if (mostCommonValues.isEmpty()) {
            return null;
        }

        List<String> mcvList = new ArrayList<>();
        for (Map.Entry<String, String> entry : mostCommonValues.entrySet()) {
            mcvList.add("[\"" + entry.getKey() + "\",\"" + entry.getValue() + "\"]");
        }
        return "[" + Joiner.on(",").join(mcvList) + "]";
    }

    private TStatisticData getSingleHistogramResult(List<TStatisticData> results, String columnName)
            throws DdlException {
        if (results.size() != 1) {
            throw new DdlException("Expected exactly one external histogram result for column " + columnName +
                    ", but got " + results.size());
        }
        if (Strings.isNullOrEmpty(results.get(0).histogram)) {
            throw new DdlException("Expected a non-empty external histogram result for column " + columnName);
        }
        return results.get(0);
    }

    private List<Expr> buildBatchInsertRow(String columnName, String buckets, String mcvJson) {
        List<Expr> row = new ArrayList<>();
        row.add(new StringLiteral(StatisticUtils.hashTableUuidForPkStorage(table.getUUID())));
        row.add(new StringLiteral(columnName));
        row.add(new StringLiteral(catalogName));
        row.add(new StringLiteral(db.getOriginName()));
        row.add(new StringLiteral(table.getName()));
        row.add(new StringLiteral(buckets));
        row.add(mcvJson == null ? new NullLiteral() : new StringLiteral(mcvJson));
        row.add(nowFn());
        return row;
    }

    private String buildBatchInsertRowSql(String columnName, String buckets, String mcvJson) {
        List<String> values = new ArrayList<>();
        values.add(quoteSqlString(StatisticUtils.hashTableUuidForPkStorage(table.getUUID())));
        values.add(quoteSqlString(columnName));
        values.add(quoteSqlString(catalogName));
        values.add(quoteSqlString(db.getOriginName()));
        values.add(quoteSqlString(table.getName()));
        values.add(quoteSqlString(buckets));
        values.add(mcvJson == null ? "NULL" : quoteSqlString(mcvJson));
        values.add("NOW()");
        return "(" + String.join(", ", values) + ")";
    }

    private void flushBatchInsert(List<List<Expr>> rowsBuffer, List<String> sqlBuffer, List<String> columnsBuffer,
                                  List<String> insertedColumns, ConnectContext context, AnalyzeStatus analyzeStatus)
            throws Exception {
        if (rowsBuffer.isEmpty()) {
            return;
        }

        List<String> targetColumnNames =
                StatisticUtils.buildStatsColumnDef(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME).stream()
                        .map(ColumnDef::getName)
                        .collect(Collectors.toList());
        String sql = buildBatchInsertPrefix() + String.join(", ", sqlBuffer) + ";";
        QueryStatement queryStatement = new QueryStatement(new ValuesRelation(rowsBuffer, targetColumnNames));
        TableRef tableRef = new TableRef(
                QualifiedName.of(Lists.newArrayList(STATISTICS_DB_NAME, EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME)),
                null, NodePosition.ZERO);
        InsertStmt insertStmt = new InsertStmt(tableRef, queryStatement);
        insertStmt.setTargetColumnNames(targetColumnNames);
        insertStmt.setOrigStmt(new OriginStatement(sql, 0));

        collectStatisticSync(insertStmt, sql, context, analyzeStatus);
        insertedColumns.addAll(columnsBuffer);
        rowsBuffer.clear();
        sqlBuffer.clear();
        columnsBuffer.clear();
    }

    private long batchInsertPrefixSize() {
        return utf8Length(buildBatchInsertPrefix()) + 1;
    }

    private String buildBatchInsertPrefix() {
        List<String> targetColumnNames =
                StatisticUtils.buildStatsColumnDef(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME).stream()
                        .map(ColumnDef::getName)
                        .collect(Collectors.toList());
        return "INSERT INTO " + STATISTICS_DB_NAME + "." + EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME +
                "(" + String.join(", ", targetColumnNames) + ") VALUES ";
    }

    private static String quoteSqlString(String value) {
        return "'" + StringEscapeUtils.escapeSql(value) + "'";
    }

    private static long utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private void putMcvExclude(VelocityContext context, Map<String, String> mostCommonValues, String quoteColumName,
                               Type columnType) {
        if (!mostCommonValues.isEmpty()) {
            if (columnType.getPrimitiveType().isDateType() || columnType.getPrimitiveType().isCharFamily()) {
                context.put("MCVExclude", " and " + quoteColumName + " not in (\"" +
                        Joiner.on("\",\"").join(mostCommonValues.keySet()) + "\")");
            } else {
                context.put("MCVExclude", " and " + quoteColumName + " not in (" +
                        Joiner.on(",").join(mostCommonValues.keySet()) + ")");
            }
        } else {
            context.put("MCVExclude", "");
        }
    }
}
