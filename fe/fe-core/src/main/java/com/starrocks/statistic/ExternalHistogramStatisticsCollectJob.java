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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.HistogramStatisticsUtils.batchInsertPrefixSize;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBatchInsertPrefix;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsLiteral;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildMcvJson;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildStatsTargetColumnNames;
import static com.starrocks.statistic.HistogramStatisticsUtils.createInsertStmt;
import static com.starrocks.statistic.HistogramStatisticsUtils.quoteSqlString;
import static com.starrocks.statistic.HistogramStatisticsUtils.utf8Length;
import static com.starrocks.statistic.StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;

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
        long bufferSize = batchInsertPrefixSize(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
        long bufferLimit = Math.max(1, Config.histogram_batch_insert_buffer_size);

        try {
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i);
                Type columnType = columnTypes.get(i);
                List<Expr> row;
                String rowSql;
                long rowSize;
                try {
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
                    row = buildBatchInsertRow(columnName, buckets, mcvJson);
                    rowSql = buildBatchInsertRowSql(columnName, buckets, mcvJson);
                    rowSize = utf8Length(rowSql) + (sqlBuffer.isEmpty() ? 0 : 2);
                } catch (Exception collectionFailure) {
                    flushBatchInsertOnCollectionFailure(
                            rowsBuffer, sqlBuffer, columnsBuffer, insertedColumns,
                            context, analyzeStatus, columnName, collectionFailure);
                    throw collectionFailure;
                }

                if (!rowsBuffer.isEmpty() && bufferSize + rowSize > bufferLimit) {
                    flushBatchInsert(rowsBuffer, sqlBuffer, columnsBuffer, insertedColumns, context, analyzeStatus);
                    bufferSize = batchInsertPrefixSize(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
                    rowSize = utf8Length(rowSql);
                }

                rowsBuffer.add(row);
                sqlBuffer.add(rowSql);
                columnsBuffer.add(columnName);
                bufferSize += rowSize;
                if (bufferSize >= bufferLimit) {
                    flushBatchInsert(rowsBuffer, sqlBuffer, columnsBuffer, insertedColumns, context, analyzeStatus);
                    bufferSize = batchInsertPrefixSize(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
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

    private void flushBatchInsertOnCollectionFailure(
            List<List<Expr>> rowsBuffer, List<String> sqlBuffer, List<String> columnsBuffer,
            List<String> insertedColumns, ConnectContext context, AnalyzeStatus analyzeStatus,
            String columnName, Exception collectionFailure) {
        try {
            flushBatchInsert(rowsBuffer, sqlBuffer, columnsBuffer, insertedColumns, context, analyzeStatus);
        } catch (Exception flushFailure) {
            if (flushFailure != collectionFailure) {
                collectionFailure.addSuppressed(flushFailure);
            }
            LOG.warn("Failed to flush buffered external histogram statistics after collection failed for column {}",
                    columnName, flushFailure);
        }
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
        List<String> targetColumnNames = buildStatsTargetColumnNames(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
        String columnNames = "(" + String.join(", ", targetColumnNames) + ")";
        StringBuilder builder = new StringBuilder("INSERT INTO ").append(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME)
                .append(columnNames).append(" ");

        String quoteColumName = StatisticUtils.quoting(table, columnName);

        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("tableUUID", StatisticUtils.hashTableUuidForPkStorage(table.getUUID()));

        String mcvJson = buildMcvJson(mostCommonValues);
        if (mcvJson == null) {
            context.put("mcv", "NULL");
        } else {
            context.put("mcv", quoteSqlString(mcvJson));
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
        VelocityContext context = buildBaseContext(database, table, columnName);
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

    private VelocityContext buildBaseContext(Database database, Table table, String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("columnNameStr", SqlUtils.escapeSqlString(columnName));
        context.put("catalogName", catalogName);
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        return context;
    }

    private TStatisticData getSingleHistogramResult(List<TStatisticData> results, String columnName)
            throws DdlException {
        return HistogramStatisticsUtils.getSingleHistogramResult(results, columnName, "external histogram");
    }

    private List<Expr> buildBatchInsertRow(String columnName, String buckets, String mcvJson) {
        List<Expr> row = new ArrayList<>();
        row.add(new StringLiteral(StatisticUtils.hashTableUuidForPkStorage(table.getUUID())));
        row.add(new StringLiteral(columnName));
        row.add(new StringLiteral(catalogName));
        row.add(new StringLiteral(db.getOriginName()));
        row.add(new StringLiteral(table.getName()));
        row.add(buildBucketsLiteral(buckets));
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
        values.add(buildBucketsSql(buckets));
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

        String sql = buildBatchInsertPrefix(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME) +
                String.join(", ", sqlBuffer) + ";";
        collectStatisticSync(() -> createInsertStmt(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME, rowsBuffer, sql),
                context, analyzeStatus);
        insertedColumns.addAll(columnsBuffer);
        rowsBuffer.clear();
        sqlBuffer.clear();
        columnsBuffer.clear();
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
