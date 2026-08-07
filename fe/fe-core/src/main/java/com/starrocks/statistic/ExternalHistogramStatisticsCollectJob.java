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
import com.starrocks.common.Pair;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    // A single bucket replaces the histogram() aggregate in two cases: char-family columns (whose buckets we
    // cannot build) and jobs whose histogram_stats_scope excludes buckets. Either way we still need
    // Histogram.getTotalRows() to reflect the column's real cardinality, so instead of storing NULL buckets we
    // store one bucket representing "all values excluding the MCVs". Its bounds are the sampled min/max when
    // the column type can carry them, and the INFINITE_BOUND placeholder otherwise.
    private static final String COLLECT_SINGLE_BUCKET_STATISTIC_TEMPLATE =
            "SELECT '$tableUUID', '$columnNameStr', '$catalogName', '$dbName', '$tableName'," +
                    " $bucketExpr, $mcv, NOW()" +
                    " FROM `$catalogName`.`$dbName`.`$tableName`";

    private static final String QUERY_SINGLE_BUCKET_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT)," +
                    " '$columnNameStr', $bucketExpr" +
                    " FROM `$catalogName`.`$dbName`.`$tableName`";

    // Bounds of the single bucket stored when the bucket aggregate is skipped. Three result columns, because
    // that is what the external-histogram statistic result writer expects (version, varchar, varchar), so the
    // min lands in TStatisticData.columnName and the max in TStatisticData.histogram.
    private static final String SAMPLE_MIN_MAX_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT)," +
                    " cast($minFunction as varchar), cast($maxFunction as varchar)" +
                    " FROM (SELECT $columnName as column_key" +
                    " FROM `$catalogName`.`$dbName`.`$tableName`" +
                    " where rand() <= $sampleRatio and $columnName is not null" +
                    " LIMIT $totalRows) t";

    private static final String INFINITE_BOUND = "Infinity";

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
        String statScope = properties.getOrDefault(StatsConstants.HISTOGRAM_STATS_SCOPE,
                StatsConstants.HISTOGRAM_STATS_SCOPE_BOTH);
        boolean collectMcv = statScope.equalsIgnoreCase(StatsConstants.HISTOGRAM_STATS_SCOPE_MCV)
                || statScope.equalsIgnoreCase(StatsConstants.HISTOGRAM_STATS_SCOPE_BOTH);
        boolean collectBuckets = statScope.equalsIgnoreCase(StatsConstants.HISTOGRAM_STATS_SCOPE_BUCKETS)
                || statScope.equalsIgnoreCase(StatsConstants.HISTOGRAM_STATS_SCOPE_BOTH);

        if (Config.enable_batch_insert_histogram_statistics && columnNames.size() > 1) {
            collectBatched(context, analyzeStatus, sampleRatio, bucketNum, mcvSize, collectMcv, collectBuckets);
        } else {
            collectLegacy(context, analyzeStatus, sampleRatio, bucketNum, mcvSize, collectMcv, collectBuckets);
        }
    }

    private void collectLegacy(ConnectContext context, AnalyzeStatus analyzeStatus, double sampleRatio, long bucketNum,
                               long mcvSize, boolean collectMcv, boolean collectBuckets) throws Exception {
        long finishedSQLNum = 0;
        long totalCollectSQL = columnNames.size();

        StatisticExecutor statisticExecutor = new StatisticExecutor();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);

            Map<String, String> mostCommonValues = collectMcv
                    ? collectMostCommonValues(context, statisticExecutor, mcvSize, columnName)
                    : Collections.emptyMap();

            String sql;
            if (collectBuckets && !shouldSkipHistogramBuckets(columnType)) {
                sql = buildCollectHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, columnName, columnType);
            } else {
                Optional<Pair<String, String>> minMax =
                        sampleColumnMinMax(context, statisticExecutor, columnName, columnType, sampleRatio);
                sql = buildCollectSingleBucket(db, table, mostCommonValues, columnName, minMax);
            }
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
                                long mcvSize, boolean collectMcv, boolean collectBuckets) throws Exception {
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
                    Map<String, String> mostCommonValues = collectMcv
                            ? buildMostCommonValues(queryStatisticSync(
                                    buildCollectMCV(db, table, mcvSize, columnName), context, analyzeStatus))
                            : Collections.emptyMap();

                    String bucketQuery = buildBatchedBucketQuery(
                            context, analyzeStatus, sampleRatio, bucketNum, mostCommonValues, columnName, columnType,
                            collectBuckets);
                    String buckets = getSingleHistogramResult(
                            queryStatisticSync(bucketQuery, context, analyzeStatus), columnName).histogram;
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

    // The batched path needs the bucket value as a literal, so it queries the buckets instead of computing them
    // inside the INSERT. Which query to run is decided here - the single decision point shared with collectLegacy.
    private String buildBatchedBucketQuery(ConnectContext context, AnalyzeStatus analyzeStatus, double sampleRatio,
                                           long bucketNum, Map<String, String> mostCommonValues, String columnName,
                                           Type columnType, boolean collectBuckets) throws DdlException {
        if (collectBuckets && !shouldSkipHistogramBuckets(columnType)) {
            return buildQueryHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, columnName, columnType);
        }

        Optional<Pair<String, String>> minMax =
                queryColumnMinMax(context, analyzeStatus, columnName, columnType, sampleRatio);
        return buildQuerySingleBucket(db, table, mostCommonValues, columnName, minMax);
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

    private Map<String, String> collectMostCommonValues(ConnectContext context, StatisticExecutor statisticExecutor,
                                                        long mcvSize, String columnName) {
        String sql = buildCollectMCV(db, table, mcvSize, columnName);
        return buildMostCommonValues(statisticExecutor.queryMCV(context, sql));
    }

    private Map<String, String> buildMostCommonValues(List<TStatisticData> mcv) {
        Map<String, String> mostCommonValues = new HashMap<>();
        for (TStatisticData tStatisticData : mcv) {
            mostCommonValues.put(tStatisticData.columnName, tStatisticData.histogram);
        }
        return mostCommonValues;
    }

    // Legacy path: the bounds query runs through StatisticExecutor directly.
    private Optional<Pair<String, String>> sampleColumnMinMax(ConnectContext context,
                                                              StatisticExecutor statisticExecutor, String columnName,
                                                              Type columnType, double sampleRatio) {
        if (!canCarrySampledBounds(columnType)) {
            return Optional.empty();
        }

        String sql = buildSampleMinMax(db, table, columnName, columnType, sampleRatio);
        return parseSampledBounds(statisticExecutor.executeStatisticDQL(context, sql), columnName, columnType);
    }

    // Batched path: same query, but routed through queryStatisticSync so analyze cancellation and the remaining
    // timeout are still honoured.
    private Optional<Pair<String, String>> queryColumnMinMax(ConnectContext context, AnalyzeStatus analyzeStatus,
                                                             String columnName, Type columnType, double sampleRatio)
            throws DdlException {
        if (!canCarrySampledBounds(columnType)) {
            return Optional.empty();
        }

        String sql = buildSampleMinMax(db, table, columnName, columnType, sampleRatio);
        return parseSampledBounds(queryStatisticSync(sql, context, analyzeStatus), columnName, columnType);
    }

    private Optional<Pair<String, String>> parseSampledBounds(List<TStatisticData> sampled, String columnName,
                                                             Type columnType) {
        if (sampled.isEmpty()) {
            return Optional.empty();
        }

        // The sql result is parsed and min and max values are stored
        // in the variables named below.
        String minValue = sampled.get(0).columnName;
        String maxValue = sampled.get(0).histogram;

        if (StringUtils.isBlank(minValue) || StringUtils.isBlank(maxValue)) {
            LOG.info("[ExternalStats] unusable sampled bounds, falling back to placeholder bucket | catalog={} db={} " +
                            "table={} column={} min={} max={}", catalogName, db.getOriginName(), table.getName(), columnName,
                    minValue, maxValue);
            return Optional.empty();
        }
        return Optional.of(Pair.create(minValue, maxValue));
    }

    private static boolean canCarrySampledBounds(Type columnType) {
        return columnType.getPrimitiveType().isNumericType() || columnType.getPrimitiveType().isDateType();
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

    private String buildSampleMinMax(Database database, Table table, String columnName, Type columnType,
                                     double sampleRatio) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("minFunction", getMinMaxFunction(columnType, "`column_key`", false));
        context.put("maxFunction", getMinMaxFunction(columnType, "`column_key`", true));
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);

        return build(context, SAMPLE_MIN_MAX_TEMPLATE);
    }

    private String buildCollectSingleBucket(Database database, Table table, Map<String, String> mostCommonValues,
                                            String columnName, Optional<Pair<String, String>> minMax) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        putMcv(context, mostCommonValues);
        putSingleBucketExpr(context, table, mostCommonValues, columnName, minMax);

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_SINGLE_BUCKET_STATISTIC_TEMPLATE));
    }

    private String buildQuerySingleBucket(Database database, Table table, Map<String, String> mostCommonValues,
                                          String columnName, Optional<Pair<String, String>> minMax) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        putSingleBucketExpr(context, table, mostCommonValues, columnName, minMax);

        return build(context, QUERY_SINGLE_BUCKET_STATISTIC_TEMPLATE);
    }

    private String buildCollectHistogram(Database database, Table table, double sampleRatio,
                                         Long bucketNum, Map<String, String> mostCommonValues, String columnName,
                                         Type columnType) {
        String quoteColumName = StatisticUtils.quoting(table, columnName);

        VelocityContext context = buildBaseContext(database, table, columnName);
        putMcv(context, mostCommonValues);
        putMcvExclude(context, mostCommonValues, quoteColumName, columnType);

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
    }

    private String buildQueryHistogram(Database database, Table table, double sampleRatio, Long bucketNum,
                                       Map<String, String> mostCommonValues, String columnName, Type columnType) {
        String quoteColumnName = StatisticUtils.quoting(table, columnName);
        VelocityContext context = buildBaseContext(database, table, columnName);
        putMcvExclude(context, mostCommonValues, quoteColumnName, columnType);

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        return build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }

    // The single bucket spans everything except the MCVs, so both the INSERT and the batched query variant share
    // this expression - they cannot drift apart.
    private void putSingleBucketExpr(VelocityContext context, Table table, Map<String, String> mostCommonValues,
                                     String columnName, Optional<Pair<String, String>> minMax) {
        String quoteColumnName = StatisticUtils.quoting(table, columnName);
        long mcvSum = mostCommonValues.values().stream().mapToLong(Long::parseLong).sum();
        String minValue = minMax.map(minAndMax -> minAndMax.first).orElse(INFINITE_BOUND);
        String maxValue = minMax.map(minAndMax -> minAndMax.second).orElse(INFINITE_BOUND);

        context.put("bucketExpr",
                "concat('[[\"" + minValue + "\",\"" + maxValue + "\",', cast(greatest(0, count(" + quoteColumnName +
                        ") - " + mcvSum + ") as varchar), ',0]]')");
    }

    private VelocityContext buildBaseContext(Database database, Table table, String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("tableUUID", StatisticUtils.hashTableUuidForPkStorage(table.getUUID()));
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("columnNameStr", SqlUtils.escapeSqlString(columnName));
        context.put("catalogName", catalogName);
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        return context;
    }

    private void putMcv(VelocityContext context, Map<String, String> mostCommonValues) {
        String mcvJson = buildMcvJson(mostCommonValues);
        if (mcvJson == null) {
            context.put("mcv", "NULL");
        } else {
            context.put("mcv", quoteSqlString(mcvJson));
        }
    }

    private static String buildInsertIntoHistogramStatistics(String selectSql) {
        List<String> targetColumnNames = buildStatsTargetColumnNames(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
        return "INSERT INTO " + EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME +
                "(" + String.join(", ", targetColumnNames) + ") " + selectSql;
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
