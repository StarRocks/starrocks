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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.math.BigDecimal;
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
import static com.starrocks.statistic.HistogramStatisticsUtils.normalizeBucketsForHll;
import static com.starrocks.statistic.HistogramStatisticsUtils.quoteSqlString;
import static com.starrocks.statistic.HistogramStatisticsUtils.utf8Length;
import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

public class HistogramStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(HistogramStatisticsCollectJob.class);

    private static final String HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE =
            "histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double))";

    private static final String HISTOGRAM_FUNCTION_WITH_NDV_TEMPLATE =
            "histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double), '$ndvEstimator')";

    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnNameStr', $dbId, '$dbName.$tableName'," +
                    " $histogramFunction, " +
                    " $mcv," +
                    " NOW()" +
                    " FROM (" +
                    "   SELECT $columnName as column_key " +
                    "   FROM `$dbName`.`$tableName` $sampleClause " +
                    "   WHERE $randFilter and $columnName is not null $MCVExclude" +
                    "   ORDER BY $columnName LIMIT $totalRows) t";

    private static final String QUERY_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT)," +
                    " cast($dbId as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " $histogramFunction" +
                    " FROM (" +
                    "   SELECT $columnName as column_key " +
                    "   FROM `$dbName`.`$tableName` $sampleClause " +
                    "   WHERE $randFilter and $columnName is not null $MCVExclude" +
                    "   ORDER BY $columnName LIMIT $totalRows) t";

    private static final String COLLECT_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnNameStr', $dbId, '$dbName.$tableName'," +
                    " histogram_hll_ndv($columnName, '$buckets')," +
                    " $mcv," +
                    " NOW()" +
                    " FROM `$dbName`.`$tableName`;";

    private static final String QUERY_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT)," +
                    " cast($dbId as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " histogram_hll_ndv($columnName, '$buckets')" +
                    " FROM `$dbName`.`$tableName`;";

    private static final String COLLECT_BUCKETS_WITHOUT_NDV_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT) as version," +
                    " cast($dbId  as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " $histogramFunction" +
                    " FROM (SELECT $columnName as column_key FROM `$dbName`.`$tableName` where rand() <= $sampleRatio" +
                    " and $columnName is not null $MCVExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

    // For char-family columns we skip the histogram() bucket aggregate, but we still need
    // Histogram.getTotalRows() to reflect the column's real cardinality. So instead of storing
    // NULL buckets we store a single placeholder bucket that represents "all values excluding
    // the MCVs".
    private static final String COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnNameStr', $dbId, '$dbName.$tableName'," +
                    " $bucketExpr," +
                    " $mcv," +
                    " NOW()" +
                    " FROM `$dbName`.`$tableName`$sampleClause$randFilter";

    private static final String QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT)," +
                    " cast($dbId as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " $bucketExpr" +
                    " FROM `$dbName`.`$tableName`$sampleClause$randFilter";

    private static final String COLLECT_MCV_STATISTIC_TEMPLATE =
            "select cast(version as INT), cast(db_id as BIGINT), cast(table_id as BIGINT), " +
                    "cast(column_key as varchar), cast(column_value as varchar) from (" +
                    "SELECT " +
                    StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as version, " +
                    "   $dbId as db_id, " +
                    "   $tableId as table_id, " +
                    "   $columnName as column_key, " +
                    "   count($columnName) as column_value " +
                    "FROM `$dbName`.`$tableName` $sampleClause " +
                    "WHERE $columnName is not null " +
                    "GROUP BY $columnName " +
                    "ORDER BY count($columnName) desc limit $topN ) t";

    public HistogramStatisticsCollectJob(Database db, Table table, List<String> columnNames, List<Type> columnTypes,
                                         StatsConstants.ScheduleType scheduleType, Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, StatsConstants.AnalyzeType.HISTOGRAM, scheduleType, properties);
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        context.getSessionVariable().setNewPlanerAggStage(1);

        double sampleRatio = Double.parseDouble(properties.get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));
        long bucketNum = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_BUCKET_NUM));
        long mcvSize = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_MCV_SIZE));
        StatsConstants.HistogramCollectBucketNdvMode ndvMode = getHistogramCollectBucketNdvMode();

        if (table.isTemporaryTable()) {
            context.setSessionId(((OlapTable) table).getSessionId());
        }

        if (Config.enable_batch_insert_histogram_statistics && columnNames.size() > 1) {
            collectBatched(context, analyzeStatus, sampleRatio, bucketNum, mcvSize, ndvMode);
        } else {
            collectLegacy(context, analyzeStatus, sampleRatio, bucketNum, mcvSize, ndvMode);
        }
    }

    private void collectLegacy(ConnectContext context, AnalyzeStatus analyzeStatus, double sampleRatio, long bucketNum,
                               long mcvSize, StatsConstants.HistogramCollectBucketNdvMode ndvMode) throws Exception {
        long finishedSQLNum = 0;
        long totalCollectSQL = columnNames.size();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            String sql = buildCollectMCV(db, table, mcvSize, columnName, sampleRatio);
            StatisticExecutor statisticExecutor = new StatisticExecutor();
            List<TStatisticData> mcv = statisticExecutor.queryMCV(context, sql);

            Map<String, String> mostCommonValues = buildMostCommonValues(mcv, sampleRatio);

            if (shouldSkipHistogramBuckets(columnType)) {
                sql = buildCollectDefaultBucket(db, table, sampleRatio, mostCommonValues, columnName);
            } else if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.NONE) {
                sql = buildCollectHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, columnName,
                        columnType, false);
            } else if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.SAMPLE) {
                sql = buildCollectHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, columnName,
                        columnType, true);
            } else if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.HLL) {
                sql = buildCollectBucketsWithoutNdv(db, table, sampleRatio, bucketNum, mostCommonValues, columnName, columnType);
                List<TStatisticData> buckets = statisticExecutor.executeStatisticDQL(context, sql);
                sql = buildCollectHistogramWithHllNdv(db, table, mostCommonValues, buckets.get(0).histogram, columnName);
            }
            collectStatisticSync(sql, context, analyzeStatus);

            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    private void collectBatched(ConnectContext context, AnalyzeStatus analyzeStatus, double sampleRatio, long bucketNum,
                                long mcvSize, StatsConstants.HistogramCollectBucketNdvMode ndvMode) throws Exception {
        List<List<Expr>> rowsBuffer = new ArrayList<>();
        List<String> sqlBuffer = new ArrayList<>();
        long bufferSize = batchInsertPrefixSize(HISTOGRAM_STATISTICS_TABLE_NAME);
        long bufferLimit = Math.max(1, Config.histogram_batch_insert_buffer_size);

        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            List<Expr> row;
            String rowSql;
            long rowSize;
            try {
                List<TStatisticData> mcv = queryStatisticSync(
                        buildCollectMCV(db, table, mcvSize, columnName, sampleRatio), context, analyzeStatus);
                Map<String, String> mostCommonValues = buildMostCommonValues(mcv, sampleRatio);

                String histogramQuery = buildBatchedHistogramQuery(
                        context, analyzeStatus, sampleRatio, bucketNum, ndvMode,
                        mostCommonValues, columnName, columnType);

                String buckets = getSingleHistogramResult(
                        queryStatisticSync(histogramQuery, context, analyzeStatus), columnName).histogram;
                String mcvJson = buildMcvJson(mostCommonValues);
                row = buildBatchInsertRow(columnName, buckets, mcvJson);
                rowSql = buildBatchInsertRowSql(columnName, buckets, mcvJson);
                rowSize = utf8Length(rowSql) + (sqlBuffer.isEmpty() ? 0 : 2);
            } catch (Exception collectionFailure) {
                flushBatchInsertOnCollectionFailure(
                        rowsBuffer, sqlBuffer, context, analyzeStatus, columnName, collectionFailure);
                throw collectionFailure;
            }

            if (!rowsBuffer.isEmpty() && bufferSize + rowSize > bufferLimit) {
                flushBatchInsert(rowsBuffer, sqlBuffer, context, analyzeStatus);
                bufferSize = batchInsertPrefixSize(HISTOGRAM_STATISTICS_TABLE_NAME);
                rowSize = utf8Length(rowSql);
            }

            rowsBuffer.add(row);
            sqlBuffer.add(rowSql);
            bufferSize += rowSize;
            if (bufferSize >= bufferLimit) {
                flushBatchInsert(rowsBuffer, sqlBuffer, context, analyzeStatus);
                bufferSize = batchInsertPrefixSize(HISTOGRAM_STATISTICS_TABLE_NAME);
            }

            analyzeStatus.setProgress((i + 1) * 99L / columnNames.size());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }

        flushBatchInsert(rowsBuffer, sqlBuffer, context, analyzeStatus);
        analyzeStatus.setProgress(100);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
    }

    private void flushBatchInsertOnCollectionFailure(
            List<List<Expr>> rowsBuffer, List<String> sqlBuffer, ConnectContext context,
            AnalyzeStatus analyzeStatus, String columnName, Exception collectionFailure) {
        try {
            flushBatchInsert(rowsBuffer, sqlBuffer, context, analyzeStatus);
        } catch (Exception flushFailure) {
            if (flushFailure != collectionFailure) {
                collectionFailure.addSuppressed(flushFailure);
            }
            LOG.warn("Failed to flush buffered histogram statistics after collection failed for column {}",
                    columnName, flushFailure);
        }
    }

    private String buildBatchedHistogramQuery(
            ConnectContext context, AnalyzeStatus analyzeStatus, double sampleRatio, long bucketNum,
            StatsConstants.HistogramCollectBucketNdvMode ndvMode, Map<String, String> mostCommonValues,
            String columnName, Type columnType) throws Exception {
        if (shouldSkipHistogramBuckets(columnType)) {
            return buildQueryDefaultBucket(db, table, sampleRatio, mostCommonValues, columnName);
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.NONE) {
            return buildQueryHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, columnName,
                    columnType, false);
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.SAMPLE) {
            return buildQueryHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, columnName,
                    columnType, true);
        }

        List<TStatisticData> buckets = queryStatisticSync(
                buildCollectBucketsWithoutNdv(db, table, sampleRatio, bucketNum, mostCommonValues, columnName,
                        columnType),
                context, analyzeStatus);
        return buildQueryHistogramWithHllNdv(db, table,
                normalizeBucketsForHll(getSingleHistogramResult(buckets, columnName).histogram), columnName);
    }

    private Map<String, String> buildMostCommonValues(List<TStatisticData> mcv, double sampleRatio) {
        Map<String, String> mostCommonValues = new HashMap<>();
        for (TStatisticData tStatisticData : mcv) {
            if (sampleRatio > 0.0 && sampleRatio < 1.0) {
                long count = Long.parseLong(tStatisticData.histogram);
                count = (long) (1.0 * count / sampleRatio);
                mostCommonValues.put(tStatisticData.columnName, String.valueOf(count));
            } else {
                mostCommonValues.put(tStatisticData.columnName, tStatisticData.histogram);
            }
        }
        return mostCommonValues;
    }

    private StatsConstants.HistogramCollectBucketNdvMode getHistogramCollectBucketNdvMode() {
        String mode = properties.get(StatsConstants.HISTOGRAM_COLLECT_BUCKET_NDV_MODE);
        if (mode.equalsIgnoreCase("none")) {
            return StatsConstants.HistogramCollectBucketNdvMode.NONE;
        } else if (mode.equalsIgnoreCase("sample")) {
            return StatsConstants.HistogramCollectBucketNdvMode.SAMPLE;
        } else if (mode.equalsIgnoreCase("hll")) {
            return StatsConstants.HistogramCollectBucketNdvMode.HLL;
        } else {
            LOG.warn("Invalid histogram collect bucket ndv mode {}.", mode);
            return StatsConstants.HistogramCollectBucketNdvMode.NONE;
        }
    }

    // Convert a sample ratio in (0, 1) into a percent string in (0, 100) for the SAMPLE('percent'=...) clause.
    // Uses BigDecimal to avoid both binary float noise (e.g. 0.49999999999999994) and truncation to 0 for
    // sub-1% ratios on very large tables (which used to produce the illegal SAMPLE('percent'='0')).
    @VisibleForTesting
    static String formatSamplePercent(double sampleRatio) {
        BigDecimal percent = BigDecimal.valueOf(sampleRatio).multiply(BigDecimal.valueOf(100));
        // Drop trailing zeros so integral percents stay clean (e.g. "50" not "50.00"), and avoid
        // scientific notation that the SQL parser cannot consume.
        return percent.stripTrailingZeros().toPlainString();
    }

    private String buildCollectMCV(Database database, Table table, Long topN, String columnName, double sampleRatio) {
        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("dbId", database.getId());

        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", topN);

        if (sampleRatio > 0.0 && sampleRatio < 1.0) {
            String sample = String.format("SAMPLE('percent'='%s')", formatSamplePercent(sampleRatio));
            context.put("sampleClause", sample);
        } else {
            context.put("sampleClause", "");
        }

        return build(context, COLLECT_MCV_STATISTIC_TEMPLATE);
    }

    private String buildInsertIntoHistogramStatistics(String query) {
        List<String> targetColumnNames = buildStatsTargetColumnNames(HISTOGRAM_STATISTICS_TABLE_NAME);
        String columnNames = "(" + String.join(", ", targetColumnNames) + ")";
        return "INSERT INTO " +
                HISTOGRAM_STATISTICS_TABLE_NAME +
                columnNames +
                " " +
                query;
    }

    private VelocityContext buildBaseContext(Database database, Table table, String columnName) {
        String quoteColumName = StatisticUtils.quoting(table, columnName);

        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", quoteColumName);
        context.put("columnNameStr", SqlUtils.escapeSqlString(columnName));
        context.put("dbId", database.getId());
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());

        return context;
    }

    private void addMcvToContext(VelocityContext context, Map<String, String> mostCommonValues) {
        String mcvJson = buildMcvJson(mostCommonValues);
        if (mcvJson == null) {
            context.put("mcv", "NULL");
        } else {
            context.put("mcv", quoteSqlString(mcvJson));
        }
    }

    private void addMcvExcludeToContext(VelocityContext context, Map<String, String> mostCommonValues, String columnName,
                                        Type columnType) {
        String quoteColumName = StatisticUtils.quoting(table, columnName);
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

    private String buildHistogramFunctionWithoutNdv(Database database, Table table, double sampleRatio, Long bucketNum,
                                          String columnName) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);

        return build(context, HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE);
    }

    private String buildHistogramFunction(Database database, Table table, double sampleRatio, Long bucketNum,
                                          String columnName, boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        if (withSampleNdv) {
            context.put("ndvEstimator", Config.statistics_sample_ndv_estimator);
            return build(context, HISTOGRAM_FUNCTION_WITH_NDV_TEMPLATE);
        } else {
            return build(context, HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE);
        }
    }

    private String buildCollectHistogram(Database database, Table table, double sampleRatio, Long bucketNum,
                                         Map<String, String> mostCommonValues, String columnName, Type columnType,
                                         boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        addMcvToContext(context, mostCommonValues);
        addMcvExcludeToContext(context, mostCommonValues, columnName, columnType);

        context.put("histogramFunction", buildHistogramFunction(database, table, sampleRatio, bucketNum, columnName,
                withSampleNdv));
        context.put("totalRows", Config.histogram_max_sample_row_count);

        // TODO: use it by default and remove this switch
        if (Config.enable_use_table_sample_collect_statistics && sampleRatio > 0.0 && sampleRatio < 1.0) {
            String sampleClause = String.format("SAMPLE('percent'='%s')", formatSamplePercent(sampleRatio));
            context.put("sampleClause", sampleClause);
            context.put("randFilter", "TRUE");
        } else {
            String randFilter = String.format(" rand() <= %f", sampleRatio);
            context.put("randFilter", randFilter);
            context.put("sampleClause", "");
        }

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
    }

    private String buildQueryHistogram(Database database, Table table, double sampleRatio, Long bucketNum,
                                       Map<String, String> mostCommonValues, String columnName, Type columnType,
                                       boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        addMcvExcludeToContext(context, mostCommonValues, columnName, columnType);

        context.put("histogramFunction", buildHistogramFunction(database, table, sampleRatio, bucketNum, columnName,
                withSampleNdv));
        context.put("totalRows", Config.histogram_max_sample_row_count);

        if (Config.enable_use_table_sample_collect_statistics && sampleRatio > 0.0 && sampleRatio < 1.0) {
            String sampleClause = String.format("SAMPLE('percent'='%s')", formatSamplePercent(sampleRatio));
            context.put("sampleClause", sampleClause);
            context.put("randFilter", "TRUE");
        } else {
            context.put("randFilter", String.format(" rand() <= %f", sampleRatio));
            context.put("sampleClause", "");
        }

        return build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }

    // In case we skip histogram collection, we simply add one tail bucket that contains all values - sum(MCVs)
    private String buildCollectDefaultBucket(Database database, Table table, double sampleRatio,
                                             Map<String, String> mostCommonValues, String columnName) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        addMcvToContext(context, mostCommonValues);
        addDefaultBucketToContext(context, table, sampleRatio, mostCommonValues, columnName);

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE));
    }

    private String buildQueryDefaultBucket(Database database, Table table, double sampleRatio,
                                           Map<String, String> mostCommonValues, String columnName) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        addDefaultBucketToContext(context, table, sampleRatio, mostCommonValues, columnName);
        return build(context, QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE);
    }

    private void addDefaultBucketToContext(VelocityContext context, Table table, double sampleRatio,
                                           Map<String, String> mostCommonValues, String columnName) {
        String quoteColumName = StatisticUtils.quoting(table, columnName);
        String countExpr;
        if (sampleRatio > 0.0 && sampleRatio < 1.0) {
            String ratioLiteral = BigDecimal.valueOf(sampleRatio).stripTrailingZeros().toPlainString();
            countExpr = "count(" + quoteColumName + ") / cast(" + ratioLiteral + " as double)";
            if (Config.enable_use_table_sample_collect_statistics) {
                context.put("sampleClause", " SAMPLE('percent'='" + formatSamplePercent(sampleRatio) + "')");
                context.put("randFilter", "");
            } else {
                context.put("sampleClause", "");
                context.put("randFilter", " WHERE rand() <= " + ratioLiteral);
            }
        } else {
            context.put("sampleClause", "");
            context.put("randFilter", "");
            countExpr = "count(" + quoteColumName + ")";
        }

        long mcvSum = mostCommonValues.values().stream().mapToLong(Long::parseLong).sum();
        String nonMcvExpr = "greatest(0, " + countExpr + " - " + mcvSum + ")";

        context.put("bucketExpr",
                "concat('[[\"Infinity\",\"Infinity\",', cast(cast(" + nonMcvExpr + " as bigint) as varchar), ',0]]')");
    }

    private String buildCollectHistogramWithHllNdv(Database database, Table table, Map<String, String> mostCommonValues,
                                                String buckets, String columnName) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        addMcvToContext(context, mostCommonValues);
        context.put("buckets", buckets);

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE));
    }

    private String buildQueryHistogramWithHllNdv(Database database, Table table, String buckets, String columnName) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("buckets", StringEscapeUtils.escapeSql(buckets));
        return build(context, QUERY_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE);
    }

    private String buildCollectBucketsWithoutNdv(Database database, Table table, double sampleRatio,
                                      Long bucketNum, Map<String, String> mostCommonValues, String columnName,
                                      Type columnType) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("histogramFunction", buildHistogramFunction(database, table, sampleRatio, bucketNum, columnName, false));
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        addMcvExcludeToContext(context, mostCommonValues, columnName, columnType);

        return build(context, COLLECT_BUCKETS_WITHOUT_NDV_STATISTIC_TEMPLATE);
    }

    private TStatisticData getSingleHistogramResult(List<TStatisticData> results, String columnName)
            throws DdlException {
        return HistogramStatisticsUtils.getSingleHistogramResult(results, columnName, "histogram");
    }

    private List<Expr> buildBatchInsertRow(String columnName, String buckets, String mcvJson) {
        List<Expr> row = new ArrayList<>();
        row.add(new IntLiteral(table.getId(), IntegerType.BIGINT));
        row.add(new StringLiteral(columnName));
        row.add(new IntLiteral(db.getId(), IntegerType.BIGINT));
        row.add(new StringLiteral(db.getOriginName() + "." + table.getName()));
        row.add(buildBucketsLiteral(buckets));
        row.add(mcvJson == null ? new NullLiteral() : new StringLiteral(mcvJson));
        row.add(nowFn());
        return row;
    }

    private String buildBatchInsertRowSql(String columnName, String buckets, String mcvJson) {
        List<String> values = new ArrayList<>();
        values.add(String.valueOf(table.getId()));
        values.add(quoteSqlString(columnName));
        values.add(String.valueOf(db.getId()));
        values.add(quoteSqlString(db.getOriginName() + "." + table.getName()));
        values.add(buildBucketsSql(buckets));
        values.add(mcvJson == null ? "NULL" : quoteSqlString(mcvJson));
        values.add("NOW()");
        return "(" + String.join(", ", values) + ")";
    }

    private void flushBatchInsert(List<List<Expr>> rowsBuffer, List<String> sqlBuffer, ConnectContext context,
                                  AnalyzeStatus analyzeStatus) throws Exception {
        if (rowsBuffer.isEmpty()) {
            return;
        }

        String sql = buildBatchInsertPrefix(HISTOGRAM_STATISTICS_TABLE_NAME) + String.join(", ", sqlBuffer) + ";";
        collectStatisticSync(() -> createInsertStmt(HISTOGRAM_STATISTICS_TABLE_NAME, rowsBuffer, sql),
                context, analyzeStatus);
        rowsBuffer.clear();
        sqlBuffer.clear();
    }

    @Override
    public String getName() {
        return "Histogram";
    }
}
