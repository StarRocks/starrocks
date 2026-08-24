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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.HistogramStatisticsUtils.buildBaseContext;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsLiteral;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildDefaultBucketSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildStatsTargetColumnNames;
import static com.starrocks.statistic.HistogramStatisticsUtils.formatSamplePercent;
import static com.starrocks.statistic.HistogramStatisticsUtils.normalizeBucketsForHll;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcv;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcvExclude;
import static com.starrocks.statistic.HistogramStatisticsUtils.quoteSqlString;
import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

public class HistogramStatisticsCollectJob extends StatisticsCollectJob
        implements LegacyCollectTarget, BatchedCollectTarget {
    private static final Logger LOG = LogManager.getLogger(HistogramStatisticsCollectJob.class);

    // Set at the start of each collect() and read by the SQL builders it drives.
    private StatsConstants.HistogramCollectBucketNdvMode ndvMode =
            StatsConstants.HistogramCollectBucketNdvMode.NONE;

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

        HistogramCollectParams params = new HistogramCollectParams(properties);
        // Derived once per collection so an invalid mode is reported once, not once per column.
        ndvMode = getHistogramCollectBucketNdvMode();

        if (table.isTemporaryTable()) {
            context.setSessionId(((OlapTable) table).getSessionId());
        }

        if (Config.enable_batch_insert_histogram_statistics && columnNames.size() > 1) {
            new BatchedHistogramCollector(this, params).collect(context, analyzeStatus);
        } else {
            new LegacyHistogramCollector(this, params).collect(context, analyzeStatus);
        }
    }

    @Override
    public StatisticsCollectJob job() {
        return this;
    }

    @Override
    public String statsTableName() {
        return HISTOGRAM_STATISTICS_TABLE_NAME;
    }

    @Override
    public String statisticsDescription() {
        return "histogram";
    }

    @Override
    public String buildMcvQuery(HistogramCollectParams params, String columnName) {
        return buildCollectMCV(db, table, params.mcvSize(), columnName, params.sampleRatio());
    }

    @Override
    public double mcvCountScaleRatio(HistogramCollectParams params) {
        return params.sampleRatio();
    }

    @Override
    public String buildLegacyCollectSql(ConnectContext context, StatisticExecutor executor,
                                        HistogramCollectParams params, String columnName, Type columnType,
                                        Map<String, String> mostCommonValues) throws Exception {
        if (shouldSkipHistogramBuckets(columnType)) {
            return buildInsertIntoHistogramStatistics(buildDefaultBucketSql(db, table, getCatalogName(),
                    columnName, mostCommonValues, params.sampleRatio(), COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE));
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.NONE) {
            return buildCollectHistogram(db, table, params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, false);
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.SAMPLE) {
            return buildCollectHistogram(db, table, params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, true);
        }

        // HLL mode needs the bucket boundaries in hand before the INSERT can name them, so it runs an
        // intermediate query on the caller's executor first.
        String bucketQuery = buildCollectBucketsWithoutNdv(db, table, params.sampleRatio(), params.bucketNum(),
                mostCommonValues, columnName, columnType);
        List<TStatisticData> buckets = executor.executeStatisticDQL(context, bucketQuery);
        return buildCollectHistogramWithHllNdv(db, table, mostCommonValues, buckets.get(0).histogram, columnName);
    }

    @Override
    public void afterColumnInserted(ConnectContext context, StatisticExecutor executor, String columnName) {
        // Internal statistics need no post-write cleanup.
    }

    @Override
    public void afterCollection(ConnectContext context, List<String> insertedColumns) {
        // Internal statistics need no post-write cleanup.
    }

    @Override
    public String buildBatchedHistogramQuery(ConnectContext context, AnalyzeStatus analyzeStatus,
                                             HistogramCollectParams params, String columnName, Type columnType,
                                             Map<String, String> mostCommonValues) throws Exception {
        if (shouldSkipHistogramBuckets(columnType)) {
            return buildDefaultBucketSql(db, table, getCatalogName(), columnName, mostCommonValues,
                    params.sampleRatio(), QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE);
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.NONE) {
            return buildQueryHistogram(db, table, params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, false);
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.SAMPLE) {
            return buildQueryHistogram(db, table, params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, true);
        }

        List<TStatisticData> buckets = queryStatisticSync(
                buildCollectBucketsWithoutNdv(db, table, params.sampleRatio(), params.bucketNum(), mostCommonValues,
                        columnName, columnType),
                context, analyzeStatus);
        return buildQueryHistogramWithHllNdv(db, table,
                normalizeBucketsForHll(getSingleHistogramResult(buckets, columnName).histogram), columnName);
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

    private String buildHistogramFunction(Database database, Table table, double sampleRatio, Long bucketNum,
                                          String columnName, boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(database, table, getCatalogName(), columnName);
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
        VelocityContext context = buildBaseContext(database, table, getCatalogName(), columnName);
        putMcv(context, mostCommonValues);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("histogramFunction", buildHistogramFunction(database, table, sampleRatio, bucketNum, columnName,
                withSampleNdv));
        context.put("totalRows", Config.histogram_max_sample_row_count);
        addSampleClauseToContext(context, sampleRatio);

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
    }

    private String buildQueryHistogram(Database database, Table table, double sampleRatio, Long bucketNum,
                                       Map<String, String> mostCommonValues, String columnName, Type columnType,
                                       boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(database, table, getCatalogName(), columnName);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("histogramFunction", buildHistogramFunction(database, table, sampleRatio, bucketNum, columnName,
                withSampleNdv));
        context.put("totalRows", Config.histogram_max_sample_row_count);
        addSampleClauseToContext(context, sampleRatio);

        return build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }

    // TODO: use table sample by default and remove this switch
    private void addSampleClauseToContext(VelocityContext context, double sampleRatio) {
        if (Config.enable_use_table_sample_collect_statistics && sampleRatio > 0.0 && sampleRatio < 1.0) {
            context.put("sampleClause", String.format("SAMPLE('percent'='%s')", formatSamplePercent(sampleRatio)));
            context.put("randFilter", "TRUE");
        } else {
            context.put("sampleClause", "");
            context.put("randFilter", String.format(" rand() <= %f", sampleRatio));
        }
    }

    private String buildCollectHistogramWithHllNdv(Database database, Table table, Map<String, String> mostCommonValues,
                                                String buckets, String columnName) {
        VelocityContext context = buildBaseContext(database, table, getCatalogName(), columnName);
        putMcv(context, mostCommonValues);
        context.put("buckets", buckets);

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE));
    }

    private String buildQueryHistogramWithHllNdv(Database database, Table table, String buckets, String columnName) {
        VelocityContext context = buildBaseContext(database, table, getCatalogName(), columnName);
        context.put("buckets", StringEscapeUtils.escapeSql(buckets));
        return build(context, QUERY_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE);
    }

    private String buildCollectBucketsWithoutNdv(Database database, Table table, double sampleRatio,
                                      Long bucketNum, Map<String, String> mostCommonValues, String columnName,
                                      Type columnType) {
        VelocityContext context = buildBaseContext(database, table, getCatalogName(), columnName);
        context.put("histogramFunction", buildHistogramFunction(database, table, sampleRatio, bucketNum, columnName, false));
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        return build(context, COLLECT_BUCKETS_WITHOUT_NDV_STATISTIC_TEMPLATE);
    }

    private TStatisticData getSingleHistogramResult(List<TStatisticData> results, String columnName)
            throws DdlException {
        return HistogramStatisticsUtils.getSingleHistogramResult(results, columnName, statisticsDescription());
    }

    @Override
    public List<Expr> buildBatchInsertRow(String columnName, String buckets, String mcvJson) {
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

    @Override
    public String buildBatchInsertRowSql(String columnName, String buckets, String mcvJson) {
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

    @Override
    public String getName() {
        return "Histogram";
    }
}
